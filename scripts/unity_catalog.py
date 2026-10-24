"""Create or remove the Unity Catalog objects for the lakehouse.

    python scripts/unity_catalog.py up
    python scripts/unity_catalog.py down

The bundle cannot create a storage credential, and a force delete of the catalog
is the only reliable way to remove every schema, table and volume in one step.
This script therefore owns every Unity Catalog object. Each object is created
only if it is missing, so the script is safe to rerun.

Groups are created in the Databricks account, because Unity Catalog grants
only accept account groups. The script therefore uses two CLI profiles: the
workspace profile and an account profile, DATABRICKS_ACCOUNT_PROFILE.

Settings come from environment variables. The Makefile sets them from
config/project.env, .env and the outputs of the Bicep deployment.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

logger = logging.getLogger("unity_catalog")

SCHEMAS = {
    "bronze": "Raw records exactly as published, loaded incrementally by Auto Loader.",
    "silver": "Typed, validated and deduplicated records.",
    "quarantine": "Records that failed validation, kept with the reasons. Engineers only.",
    "gold": "Star schema and aggregates for analysis and the dashboard.",
}

# A new role assignment on the storage account can take a few minutes to reach
# Unity Catalog, so storage validation is retried.
STORAGE_RETRY_ATTEMPTS = 10
STORAGE_RETRY_SECONDS = 30


@dataclass(frozen=True)
class Settings:
    catalog: str
    landing_volume: str
    name_prefix: str
    storage_account: str
    container: str
    access_connector_id: str
    engineers_group: str
    analysts_group: str
    viewers_group: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        names = {
            "catalog": "CATALOG",
            "landing_volume": "LANDING_VOLUME",
            "name_prefix": "NAME_PREFIX",
            "storage_account": "STORAGE_ACCOUNT_NAME",
            "container": "STORAGE_CONTAINER",
            "access_connector_id": "ACCESS_CONNECTOR_ID",
            "engineers_group": "ENGINEERS_GROUP",
            "analysts_group": "ANALYSTS_GROUP",
            "viewers_group": "DASHBOARD_VIEWERS_GROUP",
        }
        missing = [var for var in names.values() if not env.get(var)]
        if missing:
            raise SystemExit(f"Missing settings: {', '.join(missing)}. Run make infra-up first.")
        return cls(**{field: env[var] for field, var in names.items()})

    @property
    def root_url(self) -> str:
        return f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"

    @property
    def managed_url(self) -> str:
        return f"{self.root_url}/managed"

    @property
    def landing_url(self) -> str:
        return f"{self.root_url}/landing"

    @property
    def credential_name(self) -> str:
        return f"{self.name_prefix}_lakehouse_credential"

    @property
    def location_name(self) -> str:
        return f"{self.name_prefix}_lakehouse_location"


@dataclass(frozen=True)
class Grant:
    securable_type: str
    full_name: str
    principal: str
    privileges: tuple[str, ...]


def grant_plan(s: Settings) -> list[Grant]:
    """Least-privilege access for the three roles.

    Engineers own the platform. Analysts read silver and gold, where ride_id is
    masked. Dashboard viewers read gold only. Nobody outside the engineers
    group can read bronze or quarantine, which hold ride_id unmasked.
    """
    read = ("USE_SCHEMA", "SELECT")
    return [
        Grant("catalog", s.catalog, s.engineers_group, ("ALL_PRIVILEGES",)),
        Grant("catalog", s.catalog, s.analysts_group, ("USE_CATALOG",)),
        Grant("schema", f"{s.catalog}.silver", s.analysts_group, read),
        Grant("schema", f"{s.catalog}.gold", s.analysts_group, read),
        Grant("catalog", s.catalog, s.viewers_group, ("USE_CATALOG",)),
        Grant("schema", f"{s.catalog}.gold", s.viewers_group, read),
    ]


def ensure(label: str, get: Callable[[], object], create: Callable[[], object]) -> None:
    from databricks.sdk.errors import NotFound

    try:
        get()
        logger.info("%s already exists", label)
    except NotFound:
        create()
        logger.info("Created %s", label)


def with_storage_retry(label: str, action: Callable[[], object]) -> None:
    from databricks.sdk.errors import DatabricksError

    for attempt in range(1, STORAGE_RETRY_ATTEMPTS + 1):
        try:
            action()
            return
        except DatabricksError as exc:
            if attempt == STORAGE_RETRY_ATTEMPTS:
                raise
            logger.warning(
                "%s: storage not reachable yet (%s). Retry %d of %d in %ds.",
                label,
                exc,
                attempt,
                STORAGE_RETRY_ATTEMPTS - 1,
                STORAGE_RETRY_SECONDS,
            )
            time.sleep(STORAGE_RETRY_SECONDS)


def remove_workspace_local_group(w, name: str) -> None:
    """Delete a workspace-local group of this name.

    Unity Catalog only accepts account groups as principals, and a local group
    with the same name blocks the account group from being assigned.
    """
    for group in w.groups.list(filter=f'displayName eq "{name}"', attributes="id,meta"):
        if group.meta and group.meta.resource_type == "WorkspaceGroup":
            w.groups.delete(group.id)
            logger.info("Deleted workspace-local group %s", name)


def ensure_group(w, a, name: str, add_current_user: bool = False) -> None:
    """Create the account group, assign it to the workspace and add the caller."""
    from databricks.sdk.service import iam

    remove_workspace_local_group(w, name)
    existing = list(a.groups.list(filter=f'displayName eq "{name}"', attributes="id,displayName"))
    group = existing[0] if existing else a.groups.create(display_name=name)
    logger.info("%s account group %s", "Found" if existing else "Created", name)

    a.workspace_assignment.update(
        workspace_id=w.get_workspace_id(),
        principal_id=int(group.id),
        permissions=[iam.WorkspacePermission.USER],
    )
    logger.info("Assigned %s to the workspace", name)

    if add_current_user:
        user_name = w.current_user.me().user_name
        user = next(iter(a.users.list(filter=f'userName eq "{user_name}"', attributes="id")))
        a.groups.patch(
            group.id,
            operations=[iam.Patch(op=iam.PatchOp.ADD, path="members", value=[{"value": user.id}])],
            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        )
        logger.info("Added %s to %s", user_name, name)


def apply_grants(w, s: Settings) -> None:
    from databricks.sdk.service import catalog as uc

    for g in grant_plan(s):
        w.grants.update(
            securable_type=g.securable_type,
            full_name=g.full_name,
            changes=[
                uc.PermissionsChange(
                    principal=g.principal, add=[uc.Privilege(p) for p in g.privileges]
                )
            ],
        )
        logger.info("Granted %s on %s to %s", ", ".join(g.privileges), g.full_name, g.principal)


def up(w, a, s: Settings) -> None:
    from databricks.sdk.service import catalog as uc

    ensure(
        f"storage credential {s.credential_name}",
        lambda: w.storage_credentials.get(s.credential_name),
        lambda: w.storage_credentials.create(
            name=s.credential_name,
            azure_managed_identity=uc.AzureManagedIdentityRequest(
                access_connector_id=s.access_connector_id
            ),
            comment="Managed identity of the lakehouse access connector.",
        ),
    )
    ensure(
        f"external location {s.location_name}",
        lambda: w.external_locations.get(s.location_name),
        lambda: with_storage_retry(
            s.location_name,
            lambda: w.external_locations.create(
                name=s.location_name,
                url=s.root_url,
                credential_name=s.credential_name,
                comment="Lakehouse container: landing files and managed tables.",
            ),
        ),
    )
    ensure(
        f"catalog {s.catalog}",
        lambda: w.catalogs.get(s.catalog),
        lambda: w.catalogs.create(
            name=s.catalog,
            storage_root=s.managed_url,
            comment="Capital Bikeshare analytics lakehouse.",
        ),
    )
    for schema, comment in SCHEMAS.items():
        ensure(
            f"schema {s.catalog}.{schema}",
            lambda schema=schema: w.schemas.get(f"{s.catalog}.{schema}"),
            lambda schema=schema, comment=comment: w.schemas.create(
                name=schema, catalog_name=s.catalog, comment=comment
            ),
        )
    volume = f"{s.catalog}.bronze.{s.landing_volume}"
    ensure(
        f"volume {volume}",
        lambda: w.volumes.read(volume),
        lambda: with_storage_retry(
            volume,
            lambda: w.volumes.create(
                catalog_name=s.catalog,
                schema_name="bronze",
                name=s.landing_volume,
                volume_type=uc.VolumeType.EXTERNAL,
                storage_location=s.landing_url,
                comment="Raw trip and GBFS files. External, so the files outlive the catalog.",
            ),
        ),
    )

    ensure_group(w, a, s.engineers_group, add_current_user=True)
    ensure_group(w, a, s.analysts_group)
    ensure_group(w, a, s.viewers_group)
    apply_grants(w, s)


def down(w, a, s: Settings) -> None:
    """Remove the catalog and its storage objects.

    The landing volume is external, so its files stay in the storage account.
    Managed table data is deleted with the catalog and is rebuilt from the
    landing files by the next pipeline run.
    """
    from databricks.sdk.errors import NotFound

    steps = [
        (f"catalog {s.catalog}", lambda: w.catalogs.delete(s.catalog, force=True)),
        (
            f"external location {s.location_name}",
            lambda: w.external_locations.delete(s.location_name, force=True),
        ),
        (
            f"storage credential {s.credential_name}",
            lambda: w.storage_credentials.delete(s.credential_name, force=True),
        ),
    ]
    for label, action in steps:
        try:
            action()
            logger.info("Deleted %s", label)
        except NotFound:
            logger.info("%s does not exist", label)

    for name in (s.engineers_group, s.analysts_group, s.viewers_group):
        remove_workspace_local_group(w, name)
        for group in a.groups.list(filter=f'displayName eq "{name}"', attributes="id"):
            a.groups.delete(group.id)
            logger.info("Deleted account group %s", name)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "down"])
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from databricks.sdk import AccountClient, WorkspaceClient

    settings = Settings.from_env(os.environ)
    account_profile = os.environ.get("DATABRICKS_ACCOUNT_PROFILE")
    if not account_profile:
        raise SystemExit("Set DATABRICKS_ACCOUNT_PROFILE. Run make databricks-account-login first.")
    workspace = WorkspaceClient()
    account = AccountClient(profile=account_profile)
    action = up if args.action == "up" else down
    action(workspace, account, settings)


if __name__ == "__main__":
    main()
