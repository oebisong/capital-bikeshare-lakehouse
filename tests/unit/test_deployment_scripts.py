import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    # dataclasses look the module up in sys.modules while the class is built.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


unity_catalog = load("unity_catalog")
infra_outputs = load("infra_outputs")

ENV = {
    "CATALOG": "capital_bikeshare",
    "LANDING_VOLUME": "landing",
    "NAME_PREFIX": "cabi",
    "STORAGE_ACCOUNT_NAME": "stcabi123",
    "STORAGE_CONTAINER": "lakehouse",
    "ACCESS_CONNECTOR_ID": "/subscriptions/x/providers/Microsoft.Databricks/accessConnectors/dbac",
    "ENGINEERS_GROUP": "eng",
    "ANALYSTS_GROUP": "analysts",
    "DASHBOARD_VIEWERS_GROUP": "viewers",
}


def test_storage_urls_are_built_from_settings():
    s = unity_catalog.Settings.from_env(ENV)
    assert s.root_url == "abfss://lakehouse@stcabi123.dfs.core.windows.net"
    assert s.managed_url.endswith("/managed")
    assert s.landing_url.endswith("/landing")
    assert not s.managed_url.startswith(s.landing_url)


def test_missing_settings_name_every_gap():
    env = dict(ENV, STORAGE_ACCOUNT_NAME="", ACCESS_CONNECTOR_ID="")
    with pytest.raises(SystemExit, match="STORAGE_ACCOUNT_NAME, ACCESS_CONNECTOR_ID"):
        unity_catalog.Settings.from_env(env)


def test_only_engineers_can_read_bronze_and_quarantine():
    s = unity_catalog.Settings.from_env(ENV)
    plan = unity_catalog.grant_plan(s)
    others = [g for g in plan if g.principal != "eng"]
    assert all(".bronze" not in g.full_name and ".quarantine" not in g.full_name for g in others)


def test_dashboard_viewers_read_gold_only():
    s = unity_catalog.Settings.from_env(ENV)
    schemas = {g.full_name for g in unity_catalog.grant_plan(s) if g.principal == "viewers"}
    assert schemas == {"capital_bikeshare", "capital_bikeshare.gold"}


def test_infra_outputs_become_env_lines():
    outputs = {
        "workspaceUrl": {"type": "String", "value": "adb-1.2.azuredatabricks.net"},
        "accessConnectorId": {"type": "String", "value": "/subscriptions/x"},
    }
    assert infra_outputs.to_env_lines(outputs) == [
        "ACCESS_CONNECTOR_ID=/subscriptions/x",
        "WORKSPACE_URL=adb-1.2.azuredatabricks.net",
    ]
