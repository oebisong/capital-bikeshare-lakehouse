"""Write access to the landing area used by the collectors.

Collectors depend on the small LandingStore interface so they can run against a
Unity Catalog volume in Databricks or a local folder in tests.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import BinaryIO, Protocol

logger = logging.getLogger(__name__)


class LandingStore(Protocol):
    def exists(self, relative_path: str) -> bool: ...

    def write(self, relative_path: str, contents: BinaryIO) -> None: ...


class VolumeStore:
    """Landing store backed by a Unity Catalog volume, written through the Files API.

    A Files API upload is a single request, so a reader never sees a partial file.
    """

    def __init__(self, volume_path: str, client=None):
        if not volume_path.startswith("/Volumes/"):
            raise ValueError(f"Expected a /Volumes/... path, got {volume_path!r}")
        self.root = volume_path.rstrip("/")
        if client is None:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient()
        self.client = client

    def _full(self, relative_path: str) -> str:
        return f"{self.root}/{relative_path.lstrip('/')}"

    def exists(self, relative_path: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.client.files.get_metadata(self._full(relative_path))
        except NotFound:
            return False
        return True

    def write(self, relative_path: str, contents: BinaryIO) -> None:
        self.client.files.upload(self._full(relative_path), contents, overwrite=False)
        logger.info("Wrote %s", self._full(relative_path))


class LocalStore:
    """Landing store backed by a local folder. Writes go to a temporary file first."""

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)

    def exists(self, relative_path: str) -> bool:
        return (self.root / relative_path).exists()

    def write(self, relative_path: str, contents: BinaryIO) -> None:
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".", delete=False) as tmp:
            for chunk in iter(lambda: contents.read(1024 * 1024), b""):
                tmp.write(chunk)
        os.replace(tmp.name, target)
