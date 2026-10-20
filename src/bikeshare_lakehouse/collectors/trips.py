"""Download monthly Capital Bikeshare trip files into the landing volume.

Each month is published as a zip archive holding one CSV file. The CSV is
extracted and written to trips/<YYYYMM>-capitalbikeshare-tripdata.csv. A month
that is already in the landing area is skipped, so reruns add nothing.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import tempfile
import zipfile
from pathlib import Path

import requests

from bikeshare_lakehouse.common.http import build_session
from bikeshare_lakehouse.common.landing import LandingStore, VolumeStore
from bikeshare_lakehouse.common.logs import configure_logging

logger = logging.getLogger(__name__)

MONTH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
DOWNLOAD_CHUNK_BYTES = 1024 * 1024


def parse_months(value: str) -> list[str]:
    """Turn "2025-04,2025-05" into ["202504", "202505"], rejecting bad input."""
    months = []
    for item in (part.strip() for part in value.split(",")):
        if not item:
            continue
        match = MONTH_PATTERN.match(item)
        if not match:
            raise ValueError(f"Invalid month {item!r}. Use YYYY-MM.")
        months.append(match.group(1) + match.group(2))
    if not months:
        raise ValueError("No months given.")
    return sorted(set(months))


def archive_url(base_url: str, month: str) -> str:
    return f"{base_url.rstrip('/')}/{month}-capitalbikeshare-tripdata.zip"


def landing_path(month: str) -> str:
    return f"trips/{month}-capitalbikeshare-tripdata.csv"


def csv_member(archive: zipfile.ZipFile) -> str:
    """Return the single trip CSV inside an archive, ignoring macOS metadata entries."""
    members = [
        name
        for name in archive.namelist()
        if name.lower().endswith(".csv") and not name.startswith("__MACOSX/")
    ]
    if len(members) != 1:
        raise ValueError(f"Expected one CSV in the archive, found {members}")
    return members[0]


def download_month(
    month: str, base_url: str, store: LandingStore, session: requests.Session
) -> bool:
    """Download one month. Returns True if a file was written, False if it was skipped."""
    target = landing_path(month)
    if store.exists(target):
        logger.info("Skipping %s, %s already exists", month, target)
        return False

    url = archive_url(base_url, month)
    logger.info("Downloading %s", url)
    with tempfile.TemporaryDirectory() as workdir:
        zip_path = Path(workdir) / "trips.zip"
        with session.get(url, stream=True) as response:
            response.raise_for_status()
            with zip_path.open("wb") as handle:
                for chunk in response.iter_content(DOWNLOAD_CHUNK_BYTES):
                    handle.write(chunk)

        with zipfile.ZipFile(zip_path) as archive:
            member = csv_member(archive)
            csv_path = Path(archive.extract(member, workdir))

        with csv_path.open("rb") as contents:
            store.write(target, contents)
    logger.info("Landed %s (%s)", target, month)
    return True


def run(months: list[str], base_url: str, store: LandingStore, session: requests.Session) -> int:
    written = sum(download_month(month, base_url, store, session) for month in months)
    logger.info("%d of %d months downloaded", written, len(months))
    return written


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months", required=True, help="Comma separated list such as 2025-04,2025-05"
    )
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--volume-path", required=True, help="/Volumes/<catalog>/<schema>/<volume>")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    run(parse_months(args.months), args.base_url, VolumeStore(args.volume_path), build_session())


if __name__ == "__main__":
    main()
