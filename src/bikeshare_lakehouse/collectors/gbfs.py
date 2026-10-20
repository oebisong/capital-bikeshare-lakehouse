"""Collect GBFS station snapshots into the landing volume.

Feed URLs are read from the operator's GBFS discovery file on every run, so a
moved endpoint is picked up without a code change. Each response is stored
inside an envelope that records when we collected it. The feed's own
last_updated value stays untouched inside the response, which keeps
collection time and source time separate.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime

import requests

from bikeshare_lakehouse.common.http import build_session
from bikeshare_lakehouse.common.landing import LandingStore, VolumeStore
from bikeshare_lakehouse.common.logs import configure_logging

logger = logging.getLogger(__name__)


class FeedError(RuntimeError):
    pass


def resolve_feed_urls(discovery: dict, language: str = "en") -> dict[str, str]:
    try:
        feeds = discovery["data"][language]["feeds"]
    except (KeyError, TypeError) as exc:
        raise FeedError(f"Discovery file has no feeds for language {language!r}") from exc
    return {feed["name"]: feed["url"] for feed in feeds}


def fetch_json(session: requests.Session, url: str) -> tuple[int, dict]:
    response = session.get(url)
    if response.status_code != 200:
        raise FeedError(f"{url} returned HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError as exc:
        raise FeedError(f"{url} did not return JSON") from exc
    return response.status_code, body


def validate_snapshot(feed: str, body: dict) -> None:
    stations = body.get("data", {}).get("stations") if isinstance(body, dict) else None
    if not isinstance(stations, list):
        raise FeedError(f"{feed} response has no data.stations list")
    if "last_updated" not in body:
        raise FeedError(f"{feed} response has no last_updated")


def snapshot_path(feed: str, collected_at: datetime) -> str:
    stamp = collected_at.strftime("%Y%m%dT%H%M%SZ")
    return f"gbfs/{feed}/{collected_at:%Y/%m/%d}/{feed}_{stamp}.json"


def build_envelope(feed: str, url: str, status: int, body: dict, collected_at: datetime) -> dict:
    return {
        "collected_at": collected_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "feed": feed,
        "source_url": url,
        "http_status": status,
        "response": body,
    }


def collect(
    discovery_url: str,
    feeds: list[str],
    store: LandingStore,
    session: requests.Session,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[str]:
    """Collect one snapshot of each feed. Returns the paths written.

    Every feed is attempted even if an earlier one fails. Failures are raised
    together at the end so the job run is marked as failed.
    """
    _, discovery = fetch_json(session, discovery_url)
    urls = resolve_feed_urls(discovery)

    written, errors = [], []
    for feed in feeds:
        try:
            if feed not in urls:
                raise FeedError(f"Feed {feed!r} is not listed in the discovery file")
            status, body = fetch_json(session, urls[feed])
            validate_snapshot(feed, body)
            collected_at = clock()
            path = snapshot_path(feed, collected_at)
            if store.exists(path):
                logger.warning("Snapshot %s already exists, skipping", path)
                continue
            payload = json.dumps(build_envelope(feed, urls[feed], status, body, collected_at))
            store.write(path, io.BytesIO(payload.encode("utf-8")))
            written.append(path)
            logger.info(
                "Collected %s: %d stations, source last_updated %s",
                feed,
                len(body["data"]["stations"]),
                body["last_updated"],
            )
        except (FeedError, requests.RequestException) as exc:
            logger.error("Failed to collect %s: %s", feed, exc)
            errors.append(f"{feed}: {exc}")

    if errors:
        raise FeedError("; ".join(errors))
    return written


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discovery-url", required=True)
    parser.add_argument("--feeds", required=True, help="Comma separated feed names")
    parser.add_argument("--volume-path", required=True, help="/Volumes/<catalog>/<schema>/<volume>")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    feeds = [feed.strip() for feed in args.feeds.split(",") if feed.strip()]
    collect(args.discovery_url, feeds, VolumeStore(args.volume_path), build_session())


if __name__ == "__main__":
    main()
