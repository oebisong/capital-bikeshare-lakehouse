import io
import json
import zipfile
from datetime import UTC, datetime

import pytest

from bikeshare_lakehouse.collectors import gbfs, trips
from bikeshare_lakehouse.common.landing import LocalStore

DISCOVERY_URL = "https://gbfs.example.test/gbfs.json"
DISCOVERY = {
    "data": {
        "en": {
            "feeds": [
                {"name": "station_status", "url": "https://gbfs.example.test/station_status.json"},
                {"name": "station_information", "url": "https://gbfs.example.test/info.json"},
            ]
        }
    }
}


class FakeResponse:
    def __init__(self, status=200, body=None, content=b""):
        self.status_code = status
        self._body = body
        self._content = content

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, _size):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, **_kwargs):
        self.calls.append(url)
        return self.responses[url]


def snapshot(stations=1):
    return {"last_updated": 1_792_440_000, "ttl": 60, "data": {"stations": [{}] * stations}}


def clock():
    return datetime(2026, 10, 19, 10, 15, 0, tzinfo=UTC)


def test_months_are_validated_and_sorted():
    assert trips.parse_months("2025-06, 2025-04,2025-05") == ["202504", "202505", "202506"]
    with pytest.raises(ValueError):
        trips.parse_months("2025-13")
    with pytest.raises(ValueError):
        trips.parse_months("")


def test_archive_member_ignores_macos_metadata():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("202504-capitalbikeshare-tripdata.csv", "ride_id\nA1\n")
        archive.writestr("__MACOSX/._202504-capitalbikeshare-tripdata.csv", "junk")
    with zipfile.ZipFile(buffer) as archive:
        assert trips.csv_member(archive) == "202504-capitalbikeshare-tripdata.csv"


def test_trip_download_lands_csv_and_skips_on_rerun(tmp_path):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("202504-capitalbikeshare-tripdata.csv", "ride_id\nA1\n")
    url = trips.archive_url("https://data.example.test", "202504")
    session = FakeSession({url: FakeResponse(content=buffer.getvalue())})
    store = LocalStore(tmp_path)

    assert trips.run(["202504"], "https://data.example.test", store, session) == 1
    landed = tmp_path / "trips" / "202504-capitalbikeshare-tripdata.csv"
    assert landed.read_text() == "ride_id\nA1\n"

    assert trips.run(["202504"], "https://data.example.test", store, session) == 0
    assert len(session.calls) == 1


def test_gbfs_snapshot_is_written_inside_envelope(tmp_path):
    session = FakeSession(
        {
            DISCOVERY_URL: FakeResponse(body=DISCOVERY),
            "https://gbfs.example.test/station_status.json": FakeResponse(body=snapshot(3)),
        }
    )
    written = gbfs.collect(DISCOVERY_URL, ["station_status"], LocalStore(tmp_path), session, clock)

    assert written == ["gbfs/station_status/2026/10/19/station_status_20261019T101500Z.json"]
    envelope = json.loads((tmp_path / written[0]).read_text())
    assert envelope["collected_at"] == "2026-10-19T10:15:00Z"
    assert envelope["response"]["last_updated"] == 1_792_440_000
    assert len(envelope["response"]["data"]["stations"]) == 3


def test_gbfs_failure_in_one_feed_still_collects_the_other(tmp_path):
    session = FakeSession(
        {
            DISCOVERY_URL: FakeResponse(body=DISCOVERY),
            "https://gbfs.example.test/station_status.json": FakeResponse(status=503),
            "https://gbfs.example.test/info.json": FakeResponse(body=snapshot()),
        }
    )
    store = LocalStore(tmp_path)
    with pytest.raises(gbfs.FeedError, match="station_status"):
        gbfs.collect(
            DISCOVERY_URL, ["station_status", "station_information"], store, session, clock
        )
    assert list(tmp_path.rglob("station_information_*.json"))
    assert not list(tmp_path.rglob("station_status_*.json"))


def test_gbfs_response_without_stations_is_rejected(tmp_path):
    session = FakeSession(
        {
            DISCOVERY_URL: FakeResponse(body=DISCOVERY),
            "https://gbfs.example.test/station_status.json": FakeResponse(body={"data": {}}),
        }
    )
    with pytest.raises(gbfs.FeedError, match="no data.stations"):
        gbfs.collect(DISCOVERY_URL, ["station_status"], LocalStore(tmp_path), session, clock)
    assert not list(tmp_path.rglob("*.json"))


def test_unknown_feed_name_is_reported():
    with pytest.raises(gbfs.FeedError):
        gbfs.resolve_feed_urls({"data": {}})
