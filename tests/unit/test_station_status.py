import pytest

from bikeshare_lakehouse.transformations import station_status
from tests.conftest import gbfs_file, status_station

pytestmark = pytest.mark.spark

T0 = 1_792_440_000  # 2026-10-19 00:00:00 UTC


def _run(bronze):
    checked = station_status.check_station_status(bronze)
    return (
        station_status.silver_station_status(checked),
        station_status.station_status_quarantine(checked),
    )


def test_snapshot_expands_to_one_row_per_station(bronze_gbfs):
    stations = [status_station("a", last_reported=T0), status_station("b", last_reported=T0)]
    silver, quarantine = _run(bronze_gbfs([gbfs_file("station_status", T0, stations)]))
    assert silver.count() == 2
    assert quarantine.count() == 0
    row = silver.where("station_id = 'a'").first()
    assert row.is_installed is True
    assert row.report_age_seconds == 0


def test_same_feed_state_collected_twice_is_a_duplicate(bronze_gbfs):
    files = [
        gbfs_file("station_status", T0, [status_station("a", last_reported=T0)]),
        gbfs_file(
            "station_status",
            T0,
            [status_station("a", last_reported=T0)],
            collected_at="2026-10-19T10:00:40Z",
        ),
    ]
    silver, quarantine = _run(bronze_gbfs(files))
    assert silver.count() == 1
    assert quarantine.first().dq_issues == ["duplicate_observation"]


def test_unchanged_station_in_new_snapshot_is_kept(bronze_gbfs):
    station = status_station("a", last_reported=T0)
    files = [
        gbfs_file("station_status", T0, [station]),
        gbfs_file("station_status", T0 + 900, [station]),
    ]
    silver, quarantine = _run(bronze_gbfs(files))
    assert silver.count() == 2
    assert quarantine.count() == 0


def test_negative_count_is_quarantined_with_reason(bronze_gbfs):
    stations = [status_station("a", bikes=-1, last_reported=T0)]
    silver, quarantine = _run(bronze_gbfs([gbfs_file("station_status", T0, stations)]))
    assert silver.count() == 0
    row = quarantine.first()
    assert row.dq_issues == ["negative_bike_count"]
    assert '"station_id":"a"' in row.record


def test_old_report_is_marked_stale(bronze_gbfs):
    stations = [status_station("a", last_reported=T0 - 7200)]
    silver, _ = _run(bronze_gbfs([gbfs_file("station_status", T0, stations)]))
    assert silver.first().is_stale is True


def test_boolean_flags_are_accepted(bronze_gbfs):
    stations = [status_station("a", last_reported=T0, is_installed=True, is_renting=False)]
    silver, _ = _run(bronze_gbfs([gbfs_file("station_status", T0, stations)]))
    row = silver.first()
    assert row.is_installed is True
    assert row.is_renting is False


def test_corrupt_file_is_quarantined(bronze_gbfs):
    silver, quarantine = _run(bronze_gbfs(['{"collected_at": "2026-10-19T10:00:00Z", "resp']))
    assert silver.count() == 0
    row = quarantine.first()
    assert row.dq_issues == ["unparseable_snapshot"]
    assert row.record.startswith('{"collected_at"')
