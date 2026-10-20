import pytest

from bikeshare_lakehouse.transformations import station_information
from tests.conftest import gbfs_file, info_station

pytestmark = pytest.mark.spark

T0 = 1_792_440_000


def _run(bronze):
    checked = station_information.check_station_information(bronze)
    return (
        station_information.silver_station_information(checked),
        station_information.station_information_quarantine(checked),
    )


def test_repeated_snapshots_collapse_into_one_version(bronze_gbfs):
    files = [gbfs_file("station_information", T0 + i * 900, [info_station()]) for i in range(4)]
    silver, _ = _run(bronze_gbfs(files))
    row = silver.first()
    assert silver.count() == 1
    assert row.snapshot_count == 4
    assert row.is_current is True


def test_renamed_station_becomes_a_new_current_version(bronze_gbfs):
    files = [
        gbfs_file("station_information", T0, [info_station(name="Old Name")]),
        gbfs_file("station_information", T0 + 900, [info_station(name="New Name")]),
    ]
    silver, _ = _run(bronze_gbfs(files))
    versions = {r.station_name: r.is_current for r in silver.collect()}
    assert versions == {"Old Name": False, "New Name": True}


def test_station_outside_service_area_is_quarantined(bronze_gbfs):
    files = [gbfs_file("station_information", T0, [info_station(lat=0.0, lon=0.0)])]
    silver, quarantine = _run(bronze_gbfs(files))
    assert silver.count() == 0
    assert quarantine.first().dq_issues == ["coordinates_out_of_range"]


def test_station_without_code_is_quarantined(bronze_gbfs):
    files = [gbfs_file("station_information", T0, [info_station(short_name="")])]
    silver, quarantine = _run(bronze_gbfs(files))
    assert silver.count() == 0
    assert quarantine.first().dq_issues == ["missing_station_code"]
