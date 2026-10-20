import pytest

from bikeshare_lakehouse.transformations import trips
from tests.conftest import trip_row

pytestmark = pytest.mark.spark


def _run(bronze):
    checked = trips.check_trips(trips.parse_trips(bronze))
    return trips.silver_trips(checked), trips.trips_quarantine(checked)


def test_valid_trip_is_typed_and_flagged(bronze_trips):
    silver, quarantine = _run(bronze_trips([trip_row()]))
    row = silver.first()
    assert quarantine.count() == 0
    assert row.duration_seconds == 900
    assert row.start_hour == 8
    assert str(row.start_date) == "2025-04-01"
    assert row.has_start_station and row.has_end_station
    assert not row.is_round_trip and not row.is_under_60s


def test_dockless_trip_is_kept_without_station(bronze_trips):
    row = trip_row(
        ride_id="E1",
        rideable_type="electric_bike",
        start_station_id=None,
        start_station_name=None,
        end_station_id=None,
        end_station_name=None,
    )
    silver, quarantine = _run(bronze_trips([row]))
    assert quarantine.count() == 0
    result = silver.first()
    assert result.has_start_station is False
    assert result.start_station_code is None


def test_trip_ending_before_it_starts_is_quarantined(bronze_trips):
    row = trip_row(ride_id="B1", ended_at="2025-04-01 07:00:00")
    silver, quarantine = _run(bronze_trips([row]))
    assert silver.count() == 0
    rejected = quarantine.first()
    assert rejected.dq_issues == ["ended_before_started"]
    assert '"ride_id":"B1"' in rejected.record
    assert rejected.source_file.endswith(".csv")


def test_unparseable_timestamp_is_quarantined_without_failing(bronze_trips):
    silver, quarantine = _run(bronze_trips([trip_row(ride_id="T1", started_at="not a date")]))
    assert silver.count() == 0
    assert quarantine.first().dq_issues == ["invalid_started_at"]


def test_missing_ride_id_is_quarantined(bronze_trips):
    silver, quarantine = _run(bronze_trips([trip_row(ride_id="  ")]))
    assert silver.count() == 0
    assert quarantine.first().dq_issues == ["missing_ride_id"]


def test_repeated_ride_id_keeps_first_copy(bronze_trips):
    first = trip_row(ride_id="D1", _source_file="trips/a.csv")
    second = trip_row(ride_id="D1", _source_file="trips/b.csv")
    silver, quarantine = _run(bronze_trips([second, first]))
    assert silver.count() == 1
    assert silver.first().source_file == "trips/a.csv"
    assert quarantine.first().dq_issues == ["duplicate_ride_id"]


def test_short_long_and_round_trips_are_flagged(bronze_trips):
    rows = [
        trip_row(ride_id="S1", ended_at="2025-04-01 08:00:30"),
        trip_row(ride_id="L1", ended_at="2025-04-02 09:00:00"),
        trip_row(ride_id="R1", end_station_id="31001"),
    ]
    silver, _ = _run(bronze_trips(rows))
    flags = {r.ride_id: r for r in silver.collect()}
    assert flags["S1"].is_under_60s
    assert flags["L1"].is_over_24h
    assert flags["R1"].is_round_trip


def test_silver_schema_matches_declared_columns(bronze_trips):
    silver, _ = _run(bronze_trips([trip_row()]))
    assert silver.columns == [name for name, _ in trips.SILVER_TRIP_COLUMNS]
    ddl = trips.silver_trip_schema(ride_id_mask="c.silver.mask_ride_id")
    assert ddl.startswith("ride_id STRING MASK c.silver.mask_ride_id, trip_key STRING")
