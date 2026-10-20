"""Runs a small controlled dataset through silver and gold and checks the
same cross-table properties that the quality checks job verifies in Databricks.
"""

import pytest

from bikeshare_lakehouse.gold import aggregates, dimensions, facts
from bikeshare_lakehouse.transformations import station_information, station_status, trips
from tests.conftest import gbfs_file, info_station, status_station, trip_row

pytestmark = pytest.mark.spark

T0 = 1_792_440_000


def test_layers_preserve_counts_and_grain(bronze_trips, bronze_gbfs):
    bronze_trip_rows = [
        trip_row(ride_id="A1"),
        trip_row(ride_id="A2", start_station_id="31002", end_station_id="31001"),
        trip_row(ride_id="A3", start_station_id=None, start_station_name=None),
        trip_row(ride_id="A3", _source_file="trips/202505-capitalbikeshare-tripdata.csv"),
        trip_row(ride_id="BAD", ended_at="2025-03-01 00:00:00"),
    ]
    bronze_t = bronze_trips(bronze_trip_rows)
    checked_t = trips.check_trips(trips.parse_trips(bronze_t))
    silver_t = trips.silver_trips(checked_t)
    quarantine_t = trips.trips_quarantine(checked_t)
    assert silver_t.count() + quarantine_t.count() == bronze_t.count()
    assert silver_t.count() == 3

    info = [info_station("uuid-1", "31001"), info_station("uuid-2", "31002", "Station Two")]
    info_files = [gbfs_file("station_information", T0 + i * 900, info) for i in range(3)]
    versions = station_information.silver_station_information(
        station_information.check_station_information(bronze_gbfs(info_files))
    )

    status_files = [
        gbfs_file(
            "station_status",
            T0 + i * 900,
            [
                status_station("uuid-1", bikes=i, last_reported=T0 + i * 900),
                status_station("uuid-2", docks=0, last_reported=T0 + i * 900),
            ],
        )
        for i in range(3)
    ]
    # The last file is collected twice, as happens when a poll beats the feed refresh.
    status_files.append(status_files[-1])
    checked_s = station_status.check_station_status(bronze_gbfs(status_files))
    silver_s = station_status.silver_station_status(checked_s)
    assert silver_s.count() == 6
    assert station_status.station_status_quarantine(checked_s).count() == 2

    stations = dimensions.dim_station(versions, silver_t)
    fact_t = facts.fact_trips(silver_t)
    fact_a = facts.fact_station_availability(silver_s, stations)
    dates = dimensions.dim_date(
        silver_t.selectExpr("start_date AS date"), fact_a.selectExpr("snapshot_date AS date")
    )

    assert fact_t.count() == silver_t.count()
    assert fact_a.count() == silver_s.count()
    assert stations.count() == stations.select("station_key").distinct().count()
    orphans = fact_t.join(stations, fact_t.start_station_key == stations.station_key, "left_anti")
    assert orphans.count() == 0
    assert fact_t.join(dates, "date_key", "left_anti").count() == 0

    summary = {
        r.station_code: r for r in aggregates.agg_station_availability(fact_a, stations).collect()
    }
    assert summary["31002"].full_rate == 1.0
    assert summary["31001"].empty_observations == 1

    daily = aggregates.agg_daily_ridership(fact_t, dates)
    assert daily.agg({"trips": "sum"}).first()[0] == fact_t.count()
