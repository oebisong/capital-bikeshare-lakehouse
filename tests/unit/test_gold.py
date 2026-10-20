from datetime import date, datetime

import pytest

from bikeshare_lakehouse.gold import aggregates, dimensions, facts
from bikeshare_lakehouse.transformations import station_information, trips
from tests.conftest import gbfs_file, info_station, trip_row

pytestmark = pytest.mark.spark

T0 = 1_792_440_000


@pytest.fixture
def silver_trips(bronze_trips):
    rows = [
        trip_row(ride_id="A1"),
        trip_row(ride_id="A2", start_station_id="31002", end_station_id="31001"),
        trip_row(ride_id="A3", start_station_id="39999", start_station_name="Closed Station"),
        trip_row(ride_id="A4", start_station_id=None, start_station_name=None),
    ]
    return trips.silver_trips(trips.check_trips(trips.parse_trips(bronze_trips(rows))))


@pytest.fixture
def station_versions(bronze_gbfs):
    stations = [info_station("uuid-1", "31001"), info_station("uuid-2", "31002", "Station Two")]
    bronze = bronze_gbfs([gbfs_file("station_information", T0, stations)])
    return station_information.silver_station_information(
        station_information.check_station_information(bronze)
    )


@pytest.fixture
def stations(station_versions, silver_trips):
    return dimensions.dim_station(station_versions, silver_trips)


def test_dim_station_has_live_historical_and_special_members(stations):
    rows = {r.station_code: r for r in stations.collect()}
    assert rows["31001"].in_live_feed and rows["31001"].gbfs_station_id == "uuid-1"
    assert rows["39999"].in_live_feed is False
    assert rows["39999"].station_name == "Closed Station"
    assert rows["NONE"].station_key == dimensions.NO_STATION_KEY
    assert stations.count() == stations.select("station_key").distinct().count()


def test_station_key_is_stable_and_positive(spark):
    df = spark.createDataFrame([("31001",), ("31001",), (None,)], "code STRING")
    keys = [r.k for r in df.select(dimensions.station_key(df.code).alias("k")).collect()]
    assert keys[0] == keys[1] and keys[0] > 0
    assert keys[2] == dimensions.NO_STATION_KEY


def test_fact_trips_keeps_one_row_per_trip(silver_trips, stations):
    fact = facts.fact_trips(silver_trips)
    assert fact.count() == silver_trips.count()
    joined = fact.join(stations, fact.start_station_key == stations.station_key)
    assert joined.count() == fact.count()


def test_dockless_trip_uses_no_station_member(silver_trips):
    fact = facts.fact_trips(silver_trips)
    keys = {r.trip_key: r.start_station_key for r in fact.collect()}
    dockless = silver_trips.where("ride_id = 'A4'").first().trip_key
    assert keys[dockless] == dimensions.NO_STATION_KEY


def test_dim_date_covers_every_day_between_bounds(spark):
    a = spark.createDataFrame([(date(2025, 4, 1),)], "date DATE")
    b = spark.createDataFrame([(date(2025, 4, 3),), (date(2025, 4, 6),)], "date DATE")
    rows = dimensions.dim_date(a, b).orderBy("date").collect()
    assert len(rows) == 6
    assert rows[0].date_key == 20250401
    assert rows[0].day_name == "Tuesday" and rows[0].day_of_week == 2
    assert rows[4].is_weekend and rows[4].day_name == "Saturday"


def test_hourly_demand_counts_each_trip_once_per_side(silver_trips, stations):
    fact = facts.fact_trips(silver_trips)
    dates = dimensions.dim_date(silver_trips.selectExpr("start_date AS date"))
    demand = aggregates.agg_hourly_station_demand(fact, stations, dates)
    totals = demand.agg({"departures": "sum", "arrivals": "sum"}).first()
    assert totals["sum(departures)"] == 3  # the dockless departure is excluded
    assert totals["sum(arrivals)"] == 4


def test_availability_rates_are_observed_frequencies(spark, stations):
    key = stations.where("station_code = '31001'").first().station_key
    rows = [
        ("uuid-1", datetime(2026, 10, 19, 14, 0), 0, 10, True, False),
        ("uuid-1", datetime(2026, 10, 19, 14, 15), 2, 8, True, False),
        ("uuid-1", datetime(2026, 10, 19, 14, 30), 5, 0, True, False),
        ("uuid-1", datetime(2026, 10, 19, 14, 45), 0, 10, True, True),
    ]
    status = spark.createDataFrame(
        rows,
        "station_id STRING, snapshot_at TIMESTAMP, num_bikes_available BIGINT, "
        "num_docks_available BIGINT, is_installed BOOLEAN, is_stale BOOLEAN",
    ).selectExpr(
        "*",
        "1L AS num_ebikes_available",
        "0L AS num_bikes_disabled",
        "0L AS num_docks_disabled",
        "true AS is_renting",
        "true AS is_returning",
        "0L AS report_age_seconds",
    )
    fact = facts.fact_station_availability(status, stations)
    assert fact.count() == 4
    first = fact.orderBy("snapshot_at").first()
    assert first.station_key == key
    assert first.snapshot_hour == 10  # 14:00 UTC is 10:00 in Washington DC in October

    summary = aggregates.agg_station_availability(fact, stations).first()
    assert summary.observations == 3  # the stale report is excluded
    assert summary.empty_observations == 1
    assert summary.full_observations == 1
    assert summary.empty_rate == pytest.approx(0.3333)


def test_unknown_live_station_maps_to_unmatched_member(spark, stations):
    status = spark.createDataFrame(
        [("uuid-404", datetime(2026, 10, 19, 14, 0))], "station_id STRING, snapshot_at TIMESTAMP"
    ).selectExpr(
        "*",
        "1L AS num_bikes_available",
        "1L AS num_ebikes_available",
        "0L AS num_bikes_disabled",
        "1L AS num_docks_available",
        "0L AS num_docks_disabled",
        "true AS is_installed",
        "true AS is_renting",
        "true AS is_returning",
        "false AS is_stale",
        "0L AS report_age_seconds",
    )
    fact = facts.fact_station_availability(status, stations)
    assert fact.first().station_key == dimensions.UNMATCHED_LIVE_STATION_KEY
