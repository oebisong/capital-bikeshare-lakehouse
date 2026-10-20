import json
import os
import sys
import time
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from bikeshare_lakehouse.common.schemas import TRIP_SOURCE_COLUMNS

INGESTED_AT = datetime(2025, 7, 1, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    # Spark workers must use the same interpreter as the tests.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # PySpark converts naive datetimes using the machine's local time zone.
    # Pinning the process to UTC makes results the same on every machine.
    os.environ["TZ"] = "UTC"
    time.tzset()
    session = (
        SparkSession.builder.master("local[2]")
        .appName("bikeshare-lakehouse-tests")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def trip_row(**overrides):
    row = {
        "ride_id": "A1",
        "rideable_type": "classic_bike",
        "started_at": "2025-04-01 08:00:00.000",
        "ended_at": "2025-04-01 08:15:00.000",
        "start_station_name": "Station One",
        "start_station_id": "31001",
        "end_station_name": "Station Two",
        "end_station_id": "31002",
        "start_lat": "38.90",
        "start_lng": "-77.03",
        "end_lat": "38.91",
        "end_lng": "-77.04",
        "member_casual": "member",
        "_source_file": "trips/202504-capitalbikeshare-tripdata.csv",
        "_ingested_at": INGESTED_AT,
    }
    row.update(overrides)
    return row


@pytest.fixture
def bronze_trips(spark):
    def build(rows):
        columns = [*TRIP_SOURCE_COLUMNS, "_source_file", "_ingested_at"]
        schema = ", ".join(f"{c} STRING" for c in TRIP_SOURCE_COLUMNS)
        schema += ", _source_file STRING, _ingested_at TIMESTAMP"
        return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)

    return build


def gbfs_file(feed, last_updated, stations, collected_at="2026-10-19T10:00:05Z"):
    return json.dumps(
        {
            "collected_at": collected_at,
            "feed": feed,
            "source_url": f"https://example.test/{feed}.json",
            "http_status": 200,
            "response": {"last_updated": last_updated, "ttl": 60, "data": {"stations": stations}},
        }
    )


def status_station(station_id="uuid-1", bikes=3, docks=12, last_reported=None, **extra):
    station = {
        "station_id": station_id,
        "num_bikes_available": bikes,
        "num_ebikes_available": 1,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": last_reported,
    }
    station.update(extra)
    return station


def info_station(station_id="uuid-1", short_name="31001", name="Station One", **extra):
    station = {
        "station_id": station_id,
        "short_name": short_name,
        "name": name,
        "lat": 38.90,
        "lon": -77.03,
        "capacity": 15,
        "region_id": "42",
        "station_type": "classic",
    }
    station.update(extra)
    return station


@pytest.fixture
def bronze_gbfs(spark):
    def build(files):
        rows = [(raw, f"gbfs/file_{i}.json", INGESTED_AT) for i, raw in enumerate(files)]
        return spark.createDataFrame(
            rows, "raw_json STRING, _source_file STRING, _ingested_at TIMESTAMP"
        )

    return build
