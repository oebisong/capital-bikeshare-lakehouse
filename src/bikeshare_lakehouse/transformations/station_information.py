"""Silver transformation for GBFS station_information snapshots.

Station details rarely change, and the collector stores a fresh copy on every
poll. Silver collapses those copies into versions. A version is one distinct
set of station attributes, with the first and last snapshot that reported it.
A renamed or moved station therefore shows up as a second version, without
the cost of a full slowly changing dimension.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from bikeshare_lakehouse.common.schemas import STATION_INFORMATION_SCHEMA
from bikeshare_lakehouse.quality.rules import issue, quarantine_rows, valid_rows, with_issues
from bikeshare_lakehouse.transformations.station_status import explode_snapshots

# Bounding box around the Capital Bikeshare service area with a wide margin.
LATITUDE_RANGE = (38.0, 40.0)
LONGITUDE_RANGE = (-78.5, -76.0)

VERSION_ATTRIBUTES = [
    "station_id",
    "station_code",
    "station_name",
    "latitude",
    "longitude",
    "capacity",
    "region_id",
    "station_type",
]


def check_station_information(bronze: DataFrame) -> DataFrame:
    df = explode_snapshots(bronze, STATION_INFORMATION_SCHEMA).select(
        "*",
        F.col("station.station_id").alias("station_id"),
        F.nullif(F.trim("station.short_name"), F.lit("")).alias("station_code"),
        F.trim("station.name").alias("station_name"),
        F.col("station.lat").alias("latitude"),
        F.col("station.lon").alias("longitude"),
        F.col("station.capacity").alias("capacity"),
        F.col("station.region_id").alias("region_id"),
        F.col("station.station_type").alias("station_type"),
    )
    in_area = F.col("latitude").between(*LATITUDE_RANGE) & F.col("longitude").between(
        *LONGITUDE_RANGE
    )
    has_station = F.col("station").isNotNull()
    return with_issues(
        df,
        issue(F.col("snapshot_at").isNull(), "unparseable_snapshot"),
        issue(F.col("snapshot_at").isNotNull() & ~has_station, "empty_snapshot"),
        issue(has_station & F.col("station_id").isNull(), "missing_station_id"),
        issue(has_station & F.col("station_code").isNull(), "missing_station_code"),
        issue(has_station & F.col("station_name").isNull(), "missing_station_name"),
        issue(has_station & ~F.coalesce(in_area, F.lit(False)), "coordinates_out_of_range"),
        issue(F.col("capacity") < 0, "negative_capacity"),
    )


def silver_station_information(checked: DataFrame) -> DataFrame:
    versions = (
        valid_rows(checked)
        .groupBy(*VERSION_ATTRIBUTES)
        .agg(
            F.min("snapshot_at").alias("first_seen_at"),
            F.max("snapshot_at").alias("last_seen_at"),
            F.count("*").alias("snapshot_count"),
        )
    )
    latest = Window.partitionBy("station_id").orderBy(F.col("last_seen_at").desc())
    return versions.withColumn("is_current", F.row_number().over(latest) == 1)


def station_information_quarantine(checked: DataFrame) -> DataFrame:
    record = F.when(F.col("station").isNotNull(), F.to_json("station")).otherwise(F.col("raw_json"))
    return quarantine_rows(checked, record)
