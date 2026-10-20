"""Silver transformation for GBFS station_status snapshots.

One bronze row holds one collected file. Silver expands it to one row per
station per snapshot.

The grain is (station_id, snapshot_at), where snapshot_at is the feed's own
last_updated time. Two polls can return the same feed state if the second poll
arrives before the feed refreshes. Those rows share a snapshot_at and are
duplicates. Two snapshots with different snapshot_at values and identical counts
are separate observations of a station that did not change, so both are kept.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from bikeshare_lakehouse.common.schemas import STATION_STATUS_SCHEMA, gbfs_envelope_schema
from bikeshare_lakehouse.quality.rules import issue, quarantine_rows, valid_rows, with_issues

STALE_AFTER_SECONDS = 3_600
FUTURE_TOLERANCE_SECONDS = 300

COUNT_COLUMNS = [
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
]


def _flag(value):
    """GBFS flags arrive as 0/1 or true/false. Anything else becomes null."""
    text = F.lower(F.trim(value))
    return (
        F.when(text.isin("1", "true"), F.lit(True))
        .when(text.isin("0", "false"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def explode_snapshots(bronze: DataFrame, station_schema) -> DataFrame:
    """Parse the envelope and expand the stations array.

    explode_outer keeps a file that fails to parse as a single row with no
    station, so it reaches quarantine instead of disappearing.
    """
    parsed = bronze.select(
        F.from_json("raw_json", gbfs_envelope_schema(station_schema)).alias("envelope"),
        F.col("raw_json"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )
    return parsed.select(
        F.try_to_timestamp(F.col("envelope.collected_at")).alias("collected_at"),
        F.timestamp_seconds(F.col("envelope.response.last_updated")).alias("snapshot_at"),
        F.explode_outer("envelope.response.data.stations").alias("station"),
        "raw_json",
        "source_file",
        "ingested_at",
    )


def check_station_status(bronze: DataFrame) -> DataFrame:
    df = explode_snapshots(bronze, STATION_STATUS_SCHEMA).select(
        "*",
        *(F.col(f"station.{name}").alias(name) for name in ["station_id", *COUNT_COLUMNS]),
        _flag(F.col("station.is_installed")).alias("is_installed"),
        _flag(F.col("station.is_renting")).alias("is_renting"),
        _flag(F.col("station.is_returning")).alias("is_returning"),
        F.timestamp_seconds(F.col("station.last_reported")).alias("last_reported_at"),
    )

    first_copy = Window.partitionBy("station_id", "snapshot_at").orderBy(
        "collected_at", "source_file"
    )
    df = df.withColumn("_copy", F.row_number().over(first_copy))

    negative_bikes = (
        (F.col("num_bikes_available") < 0)
        | (F.col("num_ebikes_available") < 0)
        | (F.col("num_bikes_disabled") < 0)
    )
    negative_docks = (F.col("num_docks_available") < 0) | (F.col("num_docks_disabled") < 0)
    reported_in_future = F.col("last_reported_at") > F.col("snapshot_at") + F.expr(
        f"INTERVAL {FUTURE_TOLERANCE_SECONDS} SECONDS"
    )
    df = with_issues(
        df,
        issue(F.col("snapshot_at").isNull(), "unparseable_snapshot"),
        issue(F.col("snapshot_at").isNotNull() & F.col("station").isNull(), "empty_snapshot"),
        issue(F.col("station").isNotNull() & F.col("station_id").isNull(), "missing_station_id"),
        issue(
            F.col("station").isNotNull() & F.col("num_bikes_available").isNull(),
            "missing_bikes_count",
        ),
        issue(
            F.col("station").isNotNull() & F.col("num_docks_available").isNull(),
            "missing_docks_count",
        ),
        issue(negative_bikes, "negative_bike_count"),
        issue(negative_docks, "negative_dock_count"),
        issue(F.col("station").isNotNull() & F.col("is_installed").isNull(), "invalid_status_flag"),
        issue(reported_in_future, "reported_after_snapshot"),
        issue(F.col("station_id").isNotNull() & (F.col("_copy") > 1), "duplicate_observation"),
    )
    return df.drop("_copy")


def silver_station_status(checked: DataFrame) -> DataFrame:
    age_seconds = F.unix_timestamp("snapshot_at") - F.unix_timestamp("last_reported_at")
    return valid_rows(checked).select(
        "station_id",
        "snapshot_at",
        "collected_at",
        "last_reported_at",
        age_seconds.cast("bigint").alias("report_age_seconds"),
        F.coalesce(age_seconds > STALE_AFTER_SECONDS, F.lit(False)).alias("is_stale"),
        *COUNT_COLUMNS,
        "is_installed",
        "is_renting",
        "is_returning",
        "source_file",
        "ingested_at",
    )


def station_status_quarantine(checked: DataFrame) -> DataFrame:
    record = F.when(F.col("station").isNotNull(), F.to_json("station")).otherwise(F.col("raw_json"))
    return quarantine_rows(checked, record)
