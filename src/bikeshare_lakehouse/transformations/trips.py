"""Silver transformation for trip records.

A trip is quarantined only when it cannot be used at all: no ride_id,
timestamps that do not parse, an end before the start, or a repeated ride_id.
Trips without a station are kept. Dockless e-bike trips have no station but
still count toward ridership, so they are flagged instead of rejected.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from bikeshare_lakehouse.common.schemas import TRIP_SOURCE_COLUMNS
from bikeshare_lakehouse.quality.rules import issue, quarantine_rows, valid_rows, with_issues

SECONDS_PER_DAY = 86_400
FALSE_START_SECONDS = 60

# Column order and types of silver.trips. The pipeline builds its table schema
# from this list, which is also where the ride_id column mask is attached.
SILVER_TRIP_COLUMNS = [
    ("ride_id", "STRING"),
    ("trip_key", "STRING"),
    ("rideable_type", "STRING"),
    ("member_type", "STRING"),
    ("started_at", "TIMESTAMP"),
    ("ended_at", "TIMESTAMP"),
    ("start_date", "DATE"),
    ("start_hour", "INT"),
    ("end_hour", "INT"),
    ("duration_seconds", "BIGINT"),
    ("start_station_code", "STRING"),
    ("start_station_name", "STRING"),
    ("end_station_code", "STRING"),
    ("end_station_name", "STRING"),
    ("start_lat", "DOUBLE"),
    ("start_lng", "DOUBLE"),
    ("end_lat", "DOUBLE"),
    ("end_lng", "DOUBLE"),
    ("has_start_station", "BOOLEAN"),
    ("has_end_station", "BOOLEAN"),
    ("has_end_location", "BOOLEAN"),
    ("is_round_trip", "BOOLEAN"),
    ("is_under_60s", "BOOLEAN"),
    ("is_over_24h", "BOOLEAN"),
    ("source_file", "STRING"),
    ("ingested_at", "TIMESTAMP"),
]


def silver_trip_schema(ride_id_mask: str | None = None) -> str:
    columns = []
    for name, data_type in SILVER_TRIP_COLUMNS:
        definition = f"{name} {data_type}"
        if name == "ride_id" and ride_id_mask:
            definition += f" MASK {ride_id_mask}"
        columns.append(definition)
    return ", ".join(columns)


def _text(name: str):
    return F.nullif(F.trim(F.col(name)), F.lit(""))


def _double(name: str):
    return F.expr(f"try_cast(nullif(trim({name}), '') AS DOUBLE)")


def parse_trips(bronze: DataFrame) -> DataFrame:
    """Type the raw string columns. Values that do not parse become null."""
    return bronze.select(
        F.struct(*TRIP_SOURCE_COLUMNS).alias("source_record"),
        _text("ride_id").alias("ride_id"),
        F.lower(_text("rideable_type")).alias("rideable_type"),
        F.lower(_text("member_casual")).alias("member_type"),
        F.try_to_timestamp(_text("started_at")).alias("started_at"),
        F.try_to_timestamp(_text("ended_at")).alias("ended_at"),
        _text("start_station_id").alias("start_station_code"),
        _text("start_station_name").alias("start_station_name"),
        _text("end_station_id").alias("end_station_code"),
        _text("end_station_name").alias("end_station_name"),
        _double("start_lat").alias("start_lat"),
        _double("start_lng").alias("start_lng"),
        _double("end_lat").alias("end_lat"),
        _double("end_lng").alias("end_lng"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def check_trips(parsed: DataFrame) -> DataFrame:
    """Add the dq_issues column.

    When a ride_id appears more than once, the copy ingested first is kept and
    the others are quarantined as duplicates.
    """
    first_copy = Window.partitionBy("ride_id").orderBy("ingested_at", "source_file")
    df = parsed.withColumn("_copy", F.row_number().over(first_copy))
    df = with_issues(
        df,
        issue(F.col("ride_id").isNull(), "missing_ride_id"),
        issue(F.col("started_at").isNull(), "invalid_started_at"),
        issue(F.col("ended_at").isNull(), "invalid_ended_at"),
        issue(F.col("ended_at") < F.col("started_at"), "ended_before_started"),
        issue(F.col("ride_id").isNotNull() & (F.col("_copy") > 1), "duplicate_ride_id"),
    )
    return df.drop("_copy")


def silver_trips(checked: DataFrame) -> DataFrame:
    df = valid_rows(checked)
    duration = F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")
    has_start = F.col("start_station_code").isNotNull()
    has_end = F.col("end_station_code").isNotNull()
    df = (
        df.withColumn("trip_key", F.sha2(F.col("ride_id"), 256))
        .withColumn("start_date", F.to_date("started_at"))
        .withColumn("start_hour", F.hour("started_at"))
        .withColumn("end_hour", F.hour("ended_at"))
        .withColumn("duration_seconds", duration.cast("bigint"))
        .withColumn("has_start_station", has_start)
        .withColumn("has_end_station", has_end)
        .withColumn("has_end_location", F.col("end_lat").isNotNull() & F.col("end_lng").isNotNull())
        .withColumn(
            "is_round_trip",
            F.coalesce(
                has_start & has_end & (F.col("start_station_code") == F.col("end_station_code")),
                F.lit(False),
            ),
        )
        .withColumn("is_under_60s", F.col("duration_seconds") < FALSE_START_SECONDS)
        .withColumn("is_over_24h", F.col("duration_seconds") > SECONDS_PER_DAY)
    )
    return df.select(*(name for name, _ in SILVER_TRIP_COLUMNS))


def trips_quarantine(checked: DataFrame) -> DataFrame:
    return quarantine_rows(checked, F.to_json("source_record"))
