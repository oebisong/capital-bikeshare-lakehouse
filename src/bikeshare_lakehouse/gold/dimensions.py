"""Gold dimensions: dim_station and dim_date.

Trip files identify a station by its short code, for example 31818. The GBFS
feed identifies it by a UUID and carries the short code as short_name.
dim_station is keyed on the short code, so both sources join to it.

Some stations in the trip history no longer appear in the live feed. They are
added from the trip records themselves, so every trip still finds its station.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

NO_STATION_KEY = 0
UNMATCHED_LIVE_STATION_KEY = -1

DIM_STATION_COLUMNS = [
    "station_key",
    "station_code",
    "gbfs_station_id",
    "station_name",
    "latitude",
    "longitude",
    "capacity",
    "region_id",
    "station_type",
    "in_live_feed",
]


def station_key(code: Column) -> Column:
    """Stable surrogate key derived from the station code.

    A hash keeps the key identical across full refreshes of the materialized
    view, which a generated sequence would not.
    """
    positive = F.xxhash64(code).bitwiseAND(F.lit(0x7FFFFFFFFFFFFFFF))
    return F.when(code.isNull(), F.lit(NO_STATION_KEY).cast("bigint")).otherwise(positive)


def _special_members(spark) -> DataFrame:
    rows = [
        (NO_STATION_KEY, "NONE", "No station (dockless trip)"),
        (UNMATCHED_LIVE_STATION_KEY, "UNMATCHED", "Live station missing from station_information"),
    ]
    df = spark.createDataFrame(rows, "station_key BIGINT, station_code STRING, station_name STRING")
    return df.select(
        "station_key",
        "station_code",
        F.lit(None).cast("string").alias("gbfs_station_id"),
        "station_name",
        F.lit(None).cast("double").alias("latitude"),
        F.lit(None).cast("double").alias("longitude"),
        F.lit(None).cast("bigint").alias("capacity"),
        F.lit(None).cast("string").alias("region_id"),
        F.lit(None).cast("string").alias("station_type"),
        F.lit(False).alias("in_live_feed"),
    )


def dim_station(station_versions: DataFrame, trips: DataFrame) -> DataFrame:
    latest_per_code = Window.partitionBy("station_code").orderBy(F.col("last_seen_at").desc())
    live = (
        station_versions.where("is_current")
        .withColumn("_rank", F.row_number().over(latest_per_code))
        .where("_rank = 1")
        .select(
            station_key(F.col("station_code")).alias("station_key"),
            "station_code",
            F.col("station_id").alias("gbfs_station_id"),
            "station_name",
            "latitude",
            "longitude",
            "capacity",
            "region_id",
            "station_type",
            F.lit(True).alias("in_live_feed"),
        )
    )

    trip_stations = trips.select(
        F.col("start_station_code").alias("station_code"),
        F.col("start_station_name").alias("station_name"),
        F.col("start_lat").alias("latitude"),
        F.col("start_lng").alias("longitude"),
        F.col("started_at").alias("seen_at"),
    ).unionByName(
        trips.select(
            F.col("end_station_code").alias("station_code"),
            F.col("end_station_name").alias("station_name"),
            F.col("end_lat").alias("latitude"),
            F.col("end_lng").alias("longitude"),
            F.col("ended_at").alias("seen_at"),
        )
    )
    historical = (
        trip_stations.where(F.col("station_code").isNotNull())
        .groupBy("station_code")
        .agg(
            F.max_by("station_name", "seen_at").alias("station_name"),
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
        )
        .join(live.select("station_code"), "station_code", "left_anti")
        .select(
            station_key(F.col("station_code")).alias("station_key"),
            "station_code",
            F.lit(None).cast("string").alias("gbfs_station_id"),
            "station_name",
            "latitude",
            "longitude",
            F.lit(None).cast("bigint").alias("capacity"),
            F.lit(None).cast("string").alias("region_id"),
            F.lit(None).cast("string").alias("station_type"),
            F.lit(False).alias("in_live_feed"),
        )
    )

    return (
        live.unionByName(historical)
        .unionByName(_special_members(trips.sparkSession))
        .select(*DIM_STATION_COLUMNS)
    )


def date_key(date_column: Column) -> Column:
    return F.date_format(date_column, "yyyyMMdd").cast("int")


def dim_date(*date_frames: DataFrame) -> DataFrame:
    """Build one row per calendar day between the earliest and latest date seen.

    Each input frame has a single column named date.
    """
    dates = date_frames[0]
    for frame in date_frames[1:]:
        dates = dates.unionByName(frame)
    bounds = dates.agg(F.min("date").alias("first"), F.max("date").alias("last"))
    days = bounds.where(F.col("first").isNotNull()).select(
        F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 DAY"))).alias("date")
    )
    day_of_week = (F.dayofweek("date") + 5) % 7 + 1  # Monday = 1, Sunday = 7
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        day_of_week.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (day_of_week >= 6).alias("is_weekend"),
    )
