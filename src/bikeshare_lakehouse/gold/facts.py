"""Gold facts.

fact_trips has one row per valid trip. Station keys come from the station code
directly, so no join can add or remove rows.

fact_station_availability has one row per station per GBFS snapshot. The only
join is to dim_station on gbfs_station_id, which is unique in the dimension.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bikeshare_lakehouse.gold.dimensions import UNMATCHED_LIVE_STATION_KEY, date_key, station_key

LOCAL_TIMEZONE = "America/New_York"


def fact_trips(trips: DataFrame) -> DataFrame:
    return trips.select(
        "trip_key",
        date_key(F.col("start_date")).alias("date_key"),
        F.col("start_date").alias("trip_date"),
        "start_hour",
        "end_hour",
        station_key(F.col("start_station_code")).alias("start_station_key"),
        station_key(F.col("end_station_code")).alias("end_station_key"),
        "rideable_type",
        "member_type",
        "started_at",
        "ended_at",
        "duration_seconds",
        (F.col("duration_seconds") / 60.0).alias("duration_minutes"),
        "has_start_station",
        "has_end_station",
        "is_round_trip",
        "is_under_60s",
        "is_over_24h",
    )


def fact_station_availability(
    station_status: DataFrame, stations: DataFrame, timezone: str = LOCAL_TIMEZONE
) -> DataFrame:
    """Trip timestamps in the source are local Washington DC time. GBFS times are
    Unix epochs. Snapshots are converted to local time so hours and dates line
    up with the trip data.
    """
    live_stations = stations.where(F.col("gbfs_station_id").isNotNull()).select(
        "gbfs_station_id", "station_key", "capacity"
    )
    local = F.from_utc_timestamp("snapshot_at", timezone)
    df = station_status.join(
        F.broadcast(live_stations),
        station_status["station_id"] == live_stations["gbfs_station_id"],
        "left",
    )
    installed = F.coalesce(F.col("is_installed"), F.lit(False))
    return df.select(
        F.coalesce("station_key", F.lit(UNMATCHED_LIVE_STATION_KEY).cast("bigint")).alias(
            "station_key"
        ),
        F.col("station_id").alias("gbfs_station_id"),
        "snapshot_at",
        local.alias("snapshot_local_at"),
        date_key(F.to_date(local)).alias("date_key"),
        F.to_date(local).alias("snapshot_date"),
        F.hour(local).alias("snapshot_hour"),
        "num_bikes_available",
        "num_ebikes_available",
        "num_bikes_disabled",
        "num_docks_available",
        "num_docks_disabled",
        "capacity",
        "is_installed",
        "is_renting",
        "is_returning",
        "is_stale",
        "report_age_seconds",
        (installed & (F.col("num_bikes_available") == 0)).alias("is_empty"),
        (installed & (F.col("num_docks_available") == 0)).alias("is_full"),
    )
