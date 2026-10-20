"""Gold aggregates read by the dashboard.

Availability rates are observed frequencies: the share of collected snapshots
in which a station reported zero bikes or zero docks. They are not the share of
time a station was empty or full, because state changes between polls are not
seen. Stale reports and stations that are not installed are excluded.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bikeshare_lakehouse.gold.dimensions import NO_STATION_KEY


def _rate(flag: str):
    return F.round(F.avg(F.col(flag).cast("double")), 4)


def agg_daily_ridership(trips: DataFrame, dates: DataFrame) -> DataFrame:
    daily = trips.groupBy("date_key", "trip_date", "member_type", "rideable_type").agg(
        F.count("*").alias("trips"),
        F.round(F.avg(F.when(~F.col("is_over_24h"), F.col("duration_minutes"))), 2).alias(
            "avg_duration_minutes"
        ),
        F.sum(F.col("is_round_trip").cast("int")).alias("round_trips"),
    )
    return daily.join(dates.select("date_key", "day_name", "day_of_week", "is_weekend"), "date_key")


def agg_hourly_station_demand(trips: DataFrame, stations: DataFrame, dates: DataFrame) -> DataFrame:
    """Departures and arrivals per station, hour of day and day type.

    Departures and arrivals are counted separately and then joined on the same
    key, so a trip is counted once on each side and never multiplied.
    """
    with_day = trips.join(dates.select("date_key", "is_weekend"), "date_key")
    departures = with_day.groupBy(
        F.col("start_station_key").alias("station_key"),
        F.col("start_hour").alias("hour_of_day"),
        "is_weekend",
    ).agg(F.count("*").alias("departures"))
    arrivals = with_day.groupBy(
        F.col("end_station_key").alias("station_key"),
        F.col("end_hour").alias("hour_of_day"),
        "is_weekend",
    ).agg(F.count("*").alias("arrivals"))
    day_counts = with_day.groupBy("is_weekend").agg(F.countDistinct("trip_date").alias("days"))
    demand = (
        departures.join(arrivals, ["station_key", "hour_of_day", "is_weekend"], "full_outer")
        .fillna(0, ["departures", "arrivals"])
        .where(F.col("station_key") != NO_STATION_KEY)
        .join(day_counts, "is_weekend")
    )
    return demand.join(
        stations.select("station_key", "station_code", "station_name"), "station_key"
    ).select(
        "station_key",
        "station_code",
        "station_name",
        "hour_of_day",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday").alias("day_type"),
        "departures",
        "arrivals",
        F.round(F.col("departures") / F.col("days"), 2).alias("avg_departures_per_day"),
        F.round(F.col("arrivals") / F.col("days"), 2).alias("avg_arrivals_per_day"),
    )


def _usable_observations(availability: DataFrame) -> DataFrame:
    return availability.where(
        F.coalesce(F.col("is_installed"), F.lit(False)) & ~F.col("is_stale")
    ).where(F.col("station_key") > NO_STATION_KEY)


def agg_station_availability(availability: DataFrame, stations: DataFrame) -> DataFrame:
    summary = (
        _usable_observations(availability)
        .groupBy("station_key")
        .agg(
            F.count("*").alias("observations"),
            F.sum(F.col("is_empty").cast("int")).alias("empty_observations"),
            F.sum(F.col("is_full").cast("int")).alias("full_observations"),
            _rate("is_empty").alias("empty_rate"),
            _rate("is_full").alias("full_rate"),
            F.round(F.avg("num_bikes_available"), 2).alias("avg_bikes_available"),
            F.round(F.avg("num_docks_available"), 2).alias("avg_docks_available"),
            F.min("snapshot_local_at").alias("first_observed_at"),
            F.max("snapshot_local_at").alias("last_observed_at"),
        )
    )
    return summary.join(
        stations.select(
            "station_key", "station_code", "station_name", "capacity", "latitude", "longitude"
        ),
        "station_key",
    )


def agg_availability_by_hour(availability: DataFrame) -> DataFrame:
    weekend = F.dayofweek("snapshot_date").isin(1, 7)
    return (
        _usable_observations(availability)
        .withColumn("day_type", F.when(weekend, "Weekend").otherwise("Weekday"))
        .groupBy(F.col("snapshot_hour").alias("hour_of_day"), "day_type")
        .agg(
            F.count("*").alias("observations"),
            _rate("is_empty").alias("empty_rate"),
            _rate("is_full").alias("full_rate"),
            F.round(F.avg("num_bikes_available"), 2).alias("avg_bikes_available"),
        )
    )
