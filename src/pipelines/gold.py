"""Gold: star schema and the aggregates behind the dashboard."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from bikeshare_lakehouse.gold import aggregates, dimensions, facts

CATALOG = spark.conf.get("catalog")


def _read(name: str):
    return spark.read.table(f"{CATALOG}.{name}")


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_station",
    comment="One row per station code, from the live feed and from trip history.",
)
def dim_station():
    return dimensions.dim_station(_read("silver.station_information"), _read("silver.trips"))


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_date",
    comment="One row per calendar day covered by trips or availability snapshots.",
)
def dim_date():
    trip_dates = _read("silver.trips").select(F.col("start_date").alias("date"))
    snapshot_dates = _read("gold.fact_station_availability").select(
        F.col("snapshot_date").alias("date")
    )
    return dimensions.dim_date(trip_dates, snapshot_dates)


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_trips",
    comment="One row per valid trip with role-playing start and end station keys.",
)
def fact_trips():
    return facts.fact_trips(_read("silver.trips"))


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_station_availability",
    comment="One row per station per GBFS snapshot, in local Washington DC time.",
)
def fact_station_availability():
    return facts.fact_station_availability(
        _read("silver.station_status"), _read("gold.dim_station")
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_daily_ridership",
    comment="Trips per day, rider type and bike type.",
)
def agg_daily_ridership():
    return aggregates.agg_daily_ridership(_read("gold.fact_trips"), _read("gold.dim_date"))


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_hourly_station_demand",
    comment="Departures and arrivals per station, hour of day and day type.",
)
def agg_hourly_station_demand():
    return aggregates.agg_hourly_station_demand(
        _read("gold.fact_trips"), _read("gold.dim_station"), _read("gold.dim_date")
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_station_availability",
    comment="Observed empty and full frequencies per station.",
)
def agg_station_availability():
    return aggregates.agg_station_availability(
        _read("gold.fact_station_availability"), _read("gold.dim_station")
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_availability_by_hour",
    comment="Observed empty and full frequencies by hour of day across all stations.",
)
def agg_availability_by_hour():
    return aggregates.agg_availability_by_hour(_read("gold.fact_station_availability"))
