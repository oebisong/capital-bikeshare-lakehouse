"""Silver: typed, validated records plus quarantine tables for rejected rows.

Each source is checked once in a temporary view. The silver table takes the
rows with no issues and the quarantine table takes the rest, so the two always
add up to bronze.
"""

from pyspark import pipelines as dp

from bikeshare_lakehouse.transformations import station_information, station_status, trips

CATALOG = spark.conf.get("catalog")
RIDE_ID_MASK = f"`{CATALOG}`.silver.mask_ride_id"


@dp.temporary_view(name="trips_checked")
def trips_checked():
    bronze = spark.read.table(f"{CATALOG}.bronze.trips_raw")
    return trips.check_trips(trips.parse_trips(bronze))


@dp.materialized_view(
    name=f"{CATALOG}.silver.trips",
    comment="One row per valid trip. ride_id is masked outside the data engineers group.",
    schema=trips.silver_trip_schema(ride_id_mask=RIDE_ID_MASK),
)
@dp.expect_or_fail("non_negative_duration", "duration_seconds >= 0")
@dp.expect("has_start_station", "has_start_station")
def silver_trips():
    return trips.silver_trips(spark.read.table("trips_checked"))


@dp.materialized_view(
    name=f"{CATALOG}.quarantine.trips",
    comment="Trip rows that failed validation, with the reasons and the original record.",
)
def silver_trips_quarantine():
    return trips.trips_quarantine(spark.read.table("trips_checked"))


@dp.temporary_view(name="station_status_checked")
def station_status_checked():
    bronze = spark.read.table(f"{CATALOG}.bronze.station_status_raw")
    return station_status.check_station_status(bronze)


@dp.materialized_view(
    name=f"{CATALOG}.silver.station_status",
    comment="One row per station per GBFS snapshot.",
)
@dp.expect_or_fail("non_negative_bikes", "num_bikes_available >= 0")
@dp.expect("fresh_report", "NOT is_stale")
def silver_station_status():
    return station_status.silver_station_status(spark.read.table("station_status_checked"))


@dp.materialized_view(
    name=f"{CATALOG}.quarantine.station_status",
    comment="Station status rows that failed validation.",
)
def silver_station_status_quarantine():
    return station_status.station_status_quarantine(spark.read.table("station_status_checked"))


@dp.temporary_view(name="station_information_checked")
def station_information_checked():
    bronze = spark.read.table(f"{CATALOG}.bronze.station_information_raw")
    return station_information.check_station_information(bronze)


@dp.materialized_view(
    name=f"{CATALOG}.silver.station_information",
    comment="One row per distinct version of each station's details.",
)
def silver_station_information():
    return station_information.silver_station_information(
        spark.read.table("station_information_checked")
    )


@dp.materialized_view(
    name=f"{CATALOG}.quarantine.station_information",
    comment="Station information rows that failed validation.",
)
def silver_station_information_quarantine():
    return station_information.station_information_quarantine(
        spark.read.table("station_information_checked")
    )
