"""Bronze: raw landing files loaded incrementally with Auto Loader.

Streaming tables read each new file once. Auto Loader keeps track of which
files it has seen, so a rerun with no new files adds no rows.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from bikeshare_lakehouse.common.schemas import TRIP_SOURCE_SCHEMA

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_path").rstrip("/")


def _with_ingestion_metadata(df):
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


@dp.table(
    name=f"{CATALOG}.bronze.trips_raw",
    comment="Trip CSV rows exactly as published, all columns as strings.",
)
def trips_raw():
    return _with_ingestion_metadata(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "*.csv")
        .schema(TRIP_SOURCE_SCHEMA)
        .load(f"{LANDING}/trips/")
    )


def _gbfs_files(feed: str):
    return _with_ingestion_metadata(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.json")
        .load(f"{LANDING}/gbfs/{feed}/")
        .withColumnRenamed("value", "raw_json")
    )


@dp.table(
    name=f"{CATALOG}.bronze.station_status_raw",
    comment="One row per collected station_status file, stored as raw JSON text.",
)
def station_status_raw():
    return _gbfs_files("station_status")


@dp.table(
    name=f"{CATALOG}.bronze.station_information_raw",
    comment="One row per collected station_information file, stored as raw JSON text.",
)
def station_information_raw():
    return _gbfs_files("station_information")
