"""Source schemas.

Bronze keeps the trip CSV as strings and each GBFS file as raw text. Types are
applied in silver, where a bad value becomes a quarantined record instead of a
failed load.
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRIP_SOURCE_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]

TRIP_SOURCE_SCHEMA = StructType([StructField(name, StringType()) for name in TRIP_SOURCE_COLUMNS])

# Flags are read as strings because GBFS publishers send either 0/1 or true/false.
STATION_STATUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("num_bikes_available", LongType()),
        StructField("num_ebikes_available", LongType()),
        StructField("num_bikes_disabled", LongType()),
        StructField("num_docks_available", LongType()),
        StructField("num_docks_disabled", LongType()),
        StructField("is_installed", StringType()),
        StructField("is_renting", StringType()),
        StructField("is_returning", StringType()),
        StructField("last_reported", LongType()),
    ]
)

STATION_INFORMATION_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("short_name", StringType()),
        StructField("name", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("capacity", LongType()),
        StructField("region_id", StringType()),
        StructField("station_type", StringType()),
    ]
)


def gbfs_envelope_schema(station_schema: StructType) -> StructType:
    return StructType(
        [
            StructField("collected_at", StringType()),
            StructField("feed", StringType()),
            StructField("source_url", StringType()),
            StructField("http_status", LongType()),
            StructField(
                "response",
                StructType(
                    [
                        StructField("last_updated", LongType()),
                        StructField("ttl", LongType()),
                        StructField("version", StringType()),
                        StructField(
                            "data",
                            StructType([StructField("stations", ArrayType(station_schema))]),
                        ),
                    ]
                ),
            ),
        ]
    )
