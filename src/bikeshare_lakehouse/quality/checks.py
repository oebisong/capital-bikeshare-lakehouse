"""Cross-table checks that run after each pipeline refresh.

Row-level rules live in silver. These checks cover properties that only show
up across tables: nothing lost between layers, no duplicated grain and no
orphaned keys. Any failure fails the job task, which triggers the failure
notification.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

from bikeshare_lakehouse.common.logs import configure_logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str  # returns one row with a single column named failures

    def render(self, catalog: str) -> str:
        return self.sql.format(c=f"`{catalog}`")


CHECKS = [
    Check(
        "every bronze trip lands in silver or quarantine",
        """SELECT abs((SELECT count(*) FROM {c}.bronze.trips_raw)
                    - (SELECT count(*) FROM {c}.silver.trips)
                    - (SELECT count(*) FROM {c}.quarantine.trips)) AS failures""",
    ),
    Check(
        "fact_trips keeps the grain of silver.trips",
        """SELECT abs((SELECT count(*) FROM {c}.silver.trips)
                    - (SELECT count(*) FROM {c}.gold.fact_trips)) AS failures""",
    ),
    Check(
        "fact_trips has one row per trip_key",
        """SELECT count(*) - count(DISTINCT trip_key) AS failures FROM {c}.gold.fact_trips""",
    ),
    Check(
        "dim_station has one row per station_key",
        """SELECT count(*) - count(DISTINCT station_key) AS failures FROM {c}.gold.dim_station""",
    ),
    Check(
        "every trip station exists in dim_station",
        """SELECT count(*) AS failures FROM {c}.gold.fact_trips f
           LEFT ANTI JOIN {c}.gold.dim_station s ON f.start_station_key = s.station_key""",
    ),
    Check(
        "every trip end station exists in dim_station",
        """SELECT count(*) AS failures FROM {c}.gold.fact_trips f
           LEFT ANTI JOIN {c}.gold.dim_station s ON f.end_station_key = s.station_key""",
    ),
    Check(
        "fact_station_availability has one row per station and snapshot",
        """SELECT count(*) AS failures FROM (
             SELECT gbfs_station_id, snapshot_at FROM {c}.gold.fact_station_availability
             GROUP BY ALL HAVING count(*) > 1)""",
    ),
    Check(
        "fact_station_availability keeps the grain of silver.station_status",
        """SELECT abs((SELECT count(*) FROM {c}.silver.station_status)
                    - (SELECT count(*) FROM {c}.gold.fact_station_availability)) AS failures""",
    ),
    Check(
        "every trip date exists in dim_date",
        """SELECT count(*) AS failures FROM {c}.gold.fact_trips f
           LEFT ANTI JOIN {c}.gold.dim_date d ON f.date_key = d.date_key""",
    ),
]


def run_checks(spark, catalog: str) -> list[tuple[str, int]]:
    results = []
    for check in CHECKS:
        failures = spark.sql(check.render(catalog)).first()["failures"]
        level = logging.INFO if failures == 0 else logging.ERROR
        logger.log(level, "%s: %s failures", check.name, failures)
        results.append((check.name, failures))
    return results


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    failed = [name for name, failures in run_checks(spark, args.catalog) if failures]
    if failed:
        raise SystemExit(f"{len(failed)} quality checks failed: {', '.join(failed)}")
    logger.info("All %d quality checks passed", len(CHECKS))


if __name__ == "__main__":
    main()
