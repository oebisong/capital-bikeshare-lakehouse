"""Unity Catalog functions used for fine-grained access control.

silver.trips masks ride_id for anyone outside the data engineers group. A ride
identifier, combined with exact times and stations, could help someone link a
trip to a person, which the data licence forbids. Analysts receive the SHA-256
hash instead. It equals trip_key, so joins and counts still work.

The function must exist before the pipeline refresh, because silver.trips
declares the mask in its schema. The job therefore runs this task first.
"""

from __future__ import annotations

import argparse
import logging
import sys

from bikeshare_lakehouse.common.logs import configure_logging

logger = logging.getLogger(__name__)


def mask_ride_id_sql(catalog: str, engineers_group: str) -> str:
    return f"""
CREATE OR REPLACE FUNCTION `{catalog}`.silver.mask_ride_id(ride_id STRING)
RETURNS STRING
COMMENT 'Shows ride_id to {engineers_group}; everyone else sees its SHA-256 hash.'
RETURN CASE WHEN is_account_group_member('{engineers_group}') THEN ride_id
            ELSE sha2(ride_id, 256) END
""".strip()


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--engineers-group", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(mask_ride_id_sql(args.catalog, args.engineers_group))
    logger.info("Created %s.silver.mask_ride_id", args.catalog)


if __name__ == "__main__":
    main()
