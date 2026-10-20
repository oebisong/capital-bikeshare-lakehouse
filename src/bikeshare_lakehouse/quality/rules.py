"""Helpers for recording data quality issues on each row.

Every silver transformation adds a dq_issues array. An empty array means the
row is valid. A non-empty array sends the row to the quarantine table with the
reasons attached, so nothing is silently dropped.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ISSUES = "dq_issues"


def issue(condition: Column, name: str) -> Column:
    return F.when(condition, F.lit(name))


def with_issues(df: DataFrame, *checks: Column) -> DataFrame:
    return df.withColumn(ISSUES, F.array_compact(F.array(*checks)))


def valid_rows(df: DataFrame) -> DataFrame:
    return df.where(F.size(ISSUES) == 0).drop(ISSUES)


def quarantine_rows(
    df: DataFrame, record_json: Column, source_column: str = "source_file"
) -> DataFrame:
    """Shape rejected rows for a quarantine table.

    The original record is kept as a JSON string so the table schema does not
    change when the source adds or removes columns.
    """
    return df.where(F.size(ISSUES) > 0).select(
        record_json.alias("record"),
        F.col(ISSUES),
        F.col(source_column).alias("source_file"),
        F.current_timestamp().alias("quarantined_at"),
    )
