"""Dagster asset checks for the Sales DW marts.

Operational health checks that run immediately after each mart asset is
materialized. Distinct from dbt tests (which verify data correctness like
not_null / unique / FK integrity) — these answer "is the pipeline healthy?":
  - Did fact_sales actually receive rows?
  - Is the data fresh, or has the upstream ingestion silently stalled?

A failing asset check surfaces in the Dagster UI as a warning on the asset,
but does NOT block downstream materialization — by design, checks are
advisory signals for operators, not gates that halt the pipeline.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import psycopg2
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

# Freshness threshold: source data tops out at 2014-01-29; simulator advances
# the watermark one day per run. 14 days gives the simulator some slack without
# letting truly stale data slip by.
FRESHNESS_THRESHOLD_DAYS = 14


def _pg_conn():
    """Same connection details dbt uses, read from .env."""
    return psycopg2.connect(
        host=os.environ.get("DBT_HOST", "127.0.0.1"),
        port=int(os.environ.get("DBT_PORT", "5433")),
        user=os.environ["DBT_USER"],
        password=os.environ["DBT_PASS"],
        dbname=os.environ.get("DBT_DBNAME", "data_warehouse"),
    )


def _row_count(table: str) -> int:
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"select count(*) from {table}")
        return cur.fetchone()[0]


@asset_check(asset=AssetKey(["gold", "fact_sales"]))
def fact_sales_not_empty() -> AssetCheckResult:
    """Fact must have at least 1 row — empty fact means the pipeline is broken."""
    count = _row_count("gold.fact_sales")
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
        description=f"fact_sales contains {count:,} rows",
    )


@asset_check(asset=AssetKey(["gold", "fact_sales"]))
def fact_sales_data_freshness() -> AssetCheckResult:
    """max(order_date) should be within FRESHNESS_THRESHOLD_DAYS of today.

    Catches silent ingestion stalls: pipeline runs green but no new data
    flowing means downstream reports go stale without any errors.
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("select max(order_date) from gold.fact_sales")
        max_date: date | None = cur.fetchone()[0]

    if max_date is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="fact_sales has no order_date data at all",
        )

    days_behind = (date.today() - max_date).days
    fresh = days_behind <= FRESHNESS_THRESHOLD_DAYS

    return AssetCheckResult(
        passed=fresh,
        severity=AssetCheckSeverity.WARN,  # stale data is a yellow flag, not red
        metadata={
            "max_order_date": str(max_date),
            "days_behind_today": days_behind,
            "threshold_days": FRESHNESS_THRESHOLD_DAYS,
        },
        description=(
            f"Latest order_date is {max_date} ({days_behind} days behind today). "
            f"Threshold: {FRESHNESS_THRESHOLD_DAYS} days."
        ),
    )


@asset_check(asset=AssetKey(["gold", "dim_customers"]))
def dim_customers_not_empty() -> AssetCheckResult:
    """Dim must have at least 1 row."""
    count = _row_count("gold.dim_customers")
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
        description=f"dim_customers contains {count:,} rows",
    )


@asset_check(asset=AssetKey(["gold", "dim_products"]))
def dim_products_not_empty() -> AssetCheckResult:
    """Dim must have at least 1 row."""
    count = _row_count("gold.dim_products")
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
        description=f"dim_products contains {count:,} rows",
    )
