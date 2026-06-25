"""Dagster assets for the Sales DW project.

The entire dbt project (seeds, staging models, marts, snapshot) is exposed
as a single @dbt_assets group. Each dbt resource becomes one Dagster asset,
and Dagster reads the dbt manifest to wire up the dependency graph
automatically — no manual asset wiring needed.
"""
from pathlib import Path

from dagster import AssetExecutionContext, Backoff, RetryPolicy
from dagster_dbt import DbtCliResource, dbt_assets

# Path resolution:
#   __file__ = .../dagster_project/dagster_project/assets.py
#   parents[2] = repo root (sql-data-warehouse-project/)
REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Shared dbt CLI handle. Reused across @dbt_assets and any future asset checks.
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROJECT_DIR),
)


# Retry transient failures only. dbt subprocess can fail for connection
# drops, Postgres locking timeouts, or short-lived infra blips — none of
# which need a human. Genuine bugs (SQL syntax error, model logic) will
# fail again after retry and surface to the operator anyway.
DBT_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=2,                       # first retry waits 2s
    backoff=Backoff.EXPONENTIAL,   # then 4s, then 8s — gives DB time to recover
)


@dbt_assets(manifest=DBT_MANIFEST_PATH, retry_policy=DBT_RETRY_POLICY)
def sales_dw_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt nodes (seeds + staging + marts + snapshot) as Dagster assets.

    `dbt build` runs seeds, then models, then snapshots, then tests, in the
    correct dependency order. Streaming the events back lets Dagster mark
    each child asset as completed in real time.
    """
    yield from dbt.cli(["build"], context=context).stream()
