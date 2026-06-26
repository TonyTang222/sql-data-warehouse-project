"""Dagster definitions root for the Sales DW project."""
from dagster import Definitions, load_assets_from_modules

from dagster_project import asset_checks, assets
from dagster_project.schedules import hourly_pipeline_job, hourly_schedule
from dagster_project.sensors import seed_file_sensor, slack_failure_alert

all_assets = load_assets_from_modules([assets])
all_asset_checks = [
    asset_checks.fact_sales_not_empty,
    asset_checks.fact_sales_data_freshness,
    asset_checks.dim_customers_not_empty,
    asset_checks.dim_products_not_empty,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[hourly_pipeline_job],
    schedules=[hourly_schedule],
    sensors=[seed_file_sensor, slack_failure_alert],
    resources={
        "dbt": assets.dbt_resource,
    },
)
