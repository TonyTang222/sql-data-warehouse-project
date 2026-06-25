"""Dagster schedules for the Sales DW project.

A single hourly schedule that rebuilds the entire dbt asset graph.
Equivalent to running `dbt build` every hour, but with full Dagster
observability: per-asset status, run history, failure alerts, retries.
"""
from dagster import AssetSelection, ScheduleDefinition, define_asset_job

# Job: materialize every asset in the project (16 dbt assets today).
# AssetSelection.all() auto-expands as new assets get added — no maintenance.
hourly_pipeline_job = define_asset_job(
    name="hourly_full_pipeline",
    selection=AssetSelection.all(),
    description="Rebuild the full Sales DW pipeline (seeds → silver → marts → snapshot).",
)

# Schedule: cron at the top of every hour. SchedulerDaemon picks this up
# (visible in the Dagster UI under Automation → Schedules).
hourly_schedule = ScheduleDefinition(
    job=hourly_pipeline_job,
    cron_schedule="0 * * * *",
    execution_timezone="Asia/Taipei",
    description="Runs hourly at minute 0. Mirrors a typical production ETL cadence.",
)
