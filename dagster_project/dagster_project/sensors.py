"""Dagster sensors for the Sales DW project.

Event-driven trigger: when the daily ingestion simulator writes new rows
to any seed CSV, the sensor sees the file mtime change and launches a
pipeline run. Pairs with the hourly schedule:
  - Sensor = primary driver (runs only when data arrived)
  - Schedule = safety net (catches missed events)

Also runs a failure sensor that POSTs to Slack whenever any run fails,
giving an audit trail for unattended pipeline runs.
"""
import json
import os
import urllib.request
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    run_failure_sensor,
    sensor,
)

from dagster_project.schedules import hourly_pipeline_job

REPO_ROOT = Path(__file__).resolve().parents[2]
SEEDS_DIR = REPO_ROOT / "dbt_project" / "seeds"


@sensor(
    job=hourly_pipeline_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description="Watches dbt seed CSV files. Launches a pipeline run "
                "when the simulator (or any process) appends new rows.",
)
def seed_file_sensor(context: SensorEvaluationContext) -> SensorResult:
    """Track the max mtime across all seed CSVs; trigger when it advances.

    Why max(mtime) as cursor instead of per-file tracking:
      The cursor is a single string and the dbt pipeline rebuilds the whole
      asset graph anyway — so we only need to know "did ANY seed change
      since last tick". Float-encoded mtime gives sub-second precision.
    """
    seed_files = sorted(SEEDS_DIR.glob("*.csv"))
    if not seed_files:
        return SensorResult(skip_reason=SkipReason(f"No seed CSVs in {SEEDS_DIR}"))

    current_max_mtime = max(f.stat().st_mtime for f in seed_files)

    # First-tick guard: on initial startup the cursor is empty. Recording the
    # current state without triggering avoids a spurious "catch-up" run for
    # mtimes that predate the sensor itself (e.g. previous dbt builds,
    # reset_data.sh restores, git clone touches).
    if not context.cursor:
        return SensorResult(
            cursor=str(current_max_mtime),
            skip_reason=SkipReason(
                f"Initial tick — recorded baseline mtime {current_max_mtime:.0f}, "
                "no backfill"
            ),
        )

    last_max_mtime = float(context.cursor)

    if current_max_mtime <= last_max_mtime:
        return SensorResult(
            skip_reason=SkipReason(
                f"No seed changes since {last_max_mtime:.0f}"
            )
        )

    # Find which files changed so the run tag is informative in the UI.
    changed = [
        f.name for f in seed_files if f.stat().st_mtime > last_max_mtime
    ]

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"seeds-{current_max_mtime:.0f}",  # idempotency: 1 run per mtime
                tags={"trigger": "seed_change", "changed_files": ",".join(changed)},
            )
        ],
        cursor=str(current_max_mtime),
    )


@run_failure_sensor(
    default_status=DefaultSensorStatus.STOPPED,
    description="Posts a Slack alert whenever any pipeline run fails.",
)
def slack_failure_alert(context: RunFailureSensorContext) -> None:
    """POST a single-line failure summary to Slack via Incoming Webhook.

    Skips silently when SLACK_WEBHOOK_URL is not configured so dev runs
    without secrets don't crash. urllib avoids pulling requests just for
    one POST per failure.
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        context.log.warning("SLACK_WEBHOOK_URL not set; skipping Slack alert.")
        return

    run = context.dagster_run
    payload = {
        "username": "Dagster Alert Bot",
        "icon_emoji": ":rotating_light:",
        "text": (
            f":x: *Pipeline failed* — `{run.job_name}`\n"
            f"• Run ID: `{run.run_id[:8]}`\n"
            f"• Failure: {context.failure_event.message}"
        ),
    }
    req = urllib.request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            context.log.info(f"Slack alert sent (HTTP {resp.status}).")
    except Exception as exc:  # noqa: BLE001 — never let alerting itself fail the sensor
        context.log.error(f"Slack alert failed: {exc!r}")
