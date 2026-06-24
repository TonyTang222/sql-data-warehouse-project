#!/usr/bin/env bash
# Benchmark: fact_sales — full refresh vs incremental (0 delta) vs incremental (~135 delta).
# Each scenario runs 3 times, we keep all numbers and let you pick the median.
#
# Usage:
#   ./scripts/benchmark/run_fact_sales_benchmark.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/dbt_project"

# shellcheck disable=SC1091
source "$ROOT/venv/bin/activate"
# shellcheck disable=SC1091
source "$ROOT/.env"
export DBT_HOST DBT_PORT DBT_USER DBT_PASS DBT_DBNAME

DBT="dbt run --profiles-dir . --select fact_sales --quiet"

run_timed() {
  local label="$1"
  local cmd="$2"
  # /usr/bin/time -p prints real/user/sys to stderr in a stable format
  local real
  real=$( { /usr/bin/time -p bash -c "$cmd" >/dev/null; } 2>&1 | awk '/^real/ {print $2}' )
  echo "  $label : ${real}s"
}

echo "============================================================"
echo "Reset to clean baseline (60,398 rows in seeds)"
echo "============================================================"
"$ROOT/scripts/simulator/reset_data.sh" >/dev/null
dbt seed --profiles-dir . --select crm_sales_details --quiet >/dev/null
dbt run  --profiles-dir . --select stg_crm_sales_details --quiet >/dev/null

echo ""
echo "============================================================"
echo "Scenario A: FULL REFRESH (rebuild all 60,398 rows)"
echo "============================================================"
for i in 1 2 3; do
  run_timed "run #$i" "$DBT --full-refresh"
done

echo ""
echo "============================================================"
echo "Scenario B: INCREMENTAL with 0 row delta"
echo "  (source unchanged, watermark already at max)"
echo "============================================================"
for i in 1 2 3; do
  run_timed "run #$i" "$DBT"
done

echo ""
echo "============================================================"
echo "Simulating one business day (+135 new orders, +0 anomalies)"
echo "============================================================"
python "$ROOT/scripts/simulator/simulate_daily_ingestion.py" --seed 42 --no-anomaly
dbt seed --profiles-dir . --select crm_sales_details --quiet >/dev/null

echo ""
echo "============================================================"
echo "Scenario C: INCREMENTAL with ~135 row delta"
echo "  (each run consumes the delta on first iteration,"
echo "   so we re-simulate before each measurement for fairness)"
echo "============================================================"
for i in 1 2 3; do
  # Reset target and re-create the 60,533-row baseline before each delta measurement
  "$ROOT/scripts/simulator/reset_data.sh" >/dev/null
  dbt seed --profiles-dir . --select crm_sales_details --quiet >/dev/null
  dbt run  --profiles-dir . --select stg_crm_sales_details --quiet >/dev/null
  dbt run  --profiles-dir . --select fact_sales --full-refresh --quiet >/dev/null
  python "$ROOT/scripts/simulator/simulate_daily_ingestion.py" --seed $((42 + i)) --no-anomaly >/dev/null
  dbt seed --profiles-dir . --select crm_sales_details --quiet >/dev/null
  run_timed "run #$i" "$DBT"
done

echo ""
echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
