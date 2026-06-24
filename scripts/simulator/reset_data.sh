#!/usr/bin/env bash
# Restore dbt seeds from git HEAD and clear simulator state.
#
# Why git restore instead of a vendored baseline copy:
#   - git already stores the canonical pristine seeds — no need to duplicate
#     ~5 MB of CSV in a parallel _original/ directory.
#   - Restored bytes are always byte-perfect with the committed seeds,
#     so this script can never drift from the baseline (no CRLF / lowercase
#     header sync hazards).
#
# Limitation:
#   - Must be run from inside the git repository. Acceptable trade-off here
#     since the simulator is itself a development tool and not used elsewhere.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE="$ROOT/scripts/simulator/.sim_state.json"

cd "$ROOT"

# Restore the 6 dbt seed CSVs to whatever git HEAD has.
git restore dbt_project/seeds/crm_cust_info.csv \
            dbt_project/seeds/crm_prd_info.csv \
            dbt_project/seeds/crm_sales_details.csv \
            dbt_project/seeds/erp_cust_az12.csv \
            dbt_project/seeds/erp_loc_a101.csv \
            dbt_project/seeds/erp_px_cat_g1v2.csv

rm -f "$STATE"

echo "[reset] dbt seeds restored from git HEAD"
echo "[reset] Simulator state cleared"
