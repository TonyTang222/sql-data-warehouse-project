"""
Daily Ingestion Simulator
=========================

Simulates one business day of new data being ingested into the source CSV files.

Each run:
1. Advances the simulated "current date" by one day (state stored in .sim_state.json)
2. Appends 50-200 new sales orders for that day to sales_details.csv
3. Randomly mutates 1-2% of customer attributes (gender / marital status / country)
   to drive SCD Type 2 snapshot history
4. With 10% probability, injects a data quality anomaly:
   - "null_burst": ~30% of new rows get NULL sales_amount
   - "volume_drop": only 5-10 new rows instead of 50-200
   - "future_date": some rows tagged with a date in the far future

Usage:
    python scripts/simulator/simulate_daily_ingestion.py
    python scripts/simulator/simulate_daily_ingestion.py --no-anomaly
    python scripts/simulator/simulate_daily_ingestion.py --reset
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SEEDS = ROOT / "dbt_project" / "seeds"
STATE_FILE = ROOT / "scripts" / "simulator" / ".sim_state.json"

# The simulator only writes to dbt seeds. reset_data.sh restores them from git HEAD.
SALES_FILE = SEEDS / "crm_sales_details.csv"
CUST_FILE = SEEDS / "crm_cust_info.csv"
ERP_CUST_FILE = SEEDS / "erp_cust_az12.csv"
ERP_LOC_FILE = SEEDS / "erp_loc_a101.csv"

# Defaults derived from the original dataset
DEFAULT_LAST_DATE = date(2014, 1, 28)
DEFAULT_LAST_ORDER_NUM = 75123
ANOMALY_PROBABILITY = 0.10


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "current_date": DEFAULT_LAST_DATE.isoformat(),
        "last_order_num": DEFAULT_LAST_ORDER_NUM,
        "day_count": 0,
    }


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def reset_seeds() -> None:
    """Restore dbt seeds from git HEAD and clear sim state.

    Delegates to reset_data.sh so the restore logic stays in one place.
    """
    script = Path(__file__).resolve().parent / "reset_data.sh"
    import subprocess  # local import — keeps the module's import-time surface small
    subprocess.run(["bash", str(script)], check=True)


def load_pool(path: Path, col_idx: int) -> list[str]:
    """Load a column from CSV (skipping header) as a list."""
    with path.open() as f:
        reader = csv.reader(f)
        next(reader)
        return [row[col_idx] for row in reader if len(row) > col_idx and row[col_idx]]


def append_rows(path: Path, rows: list[list[str]]) -> None:
    with path.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def generate_new_sales(
    sim_date: date,
    start_order_num: int,
    n_rows: int,
    prd_keys: list[str],
    cust_ids: list[str],
    anomaly: str | None,
) -> tuple[list[list[str]], int]:
    """Generate new sales rows for the simulated day.

    Returns (rows, next_order_num_after_batch).
    """
    rows: list[list[str]] = []
    order_num = start_order_num
    date_int = int(sim_date.strftime("%Y%m%d"))

    for i in range(n_rows):
        # Each order can have 1-3 line items (same SO number, different products)
        n_items = random.randint(1, 3)
        order_num += 1
        ord_str = f"SO{order_num}"

        for _ in range(n_items):
            prd_key = random.choice(prd_keys)
            cust_id = random.choice(cust_ids)
            quantity = random.randint(1, 5)
            price = random.choice([9, 22, 35, 159, 320, 540, 1200, 2400, 3578])
            sales = price * quantity

            ship_date = (sim_date + timedelta(days=7)).strftime("%Y%m%d")
            due_date = (sim_date + timedelta(days=12)).strftime("%Y%m%d")

            order_dt: str | int = date_int
            sales_val: str | int = sales

            # Anomaly injection
            if anomaly == "null_burst" and random.random() < 0.30:
                sales_val = ""  # NULL sales_amount
            if anomaly == "future_date" and random.random() < 0.20:
                order_dt = int((sim_date + timedelta(days=400)).strftime("%Y%m%d"))

            rows.append([
                ord_str, prd_key, cust_id,
                str(order_dt), ship_date, due_date,
                str(sales_val), str(quantity), str(price),
            ])

    return rows, order_num


def mutate_customer_attributes(pct: float = 0.005) -> int:
    """Randomly flip 1-2% of CRM customer attributes to drive SCD2 snapshots."""
    rows = list(csv.reader(CUST_FILE.open()))
    header, data = rows[0], rows[1:]

    n_to_mutate = max(1, int(len(data) * pct))
    indices = random.sample(range(len(data)), n_to_mutate)

    # Column indices in cust_info.csv:
    # cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    marital_idx, gender_idx = 4, 5
    marital_choices = ["S", "M"]
    gender_choices = ["M", "F"]

    for i in indices:
        row = data[i]
        # Pad row if short
        while len(row) < 7:
            row.append("")
        if random.random() < 0.5:
            row[marital_idx] = random.choice(marital_choices)
        else:
            row[gender_idx] = random.choice(gender_choices)
        data[i] = row

    with CUST_FILE.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

    return n_to_mutate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true", help="Restore original CSVs and clear state")
    parser.add_argument("--no-anomaly", action="store_true", help="Disable anomaly injection")
    parser.add_argument("--force-anomaly", choices=["null_burst", "volume_drop", "future_date"],
                        help="Force a specific anomaly type")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.reset:
        reset_seeds()
        return

    if args.seed is not None:
        random.seed(args.seed)

    state = load_state()
    current_date = date.fromisoformat(state["current_date"])
    sim_date = current_date + timedelta(days=1)
    last_order_num = state["last_order_num"]
    day_count = state["day_count"] + 1

    # Decide anomaly for this day
    if args.no_anomaly:
        anomaly = None
    elif args.force_anomaly:
        anomaly = args.force_anomaly
    else:
        anomaly = random.choice(["null_burst", "volume_drop", "future_date"]) \
            if random.random() < ANOMALY_PROBABILITY else None

    # Decide row count
    if anomaly == "volume_drop":
        n_orders = random.randint(5, 10)
    else:
        n_orders = random.randint(50, 200)

    # Generate new sales rows
    # prd_key   : reuse existing valid product keys from historical sales
    # cust_id   : CRM cst_id (integer) — sales_details.sls_cust_id schema is integer,
    #             so use CRM cust_info.cst_id (column 0), not ERP's NAS-prefixed cid
    prd_keys = list(set(load_pool(SALES_FILE, 1)))
    cust_ids = load_pool(CUST_FILE, 0)

    new_rows, next_order_num = generate_new_sales(
        sim_date, last_order_num, n_orders, prd_keys, cust_ids, anomaly,
    )
    append_rows(SALES_FILE, new_rows)

    # Mutate customer attributes (skip on anomaly days to keep signals clean)
    mutated = mutate_customer_attributes() if anomaly is None else 0

    # Persist state
    state.update({
        "current_date": sim_date.isoformat(),
        "last_order_num": next_order_num,
        "day_count": day_count,
        "last_run_anomaly": anomaly,
        "last_run_rows": len(new_rows),
        "last_run_mutations": mutated,
    })
    save_state(state)

    # Report
    print(f"[day {day_count}] simulated date = {sim_date.isoformat()}")
    print(f"  - new sales rows : {len(new_rows)} (orders: {n_orders})")
    print(f"  - cust mutations : {mutated}")
    print(f"  - anomaly        : {anomaly or 'none'}")
    print(f"  - last order num : SO{next_order_num}")


if __name__ == "__main__":
    main()
