from __future__ import annotations

from pathlib import Path
import sys
import logging

import pandas as pd

# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
)
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    dedupe_keep_latest,
)

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    # --- paths (don't depend on config Paths attributes) ---
    raw_orders = ROOT / "data" / "raw" / "orders.csv"
    raw_users = ROOT / "data" / "raw" / "users.csv"
    processed_dir = ROOT / "data" / "processed"
    reports_dir = ROOT / "reports"

    processed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    # --- 1) load raw CSVs (orders + users) ---
    orders = pd.read_csv(raw_orders)
    users = pd.read_csv(raw_users)

    # --- 2) basic checks (columns + non-empty) ---
    require_columns(
        orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status"]
    )
    require_columns(users, ["user_id"])

    assert_non_empty(orders, name="orders")
    assert_non_empty(users, name="users")

    # --- 3) enforce schema (from Day 1 idea) ---
    orders = enforce_schema(orders)

    # ensure created_at is datetime (needed for dedupe keep latest)
    orders["created_at"] = pd.to_datetime(orders["created_at"], errors="coerce", utc=True)

    # --- 4) missingness report -> reports/ ---
    miss = missingness_report(orders)
    miss_path = reports_dir / "missingness_orders.csv"
    miss.to_csv(miss_path, index=False)
    log.info("Wrote: %s", miss_path)

    # --- 5) normalize status -> status_clean ---
    orders["status_clean"] = normalize_text(orders["status"])

    # --- 6) add missing flags for amount + quantity ---
    orders = add_missing_flags(orders, cols=["amount", "quantity"])

    # --- Task 4: dedupe keep latest (fixes your duplicate order_id error) ---
    orders = dedupe_keep_latest(
        orders,
        key_cols=["order_id"],
        ts_col="created_at",
    )

    # --- Task 7: fail-fast check(s) ---
    # after dedupe, enforce uniqueness
    assert_unique_key(orders, "order_id")

    # ranges (ignore missing by design in assert_in_range)
    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=0, name="quantity")

    # --- 7) write orders_clean.parquet ---
    out_path = processed_dir / "orders_clean.parquet"
    orders.to_parquet(out_path, index=False)
    log.info("Wrote: %s", out_path)


if __name__ == "__main__":
    main()