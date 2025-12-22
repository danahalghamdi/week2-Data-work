from pathlib import Path
import sys
import logging
import pandas as pd

# Make src/ importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
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
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def main() -> None:
    # paths
    paths = make_paths(ROOT)

    # 1. load processed data from Day 1
    orders = pd.read_parquet(paths.processed_dir / "orders.parquet")

    # 2. basic checks (fail fast)
    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    assert_non_empty(orders, name="orders")
    assert_unique_key(orders, "order_id")

    # 3. enforce schema
    orders = enforce_schema(orders)

    # 4. missingness report
    report = missingness_report(orders)
    report_path = paths.reports_dir / "missingness_orders.csv"
    report.to_csv(report_path, index=False)
    log.info(f"Wrote: {report_path}")

    # 5. normalize status text
    orders = orders.assign(
        status_clean=normalize_text(orders["status"])
    )

    # 6. add missing flags
    orders = add_missing_flags(orders, ["amount", "quantity"])

    # 7. FAIL FAST checks (Task 7)
    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=0, name="quantity")

    # 8. write cleaned data
    out_path = paths.processed_dir / "orders_clean.parquet"
    orders.to_parquet(out_path, index=False)
    log.info(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()