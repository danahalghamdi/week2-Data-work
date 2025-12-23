from pathlib import Path
import pandas as pd

from bootcamp_data.config import make_paths
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    # paths
    p = make_paths(ROOT)

    # load data
    orders = pd.read_parquet(p.processed_dir / "orders_clean.parquet")
    users = pd.read_parquet(p.processed_dir / "users.parquet")

    # --------------------
    # quality checks
    # --------------------
    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],
    )
    require_columns(
        users,
        ["user_id", "country", "signup_date"],
    )

    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # --------------------
    # FIX user_id mismatch
    # --------------------
    orders["user_id"] = (
        orders["user_id"]
        .astype("string")
        .str.strip()
        .str.zfill(4)
    )

    users["user_id"] = (
        users["user_id"]
        .astype("string")
        .str.strip()
        .str.zfill(4)
    )

    # --------------------
    # datetime transforms
    # --------------------
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    n_missing_ts = int(orders_t["created_at"].isna().sum())
    print("missing created_at after parse:", n_missing_ts, "/", len(orders_t))

    # --------------------
    # safe join (orders many -> users one)
    # --------------------
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # sanity checks
    assert len(joined) == len(orders_t), "Row count changed (join explosion?)"

    match_rate = 1.0 - float(joined["country"].isna().mean())
    print("rows:", len(joined))
    print("country match rate:", round(match_rate, 3))

    # --------------------
    # outliers
    # --------------------
    joined = joined.assign(
        amount_winsor=winsorize(joined["amount"])
    )
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # --------------------
    # write analytics table
    # --------------------
    out_path = p.processed_dir / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    print("wrote:", out_path)

    # ==================================================
    # Task 7 — mini summary table (BONUS)
    # ==================================================
    summary = (
        joined
        .groupby("country", dropna=False)
        .agg(
            revenue=("amount", "sum"),
            orders_count=("order_id", "count"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )

    print("\nRevenue by country:")
    print(summary.head(10))

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)
    summary_path = reports_dir / "revenue_by_country.csv"
    summary.to_csv(summary_path, index=False)
    print("wrote:", summary_path)


if __name__ == "__main__":
    main()