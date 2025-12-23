from __future__ import annotations

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

    # 1) Load data
    orders = pd.read_parquet(p.processed_dir / "orders_clean.parquet")
    users = pd.read_parquet(p.processed_dir / "users.parquet")

    # 2) Basic checks
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

    # users must be unique
    assert_unique_key(users, "user_id")

    # 3) Normalize join keys (VERY IMPORTANT)
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

    # 4) Parse datetime + add time parts
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    n_missing_ts = int(orders_t["created_at"].isna().sum())
    print("missing created_at after parse:", n_missing_ts, "/", len(orders_t))

    # 5) Safe join (orders many -> users one)
    joined = safe_left_join(
        left=orders_t,
        right=users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # sanity check
    assert len(joined) == len(orders_t), "Row count changed after join!"

    match_rate = 1.0 - float(joined["country"].isna().mean())
    print("rows:", len(joined))
    print("country match rate:", round(match_rate, 3))

    # 6) Outliers (winsor + flag)
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # 7) Write analytics table
    out_path = p.processed_dir / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    print("wrote:", out_path)


if __name__ == "__main__":
    main()