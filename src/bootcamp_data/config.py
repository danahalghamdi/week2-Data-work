from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    # project root
    root: Path

    # folders
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    reports_dir: Path

    # raw inputs
    raw_orders: Path
    raw_users: Path

    # outputs
    orders_parquet: Path
    users_parquet: Path


def make_paths(root: Path) -> Paths:
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    reports_dir = root / "reports"

    return Paths(
        root=root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        reports_dir=reports_dir,
        raw_orders=raw_dir / "orders.csv",
        raw_users=raw_dir / "users.csv",
        orders_parquet=processed_dir / "orders.parquet",
        users_parquet=processed_dir / "users.parquet",
    )