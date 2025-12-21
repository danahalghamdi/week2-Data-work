from pathlib import Path
import pandas as pd

# Treat these strings as missing values when reading CSVs
NA = ["", "NA", "N/A", "null", "None", "not_a_number", "not_a_date"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    """
    Read raw orders CSV.
    Keep columns as strings to avoid parse errors (we'll clean/convert later).
    """
    return pd.read_csv(
        path,
        dtype={
            "order_id": "string",
            "user_id": "string",
            "amount": "string",
            "quantity": "string",
            "created_at": "string",
            "status": "string",
        },
        na_values=NA,
        keep_default_na=True,
    )


def read_users_csv(path: Path) -> pd.DataFrame:
    """Read raw users CSV."""
    return pd.read_csv(
        path,
        dtype={
            "user_id": "string",
            "country": "string",
            "signup_date": "string",
        },
        na_values=NA,
        keep_default_na=True,
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet, creating parent folders if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame."""
    return pd.read_parquet(path)
