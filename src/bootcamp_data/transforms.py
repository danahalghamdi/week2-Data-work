import pandas as pd


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)

    return (
        df.isna()
        .sum()
        .to_frame(name="missing_count")
        .assign(
            missing_pct=lambda x: x["missing_count"] / total
        )
        .reset_index()
        .rename(columns={"index": "column"})
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        df[f"{col}_is_missing"] = df[col].isna()

    return df

def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series
        .astype("string")
        .str.strip()
        .str.lower()
    )


def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    return series.map(mapping).fillna(series)


def dedupe_keep_latest(
    df: pd.DataFrame,
    key_cols: list[str],
    ts_col: str,
) -> pd.DataFrame:
    # Sort so the latest timestamp comes last (per key), then keep last
    out = df.sort_values(ts_col, kind="mergesort")
    out = out.drop_duplicates(subset=key_cols, keep="last")
    return out.reset_index(drop=True)