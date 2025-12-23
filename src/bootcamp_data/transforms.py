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


def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )



def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """
    Return (low, high) bounds using the IQR rule:
    low = Q1 - k*IQR, high = Q3 + k*IQR
    """
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return (float("nan"), float("nan"))

    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1

    lo = q1 - k * iqr
    hi = q3 + k * iqr
    return (float(lo), float(hi))


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """
    Clip values to the [lo, hi] quantiles. Keeps NA as NA.
    """
    x = pd.to_numeric(s, errors="coerce")
    non_na = x.dropna()
    if non_na.empty:
        return x

    lo_v = non_na.quantile(lo)
    hi_v = non_na.quantile(hi)
    return x.clip(lower=lo_v, upper=hi_v)

def add_outlier_flag(
    df: pd.DataFrame,
    col: str,
    k: float = 1.5,
) -> pd.DataFrame:
    """
    Add a boolean outlier flag column like: <col>_is_outlier
    Uses IQR bounds on the numeric values of df[col].
    """
    out = df.copy()

    x = pd.to_numeric(out[col], errors="coerce")
    lo, hi = iqr_bounds(x, k=k)

    flag_col = f"{col}_is_outlier"

    # If bounds are NaN (e.g., no numeric data), mark all as False
    if pd.isna(lo) or pd.isna(hi):
        out[flag_col] = False
        return out

    out[flag_col] = x.notna() & ((x < lo) | (x > hi))
    return out