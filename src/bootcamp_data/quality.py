import pandas as pd


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    assert not missing, f"Missing columns: {missing}"


def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    assert len(df) > 0, f"{name} is empty"


def assert_unique_key(
    df: pd.DataFrame,
    key: str,
    allow_na: bool = False,
) -> None:
    s = df[key]

    if not allow_na:
        assert s.notna().all(), f"{key} contains NA values"

    assert s.is_unique, f"{key} is not unique"


def assert_in_range(
    series: pd.Series,
    lo=None,
    hi=None,
    name: str = "value",
) -> None:
    x = series.dropna()

    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"

    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"