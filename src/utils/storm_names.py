"""Helpers for identifying not-yet-named storms (e.g. 70B, 71W)."""

import pandas as pd

UNNAMED_STORM_PATTERN = r"^\d{2}[A-Za-z]$"


def is_unnamed_storm(identifiers: pd.Series) -> pd.Series:
    """Flag identifiers made of two digits plus one letter (e.g. 70B).

    Args:
        identifiers: Series of storm identifiers (``sid`` or ``storm_name``).

    Returns:
        Boolean Series, True where the identifier looks like ``70B``.
    """
    return identifiers.astype(str).str.match(UNNAMED_STORM_PATTERN, na=False)


def drop_unnamed_storms(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Drop rows whose storm identifier is two digits plus one letter.

    Args:
        df: DataFrame holding the storm identifiers.
        column: Name of the column to test (e.g. ``sid`` or ``storm_name``).

    Returns:
        The rows of ``df`` whose identifier does not look like ``70B``.
    """
    return df[~is_unnamed_storm(df[column])]
