import pandas as pd
import pytest

from src.datasources.chirps_gefs import worst_rainfall_by_area


def _rainfall(area: str, issue_date: str, values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "issue_date": pd.Timestamp(issue_date),
            "valid_date": pd.date_range("2026-07-01", periods=len(values)),
            "area": area,
            "mean": values,
        }
    )


def test_worst_rainfall_uses_latest_issue_date_and_groups():
    df = pd.concat(
        [
            _rainfall("AA Target area", "2026-06-30", [500.0] * 4),
            _rainfall("AA Target area", "2026-07-01", [10.0, 20.0, 30.0, 5.0]),
            _rainfall("Sittwe", "2026-07-01", [100.0, 100.0, 100.0, 0.0]),
        ]
    )

    result = worst_rainfall_by_area(df, {"Sittwe": "AA Target area"}).set_index("area")

    assert result.loc["AA Target area", "max_rolling_sum_3"] == pytest.approx(60.0)
    assert result.loc["Sittwe", "max_rolling_sum_3"] == pytest.approx(300.0)
    assert result.loc["Sittwe", "group"] == "AA Target area"
    assert bool(result.loc["Sittwe", "is_township"])
    assert not bool(result.loc["AA Target area", "is_township"])
