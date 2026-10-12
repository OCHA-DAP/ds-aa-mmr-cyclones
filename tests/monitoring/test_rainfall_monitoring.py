import pandas as pd
from dotenv import load_dotenv

from src.utils.utils_plot import plot_chirps_gefs_forecast

load_dotenv()


def _dummy_chirps_gefs_df() -> pd.DataFrame:
    """Create a minimal CHIRPS-GEFS DataFrame for testing.

    Returns:
        DataFrame with 'issue_date', 'valid_date', and 'mean' columns
        for a single issue date with five lead times.
    """
    issue_date = pd.Timestamp("2026-03-25")
    return pd.DataFrame(
        [
            {
                "issue_date": issue_date,
                "valid_date": issue_date + pd.Timedelta(days=lt),
                "mean": 60.0 + lt * 5,
            }
            for lt in range(5)
        ]
    )


def test_plot_chirps_gefs_forecast() -> None:
    """Generate and upload a CHIRPS-GEFS rainfall forecast plot from dummy data.

    Calls plot_chirps_gefs_forecast with today='test' and save=True,
    uploading the plot to blob storage for retrieval in test_send_email.
    """
    df = _dummy_chirps_gefs_df()
    plot_chirps_gefs_forecast(df, today="test", save=True)
