import pandas as pd
import pytest
import ocha_stratus as stratus
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from shapely.geometry import Point

from src.datasources import codab
from src.monitoring.wind_speed_monitoring_ecmwf import plot_storm_track
from src.utils import constants
from src.utils.listmonk import create_campaign, generate_body_email, send_campaign
from src.utils.utils_plot import plot_chirps_gefs_forecast

load_dotenv()

_MYANMAR_TIME = "12h00 01 Jul. 2026"

_PHASE_THRESHOLD_INFO: dict[str | None, dict[str, str]] = {
    None: {
        "wind_speed_threshold_reached": "NOT REACHED",
        "rainfall_threshold_reached": "NOT REACHED",
    },
    "readiness": {
        "wind_speed_threshold_reached": "REACHED",
        "rainfall_threshold_reached": "NOT REACHED",
    },
    "action": {
        "wind_speed_threshold_reached": "REACHED",
        "rainfall_threshold_reached": "NOT REACHED",
    },
    "observational": {
        "wind_speed_threshold_reached": "REACHED",
        "rainfall_threshold_reached": "REACHED",
    },
}


def _dummy_rainfall_df(above_threshold: bool) -> pd.DataFrame:
    """Build a minimal CHIRPS-GEFS DataFrame for testing.

    Args:
        above_threshold: When True, use daily means of 65 mm so that the
            3-day rolling sum reaches 195 mm, exceeding the 175 mm alert
            level. When False, use low values well below the threshold.

    Returns:
        DataFrame with 'issue_date', 'valid_date', and 'mean' columns.
    """
    issue_date = pd.Timestamp("2026-03-25")
    mean_value = 65.0 if above_threshold else 10.0
    return pd.DataFrame(
        [
            {
                "issue_date": issue_date,
                "valid_date": issue_date + pd.Timedelta(days=lt),
                "mean": mean_value,
            }
            for lt in range(5)
        ]
    )


@pytest.fixture(scope="module")
def test_plots() -> tuple[str, bytes, bytes, bytes]:
    """Generate test plots once for all phase email tests.

    Uploads the storm track plot and two rainfall forecast plots (one below
    and one above the alert threshold) to blob storage, then returns the
    storm name and the raw PNG bytes for each.

    Returns:
        Tuple of (storm_name, storm_track_bytes, rainfall_low_bytes,
        rainfall_high_bytes) where rainfall_high_bytes contains at least one
        bar above the 175 mm threshold.
    """
    df = stratus.load_csv_from_blob(
        blob_name=f"{constants.PROJECT_PREFIX}/processed/test_monitoring.csv"
    )
    geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
    gdf = GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    adm_boundaries = codab.load_codab_from_blob(admin_level=constants.adm_level)

    plot_storm_track(
        storms_area_interest=gdf,
        adm_boundaries=adm_boundaries,
        today="test",
        hour="output",
    )
    storm_track_bytes = stratus.load_blob_data(
        blob_name=(
            f"{constants.PROJECT_PREFIX}/processed/storm_track_plot/"
            "storm_track_plot_test_output.png"
        )
    )

    plot_chirps_gefs_forecast(
        _dummy_rainfall_df(above_threshold=False), today="test", save=True
    )
    rainfall_low_bytes = stratus.load_blob_data(
        blob_name=(
            f"{constants.PROJECT_PREFIX}/processed/rainfall_forecast_plot/"
            "rainfall_forecast_plot_test_.png"
        )
    )

    plot_chirps_gefs_forecast(
        _dummy_rainfall_df(above_threshold=True), today="test_high", save=True
    )
    rainfall_high_bytes = stratus.load_blob_data(
        blob_name=(
            f"{constants.PROJECT_PREFIX}/processed/rainfall_forecast_plot/"
            "rainfall_forecast_plot_test_high_.png"
        )
    )

    storm_name = gdf["storm_name"].iloc[0]
    return storm_name, storm_track_bytes, rainfall_low_bytes, rainfall_high_bytes


@pytest.mark.parametrize("phase", [None, "readiness", "action", "observational"])
def test_send_phase_email(
    phase: str | None,
    test_plots: tuple[str, bytes, bytes, bytes],
) -> None:
    """Send a dummy email for each anticipatory action trigger phase.

    Uses a rainfall plot that exceeds the 175 mm threshold when the phase
    reports rainfall as REACHED, and a below-threshold plot otherwise.
    Both the storm track and the appropriate rainfall plot are embedded
    in the email body.

    Args:
        phase: Trigger phase ('readiness', 'action', 'observational') or
            None for the generic monitoring email.
        test_plots: Module-scoped fixture providing (storm_name,
            storm_track_bytes, rainfall_low_bytes, rainfall_high_bytes).
    """
    storm_name, storm_track_bytes, rainfall_low_bytes, rainfall_high_bytes = (
        test_plots
    )
    threshold_info = _PHASE_THRESHOLD_INFO[phase]
    rainfall_reached = threshold_info["rainfall_threshold_reached"] == "REACHED"
    rainfall_bytes = rainfall_high_bytes if rainfall_reached else rainfall_low_bytes
    phase_label = phase if phase is not None else "monitoring"

    campaign_body = generate_body_email(
        storm_name=storm_name,
        date_myanmar=_MYANMAR_TIME,
        info=threshold_info,
        plot_bytes=[storm_track_bytes, rainfall_bytes],
        phase=phase,
    )
    campaign_id = create_campaign(
        name=f"test_{phase_label}_email",
        body=campaign_body,
        subject=f"TEST {phase_label.upper()} - Anticipatory Action Myanmar - {_MYANMAR_TIME}",
    )
    send_campaign(campaign_id=campaign_id)
