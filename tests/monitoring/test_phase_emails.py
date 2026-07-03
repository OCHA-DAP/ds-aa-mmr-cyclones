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


@pytest.fixture(scope="module")
def test_plots() -> tuple[str, bytes, bytes]:
    """Generate storm track and rainfall plots once for all phase tests.

    Uploads both plots to blob storage with today='test' and returns the
    storm name extracted from the test CSV alongside the raw PNG bytes for
    each plot.

    Returns:
        Tuple of (storm_name, storm_track_bytes, rainfall_bytes).
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

    issue_date = pd.Timestamp("2026-03-25")
    df_rain = pd.DataFrame(
        [
            {
                "issue_date": issue_date,
                "valid_date": issue_date + pd.Timedelta(days=lt),
                "mean": 10.0 + lt * 5,
            }
            for lt in range(5)
        ]
    )
    plot_chirps_gefs_forecast(df_rain, today="test", save=True)
    rainfall_bytes = stratus.load_blob_data(
        blob_name=(
            f"{constants.PROJECT_PREFIX}/processed/rainfall_forecast_plot/"
            "rainfall_forecast_plot_test_.png"
        )
    )

    storm_name = gdf["storm_name"].iloc[0]
    return storm_name, storm_track_bytes, rainfall_bytes


@pytest.mark.parametrize("phase", [None, "readiness", "action", "observational"])
def test_send_phase_email(
    phase: str | None,
    test_plots: tuple[str, bytes, bytes],
) -> None:
    """Send a dummy email for each anticipatory action trigger phase.

    Generates the email body with phase-specific introductory text and
    threshold status, attaches both the storm track and rainfall forecast
    plots, then creates and sends a Listmonk campaign.

    Args:
        phase: Trigger phase ('readiness', 'action', 'observational') or
            None for the generic monitoring email.
        test_plots: Module-scoped fixture providing (storm_name,
            storm_track_bytes, rainfall_bytes).
    """
    storm_name, storm_track_bytes, rainfall_bytes = test_plots
    threshold_info = _PHASE_THRESHOLD_INFO[phase]
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
