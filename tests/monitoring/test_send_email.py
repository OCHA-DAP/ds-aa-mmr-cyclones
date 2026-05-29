import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from shapely.geometry import Point

from src.datasources import codab
from src.monitoring.wind_speed_monitoring_ecmwf import plot_storm_track
from src.utils import constants
from src.utils.listmonk import create_campaign, generate_body_email, send_campaign
from src.utils.utils_plot import plot_chirps_gefs_forecast

load_dotenv()

_MYANMAR_TIME = "12h00 01 Jan. 2026"
_THRESHOLD_INFO = {
    "wind_speed_threshold_reached": "NOT REACHED",
    "rainfall_threshold_reached": "NOT REACHED",
}


def test_send_email() -> None:
    """Send a monitoring email with both storm track and rainfall plots.

    Generates the storm track plot from the test CSV and the rainfall
    forecast plot from dummy CHIRPS-GEFS data, both uploaded to blob
    storage with today='test'. Loads the PNG bytes for each and sends
    a campaign email via Listmonk containing both plots.
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
    campaign_body = generate_body_email(
        storm_name=storm_name,
        date_myanmar=_MYANMAR_TIME,
        info=_THRESHOLD_INFO,
        plot_bytes=[storm_track_bytes, rainfall_bytes],
    )
    campaign_id = create_campaign(
        name="test_monitoring_email",
        body=campaign_body,
        subject=f"TEST - Anticipatory Action Myanmar - {_MYANMAR_TIME}",
    )
    send_campaign(campaign_id=campaign_id)
