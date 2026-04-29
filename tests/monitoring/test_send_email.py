"""Tests for the send_email monitoring module.

Covers helper functions (check_wind_speed_trigger_data, check_rainfall_data,
check_cyclone_presence, get_latest_storm_track_plot) and three dummy email
scenarios: wind exceedance, rainfall exceedance, and cyclone presence only.
"""

import datetime
from zoneinfo import ZoneInfo
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point, box

load_dotenv()

from src.monitoring.send_email import (
    check_cyclone_presence,
    check_rainfall_data,
    check_wind_speed_trigger_data,
    get_latest_storm_track_plot,
)
from src.utils import constants
from src.utils.listmonk import create_campaign, generate_body_email, send_campaign
from src.utils.utils_windpseed import plot_storm_track

TODAY: str = datetime.date.today().strftime("%Y-%m-%d")
MYANMAR_TIME: str = datetime.datetime.now(ZoneInfo("Asia/Yangon")).strftime(
    "%Hh00 %d %b. %Y"
)


def _make_storm_gdf() -> gpd.GeoDataFrame:
    """Create a dummy storm track GeoDataFrame for plot_storm_track.

    Returns:
        GeoDataFrame with sid, ensemble_number, time, and point geometry.
    """
    return gpd.GeoDataFrame(
        {
            "sid": ["2026001N10090"],
            "ensemble_number": [0],
            "time": [pd.Timestamp("2026-04-29")],
        },
        geometry=[Point(95.0, 16.5)],
        crs="EPSG:4326",
    )


def _make_adm_gdf() -> gpd.GeoDataFrame:
    """Create a dummy administrative boundaries GeoDataFrame.

    Returns:
        GeoDataFrame with ADM1_EN and polygon geometry covering Rakhine.
    """
    return gpd.GeoDataFrame(
        {"ADM1_EN": ["Rakhine"]},
        geometry=[box(92.0, 17.0, 98.0, 26.0)],
        crs="EPSG:4326",
    )


def _generate_dummy_plot_bytes() -> bytes:
    """Generate real PNG bytes via plot_storm_track with dummy data.

    Returns:
        Raw PNG bytes from the storm track plot.
    """
    with patch("src.utils.utils_windpseed.stratus") as mock_stratus:
        plot_storm_track(_make_storm_gdf(), _make_adm_gdf(), TODAY, "12")
    buf = mock_stratus.upload_blob_data.call_args.kwargs["data"]
    buf.seek(0)
    return buf.read()


DUMMY_PLOT_BYTES: bytes = _generate_dummy_plot_bytes()


def _make_wind_df() -> pd.DataFrame:
    """Create a dummy wind speed exceedance DataFrame.

    Returns:
        DataFrame with storm_name, wind_speed_at_land, sid, and ensemble_number.
    """
    return pd.DataFrame(
        {
            "storm_name": ["CYCLONE_TEST"],
            "wind_speed_at_land": [55.0],
            "sid": ["2026001N10090"],
            "ensemble_number": [0],
        }
    )


def _make_rainfall_df() -> pd.DataFrame:
    """Create a dummy rainfall exceedance DataFrame.

    Returns:
        DataFrame with storm_name and rainfall columns.
    """
    return pd.DataFrame(
        {
            "storm_name": ["CYCLONE_TEST"],
            "rainfall": [210.0],
        }
    )


def _make_cyclone_df() -> pd.DataFrame:
    """Create a dummy cyclone presence DataFrame.

    Returns:
        DataFrame with storm_name, lat, and lon columns.
    """
    return pd.DataFrame(
        {
            "storm_name": ["CYCLONE_TEST"],
            "lat": [16.5],
            "lon": [95.0],
        }
    )


class TestCheckWindSpeedTriggerData:
    """Tests for check_wind_speed_trigger_data."""

    def test_no_blobs_returns_empty_dataframe(self) -> None:
        """Returns empty DataFrame when no wind blobs exist in storage."""
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            result = check_wind_speed_trigger_data()
        assert result.empty

    def test_with_blob_loads_and_returns_latest_csv(self) -> None:
        """Loads and returns the last CSV blob when wind data is present."""
        df = _make_wind_df()
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"wind_exceedance_{TODAY}_12_ecmwf.csv"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            mock_stratus.load_csv_from_blob.return_value = df
            result = check_wind_speed_trigger_data()
        assert not result.empty
        pd.testing.assert_frame_equal(result, df)


class TestCheckRainfallData:
    """Tests for check_rainfall_data."""

    def test_no_blobs_returns_empty_dataframe(self) -> None:
        """Returns empty DataFrame when no rainfall blobs exist in storage."""
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            result = check_rainfall_data()
        assert result.empty

    def test_with_blob_loads_and_returns_latest_csv(self) -> None:
        """Loads and returns the last CSV blob when rainfall data is present."""
        df = _make_rainfall_df()
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"rainfall_exceedance_{TODAY}_12.csv"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            mock_stratus.load_csv_from_blob.return_value = df
            result = check_rainfall_data()
        assert not result.empty
        pd.testing.assert_frame_equal(result, df)


class TestCheckCyclonePresence:
    """Tests for check_cyclone_presence."""

    def test_no_blobs_returns_empty_dataframe(self) -> None:
        """Returns empty DataFrame when no monitoring blobs exist in storage."""
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            result = check_cyclone_presence()
        assert result.empty

    def test_todays_blob_returns_dataframe(self) -> None:
        """Returns cyclone DataFrame when a blob for today exists."""
        df = _make_cyclone_df()
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"monitoring_{TODAY}_12_ecmwf.csv"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            mock_stratus.load_csv_from_blob.return_value = df
            result = check_cyclone_presence()
        assert not result.empty
        pd.testing.assert_frame_equal(result, df)

    def test_old_date_blob_returns_empty_dataframe(self) -> None:
        """Returns empty DataFrame when the most recent blob is from a past date."""
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"monitoring_2020-01-01_12_ecmwf.csv"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            result = check_cyclone_presence()
        assert result.empty


class TestGetLatestStormTrackPlot:
    """Tests for get_latest_storm_track_plot."""

    def test_no_blobs_returns_none(self) -> None:
        """Returns None when no storm track plot blobs exist."""
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            result = get_latest_storm_track_plot()
        assert result is None

    def test_todays_plot_returns_bytes(self) -> None:
        """Returns raw PNG bytes when a plot for today exists."""
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"storm_track_plot_{TODAY}_12.png"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            mock_stratus.load_blob_data.return_value = DUMMY_PLOT_BYTES
            result = get_latest_storm_track_plot()
        assert result == DUMMY_PLOT_BYTES

    def test_old_plot_returns_none(self) -> None:
        """Returns None when the most recent plot is from a past date."""
        blob_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"storm_track_plot_2020-01-01_12.png"
        )
        with patch("src.monitoring.send_email.stratus") as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob_name]
            result = get_latest_storm_track_plot()
        assert result is None


class TestSendWindExceedanceEmail:
    """Dummy email scenario: wind speed threshold exceeded.

    These tests verify the email body content and the listmonk API calls
    that would be made when wind speed data exceeds the alert threshold.
    """

    def test_email_body_contains_wind_reached(self) -> None:
        """Email body shows wind threshold as REACHED and rainfall as NOT REACHED."""
        df_wind = _make_wind_df()
        threshold_info = {
            "wind_speed_threshold_reached": "REACHED",
            "rainfall_threshold_reached": "NOT REACHED",
        }
        body = generate_body_email(
            storm_name=df_wind.storm_name.unique(),
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=DUMMY_PLOT_BYTES,
        )
        assert "Wind speed threshold: REACHED" in body
        assert "Precipitation threshold: NOT REACHED" in body
        assert "CYCLONE_TEST" in body
        assert "data:image/png;base64," in body

    def test_trigger_campaign_is_created_and_sent(self) -> None:
        """MMR_trigger_email campaign is created and sent via listmonk API."""
        df_wind = _make_wind_df()
        threshold_info = {
            "wind_speed_threshold_reached": "REACHED",
            "rainfall_threshold_reached": "NOT REACHED",
        }
        body = generate_body_email(
            storm_name=df_wind.storm_name.unique(),
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=DUMMY_PLOT_BYTES,
        )
        campaign_id = create_campaign(
            name="MMR_trigger_email",
            body=body,
            subject=f"Anticipatory Action Myanmar - {MYANMAR_TIME}",
        )
        assert isinstance(campaign_id, int)
        send_campaign(campaign_id=campaign_id)


class TestSendRainfallExceedanceEmail:
    """Dummy email scenario: rainfall threshold exceeded.

    These tests verify the email body content and the listmonk API calls
    that would be made when rainfall data exceeds the alert threshold.
    """

    def test_email_body_contains_rainfall_reached(self) -> None:
        """Email body shows rainfall threshold as REACHED and wind as NOT REACHED."""
        df_wind = _make_wind_df()
        threshold_info = {
            "wind_speed_threshold_reached": "NOT REACHED",
            "rainfall_threshold_reached": "REACHED",
        }
        body = generate_body_email(
            storm_name=df_wind.storm_name.unique(),
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=None,
        )
        assert "Wind speed threshold: NOT REACHED" in body
        assert "Precipitation threshold: REACHED" in body
        assert "CYCLONE_TEST" in body

    def test_trigger_campaign_is_created_and_sent(self) -> None:
        """MMR_trigger_email campaign is created and sent via listmonk API."""
        df_wind = _make_wind_df()
        threshold_info = {
            "wind_speed_threshold_reached": "NOT REACHED",
            "rainfall_threshold_reached": "REACHED",
        }
        body = generate_body_email(
            storm_name=df_wind.storm_name.unique(),
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=None,
        )
        campaign_id = create_campaign(
            name="MMR_trigger_email",
            body=body,
            subject=f"Anticipatory Action Myanmar - {MYANMAR_TIME}",
        )
        assert isinstance(campaign_id, int)
        send_campaign(campaign_id=campaign_id)


class TestSendCyclonePresenceEmail:
    """Dummy email scenario: cyclone present but no threshold exceeded.

    These tests verify the email body content and the listmonk API calls
    that would be made when a cyclone is detected in the area but neither
    wind speed nor rainfall thresholds are exceeded.
    """

    def test_email_body_contains_both_not_reached(self) -> None:
        """Email body shows both thresholds as NOT REACHED for cyclone-only presence."""
        df_cyclone = _make_cyclone_df()
        threshold_info = {
            "wind_speed_threshold_reached": "NOT REACHED",
            "rainfall_threshold_reached": "NOT REACHED",
        }
        body = generate_body_email(
            storm_name=df_cyclone.storm_name.unique()[0],
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=DUMMY_PLOT_BYTES,
        )
        assert "Wind speed threshold: NOT REACHED" in body
        assert "Precipitation threshold: NOT REACHED" in body
        assert "CYCLONE_TEST" in body
        assert "data:image/png;base64," in body

    def test_monitoring_campaign_is_created_and_sent(self) -> None:
        """MMR_monitoring_email campaign is created and sent via listmonk API."""
        df_cyclone = _make_cyclone_df()
        threshold_info = {
            "wind_speed_threshold_reached": "NOT REACHED",
            "rainfall_threshold_reached": "NOT REACHED",
        }
        body = generate_body_email(
            storm_name=df_cyclone.storm_name.unique()[0],
            date_myanmar=MYANMAR_TIME,
            info=threshold_info,
            plot_bytes=DUMMY_PLOT_BYTES,
        )
        campaign_id = create_campaign(
            name="MMR_monitoring_email",
            body=body,
            subject=f"Anticipatory Action Myanmar - {MYANMAR_TIME}",
        )
        assert isinstance(campaign_id, int)
        send_campaign(campaign_id=campaign_id)
