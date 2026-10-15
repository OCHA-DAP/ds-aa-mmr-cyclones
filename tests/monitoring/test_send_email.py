"""Tests for the send_email monitoring module.

Covers the data-loading helpers, the threshold table and the email body
rendering. All storage and listmonk calls are mocked.
"""

import datetime
from unittest.mock import patch

import pandas as pd

from src.monitoring.send_email import (
    build_threshold_table,
    check_cyclone_presence,
    check_rainfall_data,
    check_wind_speed_trigger_data,
    get_latest_monitoring_plot,
)
from src.utils import constants
from src.utils.listmonk import generate_body_email

TODAY = datetime.date.today().strftime("%Y-%m-%d")
PREFIX = f"{constants.PROJECT_PREFIX}/processed"
MYANMAR_TIME = "12h00 01 Jan. 2026"
PNG = b"\x89PNG fake"
STRATUS = "src.monitoring.send_email.stratus"
NOT_REACHED = {
    "wind_speed_threshold_reached": "NOT REACHED",
    "rainfall_threshold_reached": "NOT REACHED",
}


def _wind_df(sid: str = "2026001N10090") -> pd.DataFrame:
    """Build a dummy wind exceedance DataFrame for one storm."""
    return pd.DataFrame({"sid": [sid], "wind_speed_at_land": [55.0]})


class TestCheckWindSpeedTriggerData:
    """Tests for check_wind_speed_trigger_data."""

    def test_no_blobs_returns_empty_dict(self) -> None:
        """Returns an empty mapping when no wind blobs exist."""
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            assert check_wind_speed_trigger_data() == {}

    def test_loads_latest_blob_per_area_and_source(self) -> None:
        """Keeps only today's latest blob for each (area, source)."""
        blobs = [
            f"{PREFIX}/wind_exceedance_rest_{TODAY}_00_ecmwf.csv",
            f"{PREFIX}/wind_exceedance_rest_{TODAY}_12_ecmwf.csv",
            f"{PREFIX}/wind_exceedance_adm3_{TODAY}_12_cma.csv",
            f"{PREFIX}/wind_exceedance_rest_2020-01-01_12_ecmwf.csv",
        ]
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = blobs
            mock_stratus.load_csv_from_blob.return_value = _wind_df()
            result = check_wind_speed_trigger_data()
        assert set(result) == {("rest", "ecmwf"), ("adm3", "cma")}
        loaded = {
            call.kwargs["blob_name"]
            for call in mock_stratus.load_csv_from_blob.call_args_list
        }
        assert f"{PREFIX}/wind_exceedance_rest_{TODAY}_12_ecmwf.csv" in loaded
        assert f"{PREFIX}/wind_exceedance_rest_{TODAY}_00_ecmwf.csv" not in loaded

    def test_unnamed_storms_dropped(self) -> None:
        """Unnamed storms such as 70B are removed from the data."""
        blob = f"{PREFIX}/wind_exceedance_rest_{TODAY}_12_ecmwf.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            mock_stratus.load_csv_from_blob.return_value = _wind_df("70B")
            result = check_wind_speed_trigger_data()
        assert result[("rest", "ecmwf")].empty


class TestBuildThresholdTable:
    """Tests for build_threshold_table."""

    def test_reached_and_not_reached_per_area(self) -> None:
        """Wind rows reflect which (area, source) has exceedance data."""
        table = build_threshold_table({("rest", "ecmwf"): _wind_df()}, pd.DataFrame())
        assert table["Wind speed (ECMWF)"] == {
            "AA Target area": "NOT REACHED",
            "Rest of Rakhine": "REACHED",
        }
        assert table["Wind speed (CMA)"]["Rest of Rakhine"] == "NOT REACHED"

    def test_rainfall_not_assessed_for_rest_of_rakhine(self) -> None:
        """Rainfall is only assessed for the AA Target area."""
        table = build_threshold_table({}, pd.DataFrame({"mean": [1.0]}))
        assert table["Rainfall"] == {
            "AA Target area": "REACHED",
            "Rest of Rakhine": "Not assessed",
        }


class TestCheckRainfallData:
    """Tests for check_rainfall_data."""

    def test_no_blobs_returns_empty(self) -> None:
        """Returns an empty DataFrame when no rainfall blobs exist."""
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            assert check_rainfall_data().empty

    def test_old_blob_ignored(self) -> None:
        """A blob from a past date is not loaded."""
        blob = f"{PREFIX}/rainfall_exceedance_2020-01-01_12.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            assert check_rainfall_data().empty
            mock_stratus.load_csv_from_blob.assert_not_called()

    def test_todays_blob_loaded(self) -> None:
        """Today's blob is loaded and returned."""
        df = pd.DataFrame({"mean": [210.0]})
        blob = f"{PREFIX}/rainfall_exceedance_{TODAY}_12.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            mock_stratus.load_csv_from_blob.return_value = df
            pd.testing.assert_frame_equal(check_rainfall_data(), df)


class TestCheckCyclonePresence:
    """Tests for check_cyclone_presence."""

    def test_no_blobs_returns_empty(self) -> None:
        """Returns an empty DataFrame when no monitoring blobs exist."""
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            assert check_cyclone_presence().empty

    def test_old_blob_ignored(self) -> None:
        """A blob from a past date yields no storms."""
        blob = f"{PREFIX}/monitoring_2020-01-01_12_ecmwf.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            assert check_cyclone_presence().empty

    def test_todays_named_storm_returned(self) -> None:
        """Today's named storm is returned."""
        df = pd.DataFrame({"storm_name": ["CYCLONE_TEST"]})
        blob = f"{PREFIX}/monitoring_{TODAY}_12_ecmwf.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            mock_stratus.load_csv_from_blob.return_value = df
            pd.testing.assert_frame_equal(check_cyclone_presence(), df)

    def test_unnamed_storm_dropped(self) -> None:
        """Unnamed storms such as 70B are removed."""
        df = pd.DataFrame({"storm_name": ["70B"]})
        blob = f"{PREFIX}/monitoring_{TODAY}_12_ecmwf.csv"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            mock_stratus.load_csv_from_blob.return_value = df
            assert check_cyclone_presence().empty


class TestGetLatestMonitoringPlot:
    """Tests for get_latest_monitoring_plot."""

    def test_no_blobs_returns_none(self) -> None:
        """Returns None when no plot blobs exist."""
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = []
            assert get_latest_monitoring_plot() is None

    def test_old_plot_returns_none(self) -> None:
        """Returns None when the plot is from a past date."""
        blob = f"{PREFIX}/storm_track_plot_2020-01-01_12.png"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            assert get_latest_monitoring_plot() is None

    def test_todays_plot_per_type(self) -> None:
        """Returns today's bytes, listing blobs by the requested plot type."""
        blob = f"{PREFIX}/rainfall_forecast_plot_{TODAY}_12.png"
        with patch(STRATUS) as mock_stratus:
            mock_stratus.list_container_blobs.return_value = [blob]
            mock_stratus.load_blob_data.return_value = PNG
            result = get_latest_monitoring_plot(plot_type="rainfall_forecast")
        assert result == PNG
        mock_stratus.list_container_blobs.assert_called_once_with(
            name_starts_with=f"{PREFIX}/rainfall_forecast_plot"
        )


class TestGenerateBodyEmail:
    """Email body rendering for the three scenarios."""

    def test_wind_reached_with_table_and_both_plots(self) -> None:
        """Phase intro, status lines, threshold table and two plots appear."""
        info = {**NOT_REACHED, "wind_speed_threshold_reached": "REACHED"}
        table = build_threshold_table({("rest", "ecmwf"): _wind_df()}, pd.DataFrame())
        body = generate_body_email(
            storm_name="CYCLONE_TEST",
            date_myanmar=MYANMAR_TIME,
            info=info,
            plot_bytes=[PNG, PNG],
            phase="action",
            threshold_table=table,
        )
        assert "Wind speed threshold: REACHED" in body
        assert "Precipitation threshold: NOT REACHED" in body
        assert "action threshold" in body
        assert "Rest of Rakhine" in body
        assert body.count("data:image/png;base64,") == 2

    def test_rainfall_reached_skips_missing_plots(self) -> None:
        """None entries in plot_bytes are skipped."""
        info = {**NOT_REACHED, "rainfall_threshold_reached": "REACHED"}
        body = generate_body_email(
            storm_name="CYCLONE_TEST",
            date_myanmar=MYANMAR_TIME,
            info=info,
            plot_bytes=[None, PNG],
            phase="readiness",
        )
        assert "Precipitation threshold: REACHED" in body
        assert "readiness threshold" in body
        assert body.count("data:image/png;base64,") == 1

    def test_monitoring_only_has_generic_intro_and_no_table(self) -> None:
        """Without phase, table or plots the generic intro is used."""
        body = generate_body_email(
            storm_name="CYCLONE_TEST",
            date_myanmar=MYANMAR_TIME,
            info=NOT_REACHED,
            plot_bytes=None,
        )
        assert "new forecast track" in body
        assert "<table" not in body
        assert "data:image/png;base64," not in body
