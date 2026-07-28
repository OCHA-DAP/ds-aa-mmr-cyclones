import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

import src.utils.utils_windpseed as uw
from src.utils.utils_windpseed import plot_storm_track


@pytest.fixture(autouse=True)
def _no_blob_upload(monkeypatch):
    """Prevent plot_storm_track from writing to real blob storage."""
    monkeypatch.setattr(uw.stratus, "upload_blob_data", lambda **kwargs: None)


def _adm_boundaries() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"ADM1_EN": ["Rakhine", "Yangon"]},
        geometry=[
            Point(93.5, 20.5).buffer(1.0),
            Point(96.2, 16.8).buffer(0.5),
        ],
        crs="EPSG:4326",
    )


def _storms(wind_speeds_by_sid: dict[str, list[float]]) -> gpd.GeoDataFrame:
    base_time = pd.Timestamp("2026-07-20")
    rows = [
        {
            "sid": sid,
            "ensemble_number": 0,
            "time": base_time + pd.Timedelta(hours=6 * i),
            "wind_speed_at_land": speed,
            "lon": 92.0 + i * 0.5,
            "lat": 18.0 + i * 0.3,
        }
        for sid, speeds in wind_speeds_by_sid.items()
        for i, speed in enumerate(speeds)
    ]
    df = pd.DataFrame(rows)
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326"
    )


def test_plot_storm_track_all_below_30kt_does_not_crash():
    """Regression test: plotting must not require a >=30kt track.

    storms_area_interest is selected by distance to Myanmar, not wind
    speed, so it can contain only sub-30kt tracks. plot_storm_track must
    handle that without raising (previously ``idxmax()`` on an empty
    groupby raised ValueError when the caller pre-filtered to >=30kt).
    """
    storms = _storms({"WP2026-A": [15, 18, 22, 20], "WP2026-B": [10, 12, 14]})

    file_name = plot_storm_track(storms, _adm_boundaries(), today="test", hour="00")

    assert file_name == "storm_track_plot_test_00.png"


def test_plot_storm_track_highlights_max_wind_track():
    storms = _storms({"WP2026-A": [15, 18, 22, 20], "WP2026-C": [25, 32, 40, 33]})

    max_wind_sid = (
        storms.groupby("sid")["wind_speed_at_land"].max().idxmax()
    )

    assert max_wind_sid == "WP2026-C"
    plot_storm_track(storms, _adm_boundaries(), today="test", hour="00")
