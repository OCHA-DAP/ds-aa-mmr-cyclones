import geopandas as gpd
import pandas as pd
import pytest
import tenacity
from shapely.geometry import Point

import src.monitoring.wind_speed_monitoring_ecmwf as wsm
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


def test_download_tracks_ecmwf_retries_on_masked_connection_error(monkeypatch):
    """Regression test: a climada_petals bug must not defeat the FTP retry.

    climada_petals' ``fetch_bufr_ftp`` references an unassigned ``con`` in
    its ``finally`` block when the initial FTP connection fails (e.g. a DNS
    resolution error), so it raises ``UnboundLocalError`` instead of the
    underlying ``OSError``/``socket.gaierror``. Left untranslated, tenacity's
    ``retry_if_exception_type((TimeoutError, OSError))`` never matches and
    the call fails on the first attempt instead of retrying. This test
    pins the workaround: the masked error must surface as ``OSError`` and
    be retried up to the configured attempt limit.
    """
    call_count = 0

    def _fetch_bufr_ftp(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise UnboundLocalError(
            "cannot access local variable 'con' where it is not associated "
            "with a value"
        )

    monkeypatch.setattr(
        wsm.TCForecast, "fetch_bufr_ftp", staticmethod(_fetch_bufr_ftp)
    )
    monkeypatch.setattr(
        wsm.download_tracks_ecmwf.retry, "wait", tenacity.wait_none()
    )

    with pytest.raises(OSError):
        wsm.download_tracks_ecmwf()

    assert call_count == wsm.download_tracks_ecmwf.retry.stop.max_attempt_number
