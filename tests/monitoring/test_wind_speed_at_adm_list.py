import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, box

from src.utils.utils_windpseed import (
    compute_wind_speed_at_adm_list,
    get_wind_exceedance,
)

ADM_COLUMN = "ADM3_EN"


@pytest.fixture
def gdf_adm() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {ADM_COLUMN: ["Sittwe", "Yangon"]},
        geometry=[box(92.0, 20.0, 93.0, 21.0), box(96.0, 16.0, 97.0, 17.0)],
        crs="EPSG:4326",
    )


def _track(points: list[Point], wind_ms: float) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"max_sustained_wind": [wind_ms] * len(points)},
        geometry=points,
        crs="EPSG:4326",
    )


def test_exceedance_inside_adm_list(gdf_adm):
    track = _track([Point(92.5, 20.5)], wind_ms=40.0)
    result = compute_wind_speed_at_adm_list(
        track, gdf_adm, ADM_COLUMN, ["Sittwe"], threshold=47
    )
    assert result["exceedance_adm"].iloc[0] == "Sittwe"
    assert result["wind_speed_at_land"].iloc[0] == pytest.approx(
        result["wind_speed_at_land_country"].iloc[0]
    )


def test_exceedance_outside_adm_list_is_reduced(gdf_adm):
    track = _track([Point(96.5, 16.5)], wind_ms=40.0)
    result = compute_wind_speed_at_adm_list(
        track, gdf_adm, ADM_COLUMN, ["Sittwe"], threshold=47
    )
    assert result["exceedance_adm"].iloc[0] == "Yangon"
    assert result["wind_speed_at_land_country"].iloc[0] >= 47
    assert result["wind_speed_at_land"].iloc[0] < 47


def test_no_exceedance(gdf_adm):
    track = _track([Point(92.5, 20.5)], wind_ms=10.0)
    result = compute_wind_speed_at_adm_list(
        track, gdf_adm, ADM_COLUMN, ["Sittwe"], threshold=47
    )
    assert result["exceedance_adm"].isna().all()


def test_get_wind_exceedance_keeps_members_at_threshold():
    track = pd.DataFrame(
        {
            "sid": ["A", "A", "B"],
            "ensemble_number": [0, 0, 0],
            "wind": [30.0, 47.0, 46.9],
        }
    )
    result = get_wind_exceedance(track, "wind", threshold=47)
    assert result["sid"].tolist() == ["A"]
