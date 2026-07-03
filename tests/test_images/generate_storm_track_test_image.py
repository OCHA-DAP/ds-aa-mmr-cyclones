"""Generate a test image for plot_storm_track with neighbouring countries.

Run from the repo root:
    python tests/test_images/generate_storm_track_test_image.py

The output is saved to tests/test_images/storm_track_with_neighbours.png.
This script uses synthetic storm data and Natural Earth boundaries so it
requires no blob storage access.
"""

import pathlib
import sys

import cartopy.io.shapereader as shpreader
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from src.utils.utils_windpseed import (  # noqa: E402
    _MYANMAR_NEIGHBOURS,
    _PLOT_BBOX,
    _load_neighbour_countries,
    plot_storm_track,
)

OUTPUT_PATH = pathlib.Path(__file__).parent / "storm_track_with_neighbours.png"


def _make_myanmar_boundaries() -> gpd.GeoDataFrame:
    """Load Myanmar admin-0 boundary from Natural Earth as a mock adm1 GDF.

    Returns:
        GeoDataFrame with a single row representing Myanmar, with an ``ADM1_EN``
        column set to ``"Rakhine"`` so the highlight branch in ``plot_storm_track``
        is exercised.
    """
    shp_path = shpreader.natural_earth(
        resolution="50m", category="cultural", name="admin_0_countries"
    )
    world = gpd.read_file(shp_path)
    mmr = world[world["NAME"] == "Myanmar"].copy()
    mmr["ADM1_EN"] = "Rakhine"
    return mmr.reset_index(drop=True)


def _make_mock_storms() -> gpd.GeoDataFrame:
    """Create two synthetic storm tracks near Myanmar.

    Track A passes through Rakhine coast (highest wind speed -> red).
    Track B remains offshore in the Bay of Bengal (lower wind speed -> green).

    Returns:
        GeoDataFrame with columns ``sid``, ``time``, ``wind_speed_at_land``,
        and point geometry in EPSG:4326.
    """
    track_a = [
        (92.5, 18.0, "2023-05-10 00:00", 55.0),
        (93.0, 17.5, "2023-05-10 06:00", 60.0),
        (93.5, 17.0, "2023-05-10 12:00", 65.0),
        (94.0, 16.8, "2023-05-10 18:00", 62.0),
        (94.5, 16.5, "2023-05-11 00:00", 58.0),
    ]
    track_b = [
        (90.0, 14.0, "2023-06-01 00:00", 30.0),
        (90.5, 14.5, "2023-06-01 06:00", 32.0),
        (91.0, 15.0, "2023-06-01 12:00", 35.0),
        (91.5, 15.5, "2023-06-01 18:00", 33.0),
    ]
    rows = []
    for lon, lat, t, spd in track_a:
        rows.append({"sid": "A", "time": t, "wind_speed_at_land": spd,
                     "geometry": Point(lon, lat)})
    for lon, lat, t, spd in track_b:
        rows.append({"sid": "B", "time": t, "wind_speed_at_land": spd,
                     "geometry": Point(lon, lat)})

    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


def main() -> None:
    """Generate and save the test image."""
    adm_boundaries = _make_myanmar_boundaries()
    storms = _make_mock_storms()

    # Monkey-patch upload so it is a no-op during local test image generation.
    import ocha_stratus as stratus
    stratus.upload_blob_data = lambda **kwargs: None  # type: ignore[assignment]

    plot_storm_track(
        storms_area_interest=storms,
        adm_boundaries=adm_boundaries,
        today="test",
        hour="00",
        local_path=str(OUTPUT_PATH),
    )
    print(f"Saved test image: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
