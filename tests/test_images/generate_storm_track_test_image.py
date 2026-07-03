"""Generate a test image for plot_storm_track with neighbouring countries.

Run from the repo root:
    python tests/test_images/generate_storm_track_test_image.py

The output is saved to tests/test_images/storm_track_with_neighbours.png.
Requires blob storage access to load CODAB admin boundaries.
"""

import pathlib
import sys

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point

load_dotenv()

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from src.datasources import codab  # noqa: E402
from src.utils import constants  # noqa: E402
from src.utils.utils_windpseed import plot_storm_track  # noqa: E402

OUTPUT_PATH = pathlib.Path(__file__).parent / "storm_track_with_neighbours.png"


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
    adm_boundaries = codab.load_codab_from_blob(admin_level=constants.adm_level)
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
