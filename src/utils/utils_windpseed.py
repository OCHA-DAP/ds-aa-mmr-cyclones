import cartopy.io.shapereader as shpreader
from dotenv import load_dotenv
import numpy as np
import io

import cartopy.io.shapereader as shpreader
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import ocha_stratus as stratus
from shapely.geometry import box as shapely_box

import src.utils.constants as constants
from src.datasources import codab
from src.utils.logging import get_logger
from src.utils.utils_fun import convert_10m_wind_to_3m, from_ms_to_knots

load_dotenv()
logger = get_logger(__name__)

def compute_wind_speed_at_land(gdf_track):

    gdf_track["wind_speed_knots"] = from_ms_to_knots(
        gdf_track["max_sustained_wind"]
    )

    gdf_track["wind_speed_knots"] = convert_10m_wind_to_3m(
        gdf_track["wind_speed_knots"]
    )

    gdf_track["wind_reduction_factor"] = (
        0.9807 * np.exp(-0.003 * gdf_track["min_dist_km"])
    )

    gdf_track["wind_speed_at_land"] = (
        gdf_track["wind_reduction_factor"] *
        gdf_track["wind_speed_knots"]
    )

    return gdf_track

def compute_distance_to_land(gdf_track, gdf_land):
    """
    Compute distance from each track point to land polygon.
    """

    # project to meters
    gdf_track = gdf_track.to_crs(3857)
    gdf_land = gdf_land.to_crs(3857)

    distances = []

    for point in gdf_track.geometry:
        dist = gdf_land.distance(point).min()
        distances.append(dist)

    gdf_track["min_dist"] = distances
    gdf_track["min_dist_km"] = gdf_track["min_dist"] / 1000

    return gdf_track


# ----------------------------------------------------
# PLOT STORM TRACK
# ----------------------------------------------------

_MYANMAR_NEIGHBOURS = frozenset(
    {"India", "Bangladesh", "China", "Laos", "Thailand", "Cambodia", "Vietnam", "Bhutan"}
)
# Bounding box (lon_min, lon_max, lat_min, lat_max) that frames Myanmar + neighbours.
_PLOT_BBOX = (85.0, 108.0, 7.0, 30.0)


def _load_neighbour_countries() -> gpd.GeoDataFrame:
    """Load Natural Earth 50 m country polygons for Myanmar's neighbours.

    Returns:
        GeoDataFrame with country polygons clipped to the plot bounding box.
    """
    shp_path = shpreader.natural_earth(
        resolution="50m", category="cultural", name="admin_0_countries"
    )
    world = gpd.read_file(shp_path)
    neighbours = world[world["NAME"].isin(_MYANMAR_NEIGHBOURS)].copy()

    lon_min, lon_max, lat_min, lat_max = _PLOT_BBOX
    clip_poly = shapely_box(lon_min, lat_min, lon_max, lat_max)
    neighbours["geometry"] = neighbours.geometry.intersection(clip_poly)
    return neighbours[~neighbours.geometry.is_empty].reset_index(drop=True)


def plot_storm_track(
    storms_area_interest: gpd.GeoDataFrame,
    adm_boundaries: gpd.GeoDataFrame,
    today: str,
    hour: str,
    file_name: str | None = None,
    local_path: str | None = None,
) -> str:
    """Create a plot showing Myanmar, neighbouring countries, and storm tracks.

    The track with the highest maximum wind speed at land is highlighted in
    red; all other tracks are drawn in green. Neighbouring countries are shown
    as a light-gray background with country name labels.

    Args:
        storms_area_interest: GeoDataFrame with storm track points. Must
            contain columns ``sid``, ``time``, ``wind_speed_at_land``, and a
            point geometry.
        adm_boundaries: GeoDataFrame of administrative boundaries. Must
            contain an ``ADM1_EN`` column.
        today: Date string used in the default blob filename.
        hour: Hour string used in the default blob filename.
        file_name: Optional override for the uploaded blob filename.
        local_path: If provided, save the figure to this local filesystem path
            in addition to uploading to blob storage.

    Returns:
        The blob filename under which the plot was uploaded.
    """
    fig, ax = plt.subplots(figsize=(14, 12))

    # --- Neighbouring countries ---
    neighbours = _load_neighbour_countries()
    neighbours.plot(
        ax=ax, facecolor="lightgray", edgecolor="darkgray", linewidth=0.8, zorder=0
    )
    for _, row in neighbours.iterrows():
        label_pt = row.geometry.representative_point()
        ax.text(
            label_pt.x,
            label_pt.y,
            row["NAME"],
            fontsize=7,
            ha="center",
            va="center",
            color="dimgray",
            zorder=1,
        )

    # --- Myanmar administrative boundaries ---
    adm_boundaries.boundary.plot(ax=ax, color="black", linewidth=1.5, zorder=2)

    rakhine = adm_boundaries[adm_boundaries["ADM1_EN"] == "Rakhine"]
    if not rakhine.empty:
        rakhine.plot(
            ax=ax,
            color="#4682b4",
            alpha=0.6,
            edgecolor="black",
            linewidth=0.8,
            zorder=2,
        )

    # --- Storm tracks ---
    storms_plot = storms_area_interest.to_crs(epsg=4326)

    max_wind_sid = (
        storms_plot.groupby("sid")["wind_speed_at_land"].max().idxmax()
    )

    for sid, group in storms_plot.groupby("sid"):
        group = group.sort_values("time")
        color = "red" if sid == max_wind_sid else "green"

        lons = group.geometry.x.values
        lats = group.geometry.y.values

        ax.plot(lons, lats, color=color, linewidth=1, alpha=1, zorder=3)
        group.plot(ax=ax, color=color, markersize=30, alpha=1, zorder=4)

        for _, row in group.iterrows():
            time_str = pd.to_datetime(row["time"]).strftime("%m-%d %H")
            ax.annotate(
                time_str,
                xy=(row.geometry.x, row.geometry.y),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=5,
                bbox=dict(boxstyle="round,pad=0.2", facecolor="yellow", alpha=0.5),
                zorder=5,
            )

    bounds = adm_boundaries.total_bounds  # [minx, miny, maxx, maxy]
    margin = 5
    ax.set_xlim(bounds[0] - margin, bounds[2] + margin)
    ax.set_ylim(bounds[1] - margin, bounds[3] + margin)

    ax.set_xlabel("Longitude", fontsize=12)
    ax.set_ylabel("Latitude", fontsize=12)
    ax.set_title("Cyclone Track over Myanmar", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close()

    if local_path is not None:
        with open(local_path, "wb") as f:
            f.write(buf.read())
        buf.seek(0)

    if file_name is None:
        file_name = f"storm_track_plot_{today}_{hour}.png"
    stratus.upload_blob_data(
        data=buf,
        blob_name=f"{constants.PROJECT_PREFIX}/processed/storm_track_plot/{file_name}",
        stage="dev",
    )
    logger.info(f"Storm track plot uploaded to blob storage: {file_name}")

    return file_name

