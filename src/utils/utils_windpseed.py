from dotenv import load_dotenv
import numpy as np
import io

import pandas as pd
import ocha_stratus as stratus
from src.utils.utils_fun import from_ms_to_knots, convert_10m_wind_to_3m
from src.utils.logging import get_logger
from src.datasources import codab
import src.utils.constants as constants
import geopandas as gpd
import matplotlib.pyplot as plt

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

def plot_storm_track(storms_area_interest, adm_boundaries, today, hour, file_name:str=None):
    """
    Create a plot showing Myanmar boundaries, highlighted Rakhine state,
    storm track with lines connecting same ensemble/sid, and time labels.
    """

    fig, ax = plt.subplots(figsize=(14, 12))

    # Plot all Myanmar boundaries
    adm_boundaries.boundary.plot(ax=ax, color='black', linewidth=1.5)

    # Highlight Rakhine state
    rakhine = adm_boundaries[adm_boundaries['ADM1_EN'] == 'Rakhine']
    if not rakhine.empty:
        rakhine.plot(ax=ax, color='lightblue', alpha=0.5, edgecolor='gray', linewidth=1.5)

    # Convert storm data to EPSG:4326
    storms_area_interest_plot = storms_area_interest.to_crs(epsg=4326)

    # Group by sid and ensemble_number to draw lines
    grouped = storms_area_interest_plot.groupby(['sid', 'ensemble_number'])

    for (sid, ensemble_num), group in grouped:
        # Sort by time to ensure proper line connection
        group = group.sort_values('time')

        # Extract coordinates
        lons = group.geometry.x.values
        lats = group.geometry.y.values

        # Plot line connecting the track points
        ax.plot(lons, lats, color='red', linewidth=1, alpha=0.7)

        # Plot points
        group.plot(ax=ax, color='red', markersize=30, alpha=0.8, zorder=5)

        # Add time labels to each point
        for idx, row in group.iterrows():
            time_str = pd.to_datetime(row['time']).strftime('%m-%d %H')
            ax.annotate(
                time_str,
                xy=(row.geometry.x, row.geometry.y),
                xytext=(5, 5),
                textcoords='offset points',
                fontsize=7,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7)
            )

    # Add labels and title
    ax.set_xlabel('Longitude', fontsize=12)
    ax.set_ylabel('Latitude', fontsize=12)
    ax.set_title('Cyclone Track over Myanmar', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # Save plot to bytes buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close()

    # Upload to blob storage
    if file_name is None:
        file_name = f"storm_track_plot_{today}_{hour}.png"
    stratus.upload_blob_data(
        data=buf,
        blob_name=file_name,
        stage="dev",
        container_name=f"projects/{constants.PROJECT_PREFIX}/processed/storm_track_plot",
    )
    logger.info(f"Storm track plot uploaded to blob storage: {file_name}")

    return file_name

