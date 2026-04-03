"""
Simple cyclone monitoring script
Checks if forecast wind speeds exceed 47 knots over Myanmar
"""
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

from climada_petals.hazard.tc_tracks_forecast import TCForecast
from climada.hazard.centroids import Centroids

# ----------------------------------------------------
# PARAMETERS
# ----------------------------------------------------

WIND_THRESHOLD = constants.wind_speed_alert_level # wind speed threshold (knots)


# ----------------------------------------------------
# CREATE GRID OVER MYANMAR
# ----------------------------------------------------

def create_centroids():
    """
    Create grid points covering Myanmar.
    Wind speed will be calculated at these locations.
    """

    lats = np.arange(constants.LAT_MIN, constants.LAT_MAX, constants.GRID_RES)
    lons = np.arange(constants.LON_MIN, constants.LON_MAX, constants.GRID_RES)

    lon_grid, lat_grid = np.meshgrid(lons, lats)

    centroids = Centroids(lat=lat_grid.flatten(), lon=lon_grid.flatten())

    return centroids


# ----------------------------------------------------
# DOWNLOAD CYCLONE FORECAST TRACKS
# ----------------------------------------------------

def download_tracks():
    """
    Download ECMWF tropical cyclone forecasts.
    """

    # download BUFR files from ECMWF
    bufr_files = TCForecast.fetch_bufr_ftp()

    forecast = TCForecast()
    forecast.fetch_ecmwf(files=bufr_files)

    return forecast.data


# ----------------------------------------------------
# FILTER CYCLONES NEAR MYANMAR
# ----------------------------------------------------

def filter_myanmar_tracks(tracks, buffer_km=200):
    """
    Keep only storms that pass near Myanmar (buffered area).
    """

    # --- Load and buffer Myanmar ---
    adm0 = codab.load_codab_from_blob(admin_level=0)

    gdf_adm_projected = adm0.to_crs(epsg=constants.MMR_UTM)

    gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
        buffer_km* 1000
    )

    gdf_adm_buffered = gdf_adm_projected.to_crs(epsg=4326)

    # --- Convert tracks to GeoDataFrame ---
    dfs = []

    for i, track in enumerate(tracks):

        df = track.to_dataframe().reset_index()

        df["storm_name"] = track.name
        df["sid"] = track.sid
        df["ensemble_number"] = track.ensemble_number
        df["is_ensemble"] = track.is_ensemble
        df["category"] = track.attrs.get("category", "N/A")

        dfs.append(df)

    gdf_all = pd.concat(dfs, ignore_index=True)

    gdf_all = gpd.GeoDataFrame(
        gdf_all,
        geometry=gpd.points_from_xy(gdf_all.lon, gdf_all.lat),
        crs="EPSG:4326"
    )

    # --- Spatial filter (points within buffer) ---
    gdf_filtered = gpd.sjoin(
        gdf_all,
        gdf_adm_buffered,
        how="inner",
        predicate="within"
    ).drop(columns="index_right")

    return gdf_filtered


# ----------------------------------------------------
# PROCESS STORM
# ----------------------------------------------------
def track_to_gdf(track):
    """
    Convert CLIMADA track to GeoDataFrame
    """

    df = track.to_dataframe().reset_index()

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326"
    )

    return gdf

def process_storm(track, gdf_land):

    # convert track
    gdf_track = track_to_gdf(track)

    # distance to land
    gdf_track = compute_distance_to_land(gdf_track, gdf_land)

    # compute reduced wind
    gdf_track = compute_wind_speed_at_land(gdf_track)

    return gdf_track

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

def plot_storm_track(storms_area_interest, adm_boundaries, today, hour):
    """
    Create a plot showing Myanmar boundaries, highlighted Rakhine state, 
    storm track with lines connecting same ensemble/sid, and time labels.
    """

    fig, ax = plt.subplots(figsize=(14, 12))

    # Load ADM1 boundaries to highlight Rakhine
    adm1_boundaries = codab.load_codab_from_blob(admin_level=1)

    # Plot all Myanmar boundaries
    adm_boundaries.boundary.plot(ax=ax, color='black', linewidth=1.5)

    # Highlight Rakhine state
    rakhine = adm1_boundaries[adm1_boundaries['ADM1_EN'] == 'Rakhine']
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
    file_name = f"storm_track_plot_{today}_{hour}.png"
    stratus.upload_blob_data(
        data=buf,
        blob_name=file_name,
        stage="dev",
        container_name=f"projects/{constants.PROJECT_PREFIX}/processed",
    )
    logger.info(f"Storm track plot uploaded to blob storage: {file_name}")

    return file_name


# ----------------------------------------------------
# MAIN WORKFLOW
# ----------------------------------------------------

def main():
    now = pd.Timestamp.now()
    today = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")

    adm_boundaries = codab.load_codab_from_blob(admin_level=constants.adm_level)
    adm_buffer = 0  # Use 0, 50 or 100: these are in km
    gdf_adm_projected = adm_boundaries.to_crs(epsg=constants.MMR_UTM)
    gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
        adm_buffer * 1000
    )
    gdf_adm_projected_filtered = gdf_adm_projected.loc[adm_boundaries[constants.adm_column].isin(constants.ADM_LIST)]

    logger.info("Downloading cyclone forecasts...")
    tracks = download_tracks()

    logger.info("Filtering storms near Myanmar...")
    gdf_filtered = filter_myanmar_tracks(tracks, buffer_km=2000)

    if gdf_filtered.empty:
        logger.info("No cyclones around Myanmar.")
        return
    else:

        gdf_track = compute_distance_to_land(
            gdf_filtered,
            gdf_adm_projected_filtered
        )
        # compute wind reduction
        gdf_track = compute_wind_speed_at_land(gdf_track)


        close_storms = (
            gdf_track
            .groupby(["sid", "ensemble_number"])["min_dist_km"]
            .min()
            .reset_index()
        )

        close_storms_idx = close_storms[
            close_storms["min_dist_km"] <= constants.buffer_km
            ]

        storms_area_interest = gdf_track[((gdf_track["ensemble_number"].isin(close_storms_idx["ensemble_number"])) & (gdf_track["sid"].isin(close_storms_idx["sid"])))]

        wind_storms = (
            gdf_track
            .groupby(["sid", "ensemble_number"])["wind_speed_at_land"]
            .max()
            .reset_index()
        )

        wind_storms = wind_storms[
            wind_storms["wind_speed_at_land"] >= constants.wind_speed_alert_level
            ]

        # --- CLOSE STORMS ---
        if not storms_area_interest.empty:
            logger.info("There are monitoring data to be uploaded to blob storage.")
            file_name = f"monitoring_{today}_{hour}.csv"

            stratus.upload_csv_to_blob(
                df=storms_area_interest,
                blob_name=file_name,
                stage="dev",
                container_name=f"projects/{constants.PROJECT_PREFIX}/processed",
            )
            logger.info("Monitoring data uploaded to blob storage.")

            # Generate and upload plot
            plot_storm_track(storms_area_interest, adm_boundaries, today, hour)

        # --- WIND STORMS ---
        if not wind_storms.empty:
            logger.info("There are data exceeding the wind speed threshold.")
            file_name = f"wind_exceedance_{today}_{hour}.csv"

            stratus.upload_csv_to_blob(
                df=wind_storms,
                blob_name=file_name,
                stage="dev",
                container_name=f"projects/{constants.PROJECT_PREFIX}/processed",
            )
            logger.info("Data exceeding the wind speed threshold uploaded to blob storage.")

# ----------------------------------------------------

if __name__ == "__main__":
    main()