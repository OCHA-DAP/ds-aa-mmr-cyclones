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

        df["track_id"] = i
        df["storm_name"] = track.name

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

    # --- Extract track IDs that intersect buffer ---
    valid_ids = gdf_filtered["track_id"].unique()

    # --- Filter original tracks ---
    filtered_tracks = [
        track for i, track in enumerate(tracks)
        if i in valid_ids
    ]

    return filtered_tracks


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
    gdf_track = compute_land_wind(gdf_track)

    return gdf_track

def compute_land_wind(gdf_track):

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
    filtered_tracks = filter_myanmar_tracks(tracks, buffer_km=2000)

    if not filtered_tracks:
        logger.info("No cyclones around Myanmar.")
        return
    else:

        gdfs = []

        for i, ds in enumerate(filtered_tracks):
            df = ds.to_dataframe().reset_index()

            # create geometry
            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df["lon"], df["lat"]),
                crs="EPSG:4326"
            )

            # optional: keep track of source dataset
            gdf["source"] = i
            gdf["storm_name"] = ds.name

            gdfs.append(gdf)

        final_gdf = pd.concat(gdfs, ignore_index=True)
        gdf_track = compute_distance_to_land(
            final_gdf,
            gdf_adm_projected_filtered
        )
        # compute wind reduction
        gdf_track = compute_land_wind(gdf_track)


        close_storms = (
            gdf_track
            .groupby(["source", "storm_name"])["min_dist_km"]
            .min()
            .reset_index()
        )

        close_storms_idx = close_storms[
            close_storms["min_dist_km"] <= constants.buffer_km
            ]

        storms_area_interest = gdf_track[((gdf_track["storm_name"].isin(close_storms_idx["storm_name"])) & (gdf_track["source"].isin(close_storms_idx["source"])))]

        wind_storms = (
            gdf_track
            .groupby(["source", "storm_name"])["wind_speed_at_land"]
            .max()
            .reset_index()
        )

        wind_storms = wind_storms[
            wind_storms["wind_speed_at_land"] >= constants.wind_speed_alert_level
            ]

        # --- CLOSE STORMS ---
        if not storms_area_interest.empty:
            file_name = f"monitoring_{today}_{hour}.csv"

            stratus.upload_csv_to_blob(
                df=storms_area_interest,
                blob_name=file_name,
                stage="dev",
                container_name=f"projects/{constants.PROJECT_PREFIX}/processed",
            )

        # --- WIND STORMS ---
        if not wind_storms.empty:
            file_name = f"wind_exceedance_{today}_{hour}.csv"

            stratus.upload_csv_to_blob(
                df=wind_storms,
                blob_name=file_name,
                stage="dev",
                container_name=f"projects/{constants.PROJECT_PREFIX}/processed",
            )

# ----------------------------------------------------

if __name__ == "__main__":
    main()