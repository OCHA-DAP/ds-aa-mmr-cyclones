"""
Simple cyclone monitoring script
Checks if forecast wind speeds exceed 47 knots over Myanmar
"""
from dotenv import load_dotenv

import pandas as pd
import ocha_stratus as stratus
from src.utils.utils_windpseed import compute_distance_to_land, compute_wind_speed_at_land, plot_storm_track
from src.utils.logging import get_logger
from src.datasources import codab
import src.utils.constants as constants
from src.utils.utils_cma import load_bob_tc_forecasts
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
# DOWNLOAD CYCLONE FORECAST TRACKS
# ----------------------------------------------------

def download_tracks_cma():
    """
    Download CMA data
    """

    data = load_bob_tc_forecasts(blob_prefix="")

    return data


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
    tracks = download_tracks_cma()

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
            file_name = f"monitoring_{today}_{hour}_cma.csv"

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
            file_name = f"wind_exceedance_{today}_{hour}_cma.csv"

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