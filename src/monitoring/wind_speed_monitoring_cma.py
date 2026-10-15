"""
Simple cyclone monitoring script
Checks if forecast wind speeds exceed 47 knots over Myanmar
"""
from dotenv import load_dotenv

import pandas as pd
import ocha_stratus as stratus
from src.utils.utils_windpseed import (
    compute_wind_speed_at_adm_list,
    get_wind_exceedance,
    plot_storm_track,
)
from src.utils.logging import get_logger
from src.datasources import codab
import src.utils.constants as constants
from src.utils.utils_cma import load_typhoon_tc_forecasts
import geopandas as gpd

load_dotenv()
logger = get_logger(__name__)

# ----------------------------------------------------
# PARAMETERS
# ----------------------------------------------------

WIND_THRESHOLD = constants.wind_speed_alert_level # wind speed threshold (knots)

# ----------------------------------------------------
# DOWNLOAD CYCLONE FORECAST TRACKS
# ----------------------------------------------------

def download_tracks_cma() -> pd.DataFrame:
    """Download and normalise CMA typhoon forecast data.

    Returns:
        DataFrame with columns renamed to match the shared processing
        utilities: ``sid``, ``time``, ``max_sustained_wind``,
        ``ensemble_number``, ``storm_name``, ``lon``, ``lat``.
    """
    df = load_typhoon_tc_forecasts(
        blob_prefix="ds-cma-datasharing/cma_ftp/data_out/typhoon/"
    )
    df = df.rename(
        columns={
            "storm_id": "sid",
            "valid_datetime": "time",
            "wind_speed_ms": "max_sustained_wind",
        }
    )
    df["ensemble_number"] = 0
    return df


# ----------------------------------------------------
# FILTER CYCLONES NEAR MYANMAR
# ----------------------------------------------------

def filter_myanmar_tracks(tracks: pd.DataFrame, buffer_km: int = 200) -> gpd.GeoDataFrame:
    """Keep only storms that pass near Myanmar (buffered area).

    Args:
        tracks: DataFrame returned by ``download_tracks_cma`` with columns
            ``lon``, ``lat``, ``sid``, ``time``, ``max_sustained_wind``,
            ``ensemble_number``, and ``storm_name``.
        buffer_km: Buffer distance in kilometres around Myanmar.

    Returns:
        GeoDataFrame of track points within the buffered Myanmar boundary.
    """
    adm0 = codab.load_codab_from_blob(admin_level=0)
    gdf_adm_projected = adm0.to_crs(epsg=constants.MMR_UTM)
    gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
        buffer_km * 1000
    )
    gdf_adm_buffered = gdf_adm_projected.to_crs(epsg=4326)

    gdf_all = gpd.GeoDataFrame(
        tracks,
        geometry=gpd.points_from_xy(tracks.lon, tracks.lat),
        crs="EPSG:4326",
    )

    gdf_filtered = gpd.sjoin(
        gdf_all,
        gdf_adm_buffered,
        how="inner",
        predicate="within",
    ).drop(columns="index_right")

    return gdf_filtered


# ----------------------------------------------------
# MAIN WORKFLOW
# ----------------------------------------------------

def main():
    now = pd.Timestamp.now()
    today = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")

    adm_boundaries = codab.load_codab_from_blob(admin_level=constants.adm_level)
    adm1_boundaries = codab.load_codab_from_blob(admin_level=1)

    logger.info("Downloading cyclone forecasts...")
    tracks = download_tracks_cma()

    logger.info("Filtering storms near Myanmar...")
    gdf_filtered = filter_myanmar_tracks(tracks, buffer_km=2000)

    if gdf_filtered.empty:
        logger.info("No cyclones around Myanmar.")
        return
    else:

        # AA Target area (ADM3_LIST): drives distances and the trigger
        gdf_track = compute_wind_speed_at_adm_list(
            gdf_filtered,
            adm_boundaries,
            constants.adm_column,
            constants.ADM3_LIST,
            constants.wind_speed_alert_level,
        )
        # Wider area (ADM1_LIST), e.g. Rakhine
        gdf_adm1_track = compute_wind_speed_at_adm_list(
            gdf_filtered,
            adm1_boundaries,
            "ADM1_EN",
            constants.ADM1_LIST,
            constants.wind_speed_alert_level,
        )
        gdf_track["wind_speed_at_land_adm1"] = gdf_adm1_track["wind_speed_at_land"]
        gdf_track["exceedance_adm1"] = gdf_adm1_track["exceedance_adm"]
        gdf_track["source"] = "CMA"

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

        wind_storms = get_wind_exceedance(
            gdf_track, "wind_speed_at_land", constants.wind_speed_alert_level
        )
        wind_storms_adm1 = get_wind_exceedance(
            gdf_track, "wind_speed_at_land_adm1", constants.wind_speed_alert_level
        )

        # --- CLOSE STORMS ---
        if not storms_area_interest.empty:
            logger.info("There are monitoring data to be uploaded to blob storage.")
            file_name = f"monitoring_{today}_{hour}_cma.csv"

            stratus.upload_csv_to_blob(
                df=storms_area_interest,
                blob_name=f"{constants.PROJECT_PREFIX}/processed/{file_name}",
                stage="dev",
            )
            logger.info("Monitoring data uploaded to blob storage.")

            # Generate and upload plot
            plot_storm_track(
                storms_area_interest,
                adm_boundaries,
                today,
                hour,
                filter_name="all",
            )

        # --- WIND STORMS (AA Target area, ADM3_LIST) ---
        if not wind_storms.empty:
            logger.info("There are data exceeding the wind speed threshold.")
            file_name = f"wind_exceedance_adm3_{today}_{hour}_cma.csv"

            stratus.upload_csv_to_blob(
                df=wind_storms,
                blob_name=f"{constants.PROJECT_PREFIX}/processed/{file_name}",
                stage="dev",
            )
            logger.info("Data exceeding the wind speed threshold uploaded to blob storage.")


        # --- WIND STORMS (wider area, ADM1_LIST) ---
        if not wind_storms_adm1.empty:
            logger.info("There are data exceeding the wind speed threshold in ADM1.")
            stratus.upload_csv_to_blob(
                df=wind_storms_adm1,
                blob_name=(
                    f"{constants.PROJECT_PREFIX}/processed/"
                    f"wind_exceedance_adm1_{today}_{hour}_cma.csv"
                ),
                stage="dev",
            )
            logger.info("ADM1 wind exceedance data uploaded to blob storage.")

# ----------------------------------------------------

if __name__ == "__main__":
    main()