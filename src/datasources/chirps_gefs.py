import datetime
import sys
import tempfile
import requests
import ocha_stratus as stratus
import pandas as pd
import rioxarray as rxr
import xarray as xr
from io import BytesIO
from azure.core.exceptions import ResourceNotFoundError
from tqdm import tqdm

from src.datasources import codab
from src.utils import constants
from src.utils.logging import get_logger

logger = get_logger(__name__)

CHIRPS_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
    "CHIRPS-GEFS_precip_v12/daily_{chirps_gefs_lead_time}day/"
    "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
    "data.{valid_year}.{valid_month:02d}{valid_day:02d}.tif"
)
CHIRPS_GEFS_BLOB_DIR = "raw/chirps_gefs"

def download_recent_chirps_gefs(
    date: datetime.date | None = None,
) -> None:
    """Download recent CHIRPS-GEFS data up to the specified date.

    Args:
        date: The end date for the download range. Defaults to today.
    """
    if date is None:
        date = datetime.date.today()

    adm0 = codab.load_codab_from_blob(admin_level=0)
    total_bounds = adm0.total_bounds

    issue_date_range = pd.date_range(
        start=f"{date.year}-03-15",
        end=date,
        freq="D",
    )

    existing_files = stratus.list_container_blobs(
        name_starts_with=f"{constants.PROJECT_PREFIX}/"
        f"{CHIRPS_GEFS_BLOB_DIR}/"
        f"chirps-gefs-mmr_issued-{date.year}"
    )

    existing_issue_dates = [
        pd.Timestamp(f.split("issued-")[1].split("_valid-")[0])
        for f in existing_files
    ]
    logger.info(
        f"Found {len(existing_issue_dates)} existing files for {date.year}"
    )
    download_dates = [
        d for d in issue_date_range if d not in existing_issue_dates
    ]
    logger.info(
        f"Downloading {len(download_dates)} new issue dates for {date.year}: "
        f"{[str(x.date()) for x in download_dates]}"
    )

    for issue_date in tqdm(
        download_dates,
        disable=not sys.stdout.isatty(),
    ):
        for leadtime in range(constants.chirps_gefs_lead_time):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            download_chirps_gefs(
                issue_date,
                valid_date,
                total_bounds,
            )


def download_chirps_gefs(
    issue_date: pd.Timestamp,
    valid_date: pd.Timestamp,
    total_bounds,

):
    """Download CHIRPS GEFS data for a specific issue and valid date."""
    url = CHIRPS_GEFS_URL.format(
        iss_year=int(issue_date.year),
        iss_month=int(issue_date.month),
        iss_day=int(issue_date.day),
        valid_year=int(valid_date.year),
        valid_month=int(valid_date.month),
        valid_day=int(valid_date.day),
        chirps_gefs_lead_time=int(constants.chirps_gefs_lead_time),
    )
    output_filename = (
        f"chirps-gefs-mmr_issued-"
        f"{issue_date.date()}_valid-{valid_date.date()}.tif"
    )
    output_path = (
        f"projects/{constants.PROJECT_PREFIX}/{CHIRPS_GEFS_BLOB_DIR}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".tif") as tmpfile:
            tmpfile.write(response.content)
            temp_filename = tmpfile.name

        with rxr.open_rasterio(temp_filename) as da:
            da_aoi = da.rio.clip_box(*total_bounds)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".tif") as tmpfile2:
                output_tmp = tmpfile2.name
                da_aoi.rio.to_raster(output_tmp, driver="COG")

        with open(output_tmp, "rb") as f:
            stratus.upload_blob_data(data=f, blob_name=output_filename, container_name=output_path)

    except Exception as e:
        logger.warning(
            f"Failed to download or process CHIRPS GEFS data for "
            f"{issue_date.date()} valid {valid_date.date()}: {e}"
        )
    return



def load_chirps_gefs_raster(
    issue_date: pd.Timestamp, valid_date: pd.Timestamp
):
    """Load CHIRPS GEFS raster data for a specific issue and valid date."""
    filename = (
        f"chirps-gefs-mmr_"
        f"issued-{issue_date.date()}_valid-{valid_date.date()}.tif"
    )
    data = stratus.load_blob_data(
        f"{constants.PROJECT_PREFIX}/{CHIRPS_GEFS_BLOB_DIR}/{filename}"
    )
    blob_data = BytesIO(data)
    da = rxr.open_rasterio(blob_data)
    da = da.squeeze(drop=True)
    return da



def process_recent_chirps_gefs(
    date: datetime.date | None = None, verbose: bool = False
) -> pd.DataFrame:
    """Process recent CHIRPS-GEFS forecasts for Myanmar up to the specified date.

    Args:
        date: The end date for the processing range. Defaults to today.
        verbose: Whether to print verbose output. Defaults to False.

    Returns:
        DataFrame with processed CHIRPS-GEFS mean daily data.
    """
    if date is None:
        date = datetime.date.today()
    try:
        existing_df = load_recent_chirps_gefs_mean_daily()
    except ResourceNotFoundError:
        logger.warning(
            "No existing data found for recent CHIRPS-GEFS mean daily."
        )
        existing_df = pd.DataFrame(
            columns=["issue_date", "valid_date", "mean"]
        )
    adm1 = codab.load_codab_from_blob(admin_level=1)
    adm1 = adm1[adm1["ADM1_EN"].isin(constants.ADM_LIST)]
    issue_date_range = pd.date_range(
        start=f"{date.year}-03-25",
        end=date,
        freq="D",
    )
    unprocessed_issue_date_range = [
        d
        for d in issue_date_range
        if d not in existing_df["issue_date"].unique()
    ]
    logger.info(
        f"Processing {len(unprocessed_issue_date_range)} new issue dates "
        "for recent CHIRPS-GEFS: "
        f"{[str(x.date()) for x in unprocessed_issue_date_range]}"
    )
    dfs = []
    for issue_date in tqdm(
        unprocessed_issue_date_range, disable=not sys.stdout.isatty()
    ):
        if issue_date in existing_df["issue_date"].unique():
            if verbose:
                print(f"Skipping {issue_date}, already processed")
            continue
        das_i = []
        for leadtime in range(constants.chirps_gefs_lead_time):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            try:
                da_in = load_chirps_gefs_raster(issue_date, valid_date)
                da_in["valid_date"] = valid_date
                das_i.append(da_in)
            except ResourceNotFoundError as e:
                if verbose:
                    print(f"{e} for {issue_date} {valid_date}")

        if das_i:
            logger.info(
                f"Processing {len(das_i)} files for issue_date {issue_date}"
            )
            da_i = xr.concat(das_i, dim="valid_date")
            da_i_clip = da_i.rio.clip(adm1.geometry, all_touched=True)
            df_in = (
                da_i_clip.mean(dim=["x", "y"])
                .to_dataframe(name="mean")["mean"]
                .reset_index()
            )
            df_in["issue_date"] = issue_date
            dfs.append(df_in)
        else:
            logger.warning(
                f"No files found for issue_date {issue_date.date()}, skipping."
            )

    updated_df = pd.concat(dfs + [existing_df], ignore_index=True)
    blob_name = (
        f"{constants.PROJECT_PREFIX}/processed/chirps_gefs/"
        f"mmr_chirps_gefs_mean_daily.parquet"
    )
    stratus.upload_parquet_to_blob(blob_name=blob_name, df=updated_df)
    return updated_df


def load_recent_chirps_gefs_mean_daily():
    return stratus.load_parquet_from_blob(
        f"{constants.PROJECT_PREFIX}/processed/chirps_gefs/"
        "mmr_chirps_gefs_mean_daily.parquet"
    )

def check_chirps_gefs_trigger(
    df: pd.DataFrame, date: datetime.date | None = None
) -> None:
    """Check CHIRPS-GEFS data against the rainfall threshold.

    Args:
        df: DataFrame with CHIRPS-GEFS mean daily data.
        date: The date to use for output file naming. Defaults to today.
    """
    if date is None:
        date = datetime.date.today()
    date_str = date.strftime("%Y-%m-%d")
    df = df.sort_values(["issue_date", "valid_date"])

    df["rolling_sum_3"] = (
        df.groupby(["issue_date"])["mean"]
        .rolling(3, min_periods=1)
        .sum()
        .reset_index(level=[0, 1], drop=True)
    )
    df_trigger_rainfall = df[
        df["rolling_sum_3"] >= constants.rainfall_alert_level_forecast
    ]
    if not df_trigger_rainfall.empty:
        file_name = (
            f"{constants.PROJECT_PREFIX}/processed/"
            f"rainfall_exceedance_{date_str}.csv"
        )
        stratus.upload_csv_to_blob(blob_name=file_name, df=df_trigger_rainfall)
        logger.info("Rainfall threshold exceeded.")
    else:
        logger.info("Rainfall threshold not exceeded.")

