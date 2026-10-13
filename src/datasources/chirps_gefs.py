import datetime
import html.parser
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import requests
import ocha_stratus as stratus
import pandas as pd
import rioxarray as rxr
import xarray as xr
from io import BytesIO
from azure.core.exceptions import ResourceNotFoundError
from tqdm import tqdm
import time
import random

from src.datasources import codab
from src.utils import constants
from src.utils.logging import get_logger

logger = get_logger(__name__)

CHIRPS_GEFS_BLOB_DIR = "raw/chirps_gefs"
CHIRPS_GEFS_BLOB_PREFIX = "chirps-gefs-mmr_issued-"
CHIRPS_GEFS_START_DATE = datetime.date(2026, 3, 15)
CHIRPS_GEFS_V3_START_DATE = datetime.date(2026, 7, 1)
DEFAULT_MAX_ISSUE_DATES = 3


@dataclass(frozen=True)
class _ChirpsGefsSource:
    """URL templates for one version of the CHIRPS-GEFS daily forecasts.

    Attributes:
        file_url: Template for one daily raster, formatted with
            ``iss_year``, ``iss_month``, ``iss_day`` and ``valid_year``,
            ``valid_month``, ``valid_day``.
        dir_url: Template for a month directory listing, formatted with
            ``year`` and ``month``. Entries are two-digit issue days.
    """

    file_url: str
    dir_url: str


_CHIRPS_GEFS_V12 = _ChirpsGefsSource(
    file_url=(
        "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
        "CHIRPS-GEFS_precip_v12/daily_16day/"
        "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
        "data.{valid_year}.{valid_month:02d}{valid_day:02d}.tif"
    ),
    dir_url=(
        "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
        "CHIRPS-GEFS_precip_v12/daily_16day/{year}/{month:02d}/"
    ),
)
_CHIRPS_GEFS_V3 = _ChirpsGefsSource(
    file_url=(
        "https://data.chc.ucsb.edu/products/CHIRPS-GEFS/v3/daily/global/"
        "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
        "c3g_{valid_year}.{valid_month:02d}.{valid_day:02d}.tif"
    ),
    dir_url=(
        "https://data.chc.ucsb.edu/products/CHIRPS-GEFS/v3/daily/global/"
        "{year}/{month:02d}/"
    ),
)


def _source_for(issue_date: datetime.date) -> _ChirpsGefsSource:
    """Return the CHIRPS-GEFS source that serves a given issue date.

    Args:
        issue_date: Forecast issue date.

    Returns:
        The v12 source before 2026-07-01 and the v3 source from that date.
    """
    if issue_date < CHIRPS_GEFS_V3_START_DATE:
        return _CHIRPS_GEFS_V12
    return _CHIRPS_GEFS_V3


def _blob_filename(issue_date: pd.Timestamp, valid_date: pd.Timestamp) -> str:
    """Return the blob file name for an issue and valid date pair."""
    return (
        f"{CHIRPS_GEFS_BLOB_PREFIX}{issue_date.date()}"
        f"_valid-{valid_date.date()}.tif"
    )


class _DirLinkParser(html.parser.HTMLParser):
    """Collect href values from an nginx directory listing."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag == "a":
            for attr, value in attrs:
                if attr == "href" and value is not None:
                    self.links.append(value)


def _fetch_available_issue_dates(
    candidates: list[pd.Timestamp],
) -> set[datetime.date]:
    """Query CHIRPS-GEFS server directory listings to find available dates.

    Args:
        candidates: Issue dates to check availability for.

    Returns:
        Set of dates confirmed available on the server.

    Raises:
        RuntimeError: If the server returns 403 for any queried directory.
    """
    year_months = {(ts.year, ts.month) for ts in candidates}
    available: set[datetime.date] = set()
    for year, month in year_months:
        source = _source_for(datetime.date(year, month, 1))
        url = source.dir_url.format(year=year, month=month)
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                raise RuntimeError(
                    f"Access denied (403) querying directory: {url}"
                ) from e
            logger.warning(f"Could not fetch directory listing {url}: {e}")
            continue

        parser = _DirLinkParser()
        parser.feed(response.text)
        for link in parser.links:
            # Directory entries are "DD/" (two-digit day)
            stripped = link.rstrip("/")
            if stripped.isdigit() and len(stripped) == 2:
                available.add(datetime.date(year, month, int(stripped)))

    return available


def download_recent_chirps_gefs(
    date: datetime.date | None = None,
    max_issue_dates: int | None = DEFAULT_MAX_ISSUE_DATES,
) -> None:
    """Download recent CHIRPS-GEFS data up to the specified date.

    An issue date counts as downloaded only when all of its lead-time files
    are in blob storage. Incomplete dates are topped up with the missing
    files. The newest missing issue dates are downloaded first.

    Args:
        date: The end date for the download range. Defaults to today.
        max_issue_dates: Maximum number of issue dates to download in this
            run, or None for no limit.

    Raises:
        RuntimeError: If the server denies access to a file or directory.
        azure.core.exceptions.AzureError: If an upload to blob storage fails.
    """
    if date is None:
        date = datetime.date.today()

    adm0 = codab.load_codab_from_blob(admin_level=0)
    total_bounds = adm0.total_bounds

    issue_date_range = pd.date_range(
        start=CHIRPS_GEFS_START_DATE,
        end=date,
        freq="D",
    )

    existing_files = stratus.list_container_blobs(
        name_starts_with=f"{constants.PROJECT_PREFIX}/"
        f"{CHIRPS_GEFS_BLOB_DIR}/{CHIRPS_GEFS_BLOB_PREFIX}"
    )
    existing_names = {f.rsplit("/", 1)[-1] for f in existing_files}
    files_per_issue_date = Counter(
        pd.Timestamp(name.split("issued-")[1].split("_valid-")[0])
        for name in existing_names
    )
    logger.info(f"Found {len(existing_names)} existing CHIRPS-GEFS files")
    download_dates = [
        d
        for d in issue_date_range
        if files_per_issue_date[d] < constants.chirps_gefs_lead_time
    ]

    available_on_server = _fetch_available_issue_dates(download_dates)
    download_dates = [
        d for d in download_dates if d.date() in available_on_server
    ][::-1]
    n_available = len(download_dates)
    if max_issue_dates is not None:
        download_dates = download_dates[:max_issue_dates]
    logger.info(
        f"Downloading {len(download_dates)} of {n_available} incomplete "
        f"issue dates available on the server, newest first: "
        f"{[str(x.date()) for x in download_dates]}"
    )

    for i, issue_date in enumerate(
        tqdm(
            download_dates,
            disable=not sys.stdout.isatty(),
        ),
        start=1,
    ):
        logger.info(
            f"Issue date {issue_date.date()} ({i}/{len(download_dates)})"
        )
        for leadtime in range(constants.chirps_gefs_lead_time):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            if _blob_filename(issue_date, valid_date) in existing_names:
                continue
            time.sleep(random.uniform(2, 6))
            download_chirps_gefs(
                issue_date,
                valid_date,
                total_bounds,
            )


def download_chirps_gefs(
    issue_date: pd.Timestamp,
    valid_date: pd.Timestamp,
    total_bounds,
) -> None:
    """Download one CHIRPS-GEFS raster, clip it to the AOI and upload it.

    Args:
        issue_date: Forecast issue date.
        valid_date: Date the forecast is valid for.
        total_bounds: Bounding box (minx, miny, maxx, maxy) to clip to.

    Raises:
        RuntimeError: If the server returns 403.
        azure.core.exceptions.AzureError: If the upload fails.
    """
    url = _source_for(issue_date.date()).file_url.format(
        iss_year=int(issue_date.year),
        iss_month=int(issue_date.month),
        iss_day=int(issue_date.day),
        valid_year=int(valid_date.year),
        valid_month=int(valid_date.month),
        valid_day=int(valid_date.day),
    )
    output_filename = _blob_filename(issue_date, valid_date)
    blob_name = (
        f"{constants.PROJECT_PREFIX}/{CHIRPS_GEFS_BLOB_DIR}/{output_filename}"
    )

    start_time = time.monotonic()
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            raise RuntimeError(
                f"Access denied (403) for CHIRPS GEFS data: "
                f"issue={issue_date.date()} valid={valid_date.date()}. "
                f"URL may require authentication or data is restricted."
            ) from e
        logger.warning(
            f"Failed to download CHIRPS GEFS data for "
            f"{issue_date.date()} valid {valid_date.date()}: {e}"
        )
        return
    except requests.RequestException as e:
        logger.warning(
            f"Failed to download CHIRPS GEFS data for "
            f"{issue_date.date()} valid {valid_date.date()}: {e}"
        )
        return
    logger.info(
        f"Downloaded {output_filename} ({len(response.content)} bytes) "
        f"in {time.monotonic() - start_time:.1f}s"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        output_tmp = Path(tmp_dir) / output_filename
        with rxr.open_rasterio(BytesIO(response.content)) as da:
            da.rio.clip_box(*total_bounds).rio.to_raster(
                output_tmp, driver="COG"
            )
        with open(output_tmp, "rb") as f:
            stratus.upload_blob_data(data=f, blob_name=blob_name)
    logger.info(
        f"Uploaded {output_filename} "
        f"(total {time.monotonic() - start_time:.1f}s)"
    )


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
    adm1 = adm1[adm1["ADM1_EN"].isin(constants.ADM1_LIST)]
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

