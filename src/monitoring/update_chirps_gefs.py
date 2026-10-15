import argparse
import datetime

from dotenv import load_dotenv

from src.datasources import codab
from src.datasources.chirps_gefs import (
    DEFAULT_MAX_ISSUE_DATES,
    check_chirps_gefs_trigger,
    download_recent_chirps_gefs,
    mean_daily_by_area,
    process_recent_chirps_gefs,
)
from src.utils.utils_plot import plot_chirps_gefs_forecast
from src.utils.logging import get_logger

load_dotenv()
logger = get_logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update CHIRPS-GEFS data pipeline."
    )
    parser.add_argument(
        "--date",
        type=datetime.date.fromisoformat,
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--max-issue-dates",
        type=int,
        default=0,
        help=(
            "Maximum number of issue dates to download in this run, newest "
            "first. Use 0 for no limit (e.g. for a backfill)."
        ),
    )
    args = parser.parse_args()

    logger.info("Starting the CHIRPS-GEFS update pipeline...")

    logger.info("Downloading recent CHIRPS-GEFS data...")
    download_recent_chirps_gefs(
        date=args.date, max_issue_dates=args.max_issue_dates or None
    )

    logger.info("Process recent CHIRPS-GEFS data...")
    df = process_recent_chirps_gefs(date=args.date)

    logger.info("Check CHIRPS-GEFS data against threshold")
    check_chirps_gefs_trigger(df=df)

    logger.info("Save CHIRPS-GEFS plot")
    plot_chirps_gefs_forecast(
        mean_daily_by_area(
            df["issue_date"].max(), codab.load_aa_target_and_rest_areas()
        )
    )


