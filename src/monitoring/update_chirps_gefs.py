import argparse
import datetime

from dotenv import load_dotenv

from src.datasources.chirps_gefs import (
    check_chirps_gefs_trigger,
    download_recent_chirps_gefs,
    process_recent_chirps_gefs,
)
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
    args = parser.parse_args()

    logger.info("Starting the CHIRPS-GEFS update pipeline...")

    logger.info("Downloading recent CHIRPS-GEFS data...")
    download_recent_chirps_gefs(date=args.date)

    logger.info("Process recent CHIRPS-GEFS data...")
    df = process_recent_chirps_gefs(date=args.date)

    logger.info("Check CHIRPS-GEFS data against threshold")
    check_chirps_gefs_trigger(df=df, date=args.date)
