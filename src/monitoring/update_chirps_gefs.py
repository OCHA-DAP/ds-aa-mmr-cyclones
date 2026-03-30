from dotenv import load_dotenv
from src.datasources.chirps_gefs import (
    download_recent_chirps_gefs, process_recent_chirps_gefs, check_chirps_gefs_trigger
)
from src.utils.logging import get_logger
load_dotenv()
logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the CHIRPS-GEFS update pipeline...")

    logger.info("Downloading recent CHIRPS-GEFS data...")
    download_recent_chirps_gefs()

    logger.info("Process recent CHIRPS-GEFS data...")
    df=process_recent_chirps_gefs()

    logger.info("Check CHIRPS-GEFS data against threshold")
    check_chirps_gefs_trigger(df=df)


