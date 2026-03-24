from dotenv import load_dotenv

import sys
import ocha_stratus as stratus
from src.utils.logging import get_logger
from src.utils import constants
load_dotenv()
from src.utils.listmonk import *
logger = get_logger(__name__)
import datetime
from zoneinfo import ZoneInfo

def check_windspeed_data():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/wind")
    if len(xx) == 0:
        logger.info(f"Windspeed threshold not reached {constants.PROJECT_PREFIX}")

    else:
        logger.info(f"Windspeed threshold reached for {constants.PROJECT_PREFIX}")
        xx.sort()
        df = stratus.load_csv_from_blob(blob_name=xx[-1])
        return df

def check_rainfall_data():
    return None

df_windspeed = check_windspeed_data()
df_rainfall = check_rainfall_data()
if df_windspeed is None and df_rainfall is None:
    logger.info("Stopping execution as no thresholds were met")
    sys.exit(0)
myanmar_time=datetime.datetime.now(ZoneInfo("Asia/Yangon")).strftime("%Hh00 %d %b. %Y")
campaign_body = generate_body_email(storm_name="STORM_NAME_TEST", date_myanmar=myanmar_time)
campaign_id = create_campaign(name="MMR_trigger_email", body=campaign_body, subject=f"Anticipatory Action Myanmar - {myanmar_time}")
send_campaign(campaign_id=campaign_id)
logger.info("Email sent successfully!")


