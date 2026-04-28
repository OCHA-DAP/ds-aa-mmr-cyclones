import pandas as pd
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

def check_wind_speed_trigger_data():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/wind")
    if len(xx) == 0:
        logger.info(f"Wind speed threshold not reached {constants.PROJECT_PREFIX}")
        return pd.DataFrame(None)

    else:
        logger.info(f"Wind speed threshold reached for {constants.PROJECT_PREFIX}")
        xx.sort()
        df = stratus.load_csv_from_blob(blob_name=xx[-1])
        return df

def check_rainfall_data():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/rainfall")
    if len(xx) == 0:
        logger.info(f"Rainfall threshold not reached {constants.PROJECT_PREFIX}")
        return pd.DataFrame(None)

    else:
        logger.info(f"Rainfall threshold reached for {constants.PROJECT_PREFIX}")
        xx.sort()
        df = stratus.load_csv_from_blob(blob_name=xx[-1])
        return df

def check_cyclone_presence():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/monitoring")
    if len(xx) != 0:
        xx.sort()
        suffix = xx[-1].split("_")[1]
        if suffix == datetime.date.today().strftime("%Y-%m-%d"):
            df = stratus.load_csv_from_blob(blob_name=xx[-1])
            return df
        else :
            logger.info(f"No storms in the area of interest.")
            return pd.DataFrame(None)
    else:
        logger.info(f"No storms in the area of interest.")
        return pd.DataFrame(None)

def get_latest_storm_track_plot() -> bytes | None:
    """Return today's storm track plot image bytes, or None if unavailable.

    Returns:
        Raw PNG bytes if a plot exists for today, otherwise None.
    """
    xx = stratus.list_container_blobs(
        name_starts_with=f"{constants.PROJECT_PREFIX}/processed/storm_track_plot"
    )
    if not xx:
        return None
    xx.sort()
    # Pattern: projects/ds-aa-mmr-cyclones/processed/storm_track_plot_YYYY-MM-DD_HH.png
    filename = xx[-1].split("/")[-1]
    date_part = filename.split("_")[3]
    if date_part == datetime.date.today().strftime("%Y-%m-%d"):
        return stratus.load_blob_data(blob_name=xx[-1])
    return None


myanmar_time=datetime.datetime.now(ZoneInfo("Asia/Yangon")).strftime("%Hh00 %d %b. %Y")
df_wind_speed = check_wind_speed_trigger_data()
df_rainfall = check_rainfall_data()
df_cyclone = check_cyclone_presence()
plot_bytes = get_latest_storm_track_plot()
if df_wind_speed.empty and df_rainfall.empty:
    threshold_info = {"wind_speed_threshold_reached":"NOT REACHED", "rainfall_threshold_reached":"NOT REACHED"}
    logger.info("No thresholds were met, checking for existence of Cyclones to be monitored")
    if not df_cyclone.empty:
        storm_name = df_cyclone.storm_name.unique()[0]
        campaign_body = generate_body_email(storm_name=storm_name, date_myanmar=myanmar_time, info=threshold_info, plot_bytes=plot_bytes)
        campaign_id = create_campaign(name="MMR_monitoring_email", body=campaign_body,
                                      subject=f"Anticipatory Action Myanmar - {myanmar_time}")
        send_campaign(campaign_id=campaign_id)
        logger.info("Monitoring email sent successfully!")
else:
    storm_name =  df_wind_speed.storm_name.unique()
    threshold_info = {"wind_speed_threshold_reached": "REACHED" if  not df_wind_speed.empty else "NOT REACHED", "rainfall_threshold_reached": "REACHED" if  not df_rainfall.empty else "NOT REACHED"}
    campaign_body = generate_body_email(storm_name=storm_name, date_myanmar=myanmar_time, info=threshold_info, plot_bytes=plot_bytes)
    campaign_id = create_campaign(name="MMR_trigger_email", body=campaign_body, subject=f"Anticipatory Action Myanmar - {myanmar_time}")
    send_campaign(campaign_id=campaign_id)
    logger.info("Trigger email sent successfully!")


