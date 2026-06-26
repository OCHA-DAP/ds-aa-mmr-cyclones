import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

import ocha_stratus as stratus
from src.utils import constants
from src.utils.listmonk import create_campaign, generate_body_email, send_campaign
from src.utils.logging import get_logger

load_dotenv()
logger = get_logger(__name__)

def check_wind_speed_trigger_data():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/wind")
    if len(xx) == 0:
        logger.info(f"Wind speed threshold not reached {constants.PROJECT_PREFIX}")
        return pd.DataFrame(None)

    else:
        logger.info(f"Wind speed threshold reached for {constants.PROJECT_PREFIX}")
        xx.sort()
        df = stratus.load_csv_from_blob(blob_name=xx[-1])
        has_valid_sid = (~df["sid"].astype(str).str.match(r"^\d{2}B$", na=False)).any()
        if has_valid_sid:
            return df
        else:
            return pd.DataFrame(None)

def check_rainfall_data():
    xx = stratus.list_container_blobs(name_starts_with=f"{constants.PROJECT_PREFIX}/processed/rainfall_exceedance")
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

def determine_trigger_phase(
    df_cyclone: pd.DataFrame,
    df_rainfall: pd.DataFrame | None = None,
) -> str | None:
    """Determine the anticipatory action trigger phase from forecast lead time.

    Lead time is computed as the median, across ensemble members, of each
    member's expected closest-approach time to Myanmar. If monitoring data is
    unavailable, rainfall valid_date is used as a fallback.

    Args:
        df_cyclone: Monitoring DataFrame with 'time', 'min_dist_km', and
            'ensemble_number' columns.
        df_rainfall: Rainfall exceedance DataFrame with 'valid_date' and
            'issue_date' columns, used as fallback when df_cyclone is empty.

    Returns:
        'readiness' (72–120 h), 'action' (48–72 h), 'observational' (<48 h),
        or None when lead time exceeds 120 h or cannot be determined.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    if not df_cyclone.empty:
        times = pd.to_datetime(df_cyclone["time"], utc=True)
        df_cyclone = df_cyclone.copy()
        df_cyclone["time"] = times
        approach_times = (
            df_cyclone.loc[df_cyclone.groupby("ensemble_number")["min_dist_km"].idxmin(), "time"]
        )
        median_approach = approach_times.median()
        lead_hours = (median_approach - now).total_seconds() / 3600
    elif df_rainfall is not None and not df_rainfall.empty:
        min_valid = pd.to_datetime(df_rainfall["valid_date"]).min()
        issue = pd.to_datetime(df_rainfall["issue_date"]).iloc[0]
        lead_hours = (min_valid - issue).total_seconds() / 3600
    else:
        return "observational"

    if lead_hours > 120:
        return None
    if lead_hours >= 72:
        return "readiness"
    if lead_hours >= 48:
        return "action"
    return "observational"


def get_latest_monitoring_plot(plot_type:str = "storm_track") -> bytes | None:
    """Return today's plot image bytes, or None if unavailable.

    Returns:
        Raw PNG bytes if a plot exists for today, otherwise None.
    """
    xx = stratus.list_container_blobs(
        name_starts_with=f"{constants.PROJECT_PREFIX}/processed/{plot_type}_plot"
    )
    if not xx:
        return None
    xx.sort()
    # Pattern: projects/ds-aa-mmr-cyclones/processed/storm_track_plot/storm_track_plot_YYYY-MM-DD_HH.png
    # Pattern: projects/ds-aa-mmr-cyclones/processed/rainfall_forecast_plot/rainfall_forecast_plot_YYYY-MM-DD_HH.png
    filename = xx[-1].split("/")[-1]
    date_part = filename.split("_")[3]
    if date_part == datetime.date.today().strftime("%Y-%m-%d"):
        return stratus.load_blob_data(blob_name=xx[-1])
    return None


myanmar_time = datetime.datetime.now(ZoneInfo("Asia/Yangon")).strftime("%Hh00 %d %b. %Y")
df_wind_speed = check_wind_speed_trigger_data()
df_rainfall = check_rainfall_data()
df_cyclone = check_cyclone_presence()
phase = determine_trigger_phase(df_cyclone, df_rainfall)
plot_bytes_storm_track = get_latest_monitoring_plot(plot_type="storm_track")
plot_bytes_rainfall = get_latest_monitoring_plot(plot_type="rainfall_forecast")
plot_bytes = [plot_bytes_storm_track, plot_bytes_rainfall]

if df_wind_speed.empty and df_rainfall.empty:
    threshold_info = {
        "wind_speed_threshold_reached": "NOT REACHED",
        "rainfall_threshold_reached": "NOT REACHED",
    }
    logger.info("No thresholds were met, checking for existence of Cyclones to be monitored")
    if not df_cyclone.empty:
        storm_name = df_cyclone.storm_name.unique()[0]
        campaign_body = generate_body_email(
            storm_name=storm_name,
            date_myanmar=myanmar_time,
            info=threshold_info,
            plot_bytes=plot_bytes,
            phase=phase,
        )
        campaign_id = create_campaign(
            name="MMR_monitoring_email",
            body=campaign_body,
            subject=f"Anticipatory Action Myanmar - {myanmar_time}",
        )
        send_campaign(campaign_id=campaign_id)
        logger.info("Monitoring email sent successfully!")
else:
    storm_name = (
        df_wind_speed.sid.unique()[0]
        if not df_wind_speed.empty
        else df_cyclone.storm_name.unique()[0]
    )
    threshold_info = {
        "wind_speed_threshold_reached": "REACHED" if not df_wind_speed.empty else "NOT REACHED",
        "rainfall_threshold_reached": "REACHED" if not df_rainfall.empty else "NOT REACHED",
    }
    campaign_body = generate_body_email(
        storm_name=storm_name,
        date_myanmar=myanmar_time,
        info=threshold_info,
        plot_bytes=plot_bytes,
        phase=phase,
    )
    campaign_name = f"MMR_{phase}_email" if phase else "MMR_trigger_email"
    campaign_id = create_campaign(
        name=campaign_name,
        body=campaign_body,
        subject=f"Anticipatory Action Myanmar - {myanmar_time}",
    )
    send_campaign(campaign_id=campaign_id)
    logger.info(f"Trigger email sent successfully (phase: {phase})!")



