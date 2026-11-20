import datetime
import re
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

import ocha_stratus as stratus
from src.utils import constants
from src.utils.listmonk import create_campaign, generate_body_email, send_campaign
from src.utils.logging import get_logger
from src.utils.storm_names import drop_unnamed_storms

load_dotenv()
logger = get_logger(__name__)

def _todays_blobs(prefix: str) -> list[str]:
    """Return blobs under processed/{prefix} whose name contains today's date.

    Blobs are never deleted, so listing without a date filter would pick up
    stale data from a past run (or leftover test fixtures) instead of
    today's. Sorted so the caller can take the last entry as a tie-break
    when more than one blob matches today (e.g. two runs the same day).
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    blobs = stratus.list_container_blobs(
        name_starts_with=f"{constants.PROJECT_PREFIX}/processed/{prefix}"
    )
    return sorted(b for b in blobs if today in b)


WIND_FILE_REGEX = re.compile(r"wind_exceedance_(adm3|rest)_.*_(\w+)\.csv$")
WIND_TOWNSHIP_REGEX = re.compile(r"wind_township_.*_(\w+)\.csv$")
AREA_LABELS = constants.AREA_LABELS


def check_wind_speed_trigger_data() -> dict[tuple[str, str], pd.DataFrame]:
    """Load today's latest wind exceedance data per area and source.

    Returns:
        Mapping of (area, source) to the exceedance DataFrame (named storms
        only), e.g. ``("rest", "ecmwf")``. Empty when no file exists today.
    """
    latest: dict[tuple[str, str], str] = {}
    for blob in _todays_blobs("wind_exceedance_"):
        match = WIND_FILE_REGEX.search(blob)
        if match:
            latest[match.groups()] = blob  # sorted: the last blob is latest
    return {
        key: drop_unnamed_storms(stratus.load_csv_from_blob(blob_name=blob), "sid")
        for key, blob in latest.items()
    }


def check_wind_township_data() -> pd.DataFrame:
    """Load today's latest worst wind speed by township, for each source.

    Returns:
        DataFrame with the 'township', 'group', 'wind_speed_at_land' and
        'source' columns. Empty when no file exists today.
    """
    latest = {
        match.group(1): blob
        for blob in _todays_blobs("wind_township_")
        if (match := WIND_TOWNSHIP_REGEX.search(blob))
    }  # sorted: the last blob of a source is the latest
    if not latest:
        return pd.DataFrame(columns=["township", "group", "wind_speed_at_land", "source"])
    return pd.concat(
        [
            stratus.load_csv_from_blob(blob_name=blob).assign(source=source)
            for source, blob in latest.items()
        ],
        ignore_index=True,
    )


def check_rainfall_township_data() -> pd.DataFrame:
    """Load today's worst 3-day rainfall forecast by area and township.

    Returns:
        DataFrame with the 'area', 'max_rolling_sum_3', 'group' and
        'is_township' columns. Empty when no file exists today.
    """
    blobs = _todays_blobs("rainfall_township_")
    if not blobs:
        return pd.DataFrame(
            columns=["area", "max_rolling_sum_3", "group", "is_township"]
        )
    return stratus.load_csv_from_blob(blob_name=blobs[-1])


def _format_worst(value: float, unit: str) -> str:
    """Format a worst forecast value with its unit ("n/a" when missing)."""
    return "n/a" if pd.isna(value) else f"{value:.0f} {unit}"


def build_threshold_table(
    wind_data: dict[tuple[str, str], pd.DataFrame],
    df_rainfall: pd.DataFrame,
    df_wind_township: pd.DataFrame,
    df_rainfall_township: pd.DataFrame,
) -> dict[str, dict[str, str]]:
    """Build the AA Target area threshold table: one row per component.

    Args:
        wind_data: Output of ``check_wind_speed_trigger_data``.
        df_rainfall: AA Target area rainfall exceedance DataFrame (empty if
            not reached).
        df_wind_township: Output of ``check_wind_township_data``.
        df_rainfall_township: Output of ``check_rainfall_township_data``.

    Returns:
        Mapping of row label to {"Threshold", "Worst forecast", "Status"},
        where the status is "REACHED" / "NOT REACHED".
    """
    target = AREA_LABELS["adm3"]
    sources = sorted(
        {source for _, source in wind_data}
        | set(df_wind_township["source"])
        | {"ecmwf", "cma"}
    )
    wind_target = df_wind_township[df_wind_township["group"] == target]
    table = {
        f"Wind speed ({source.upper()})": {
            "Threshold": f"{constants.wind_speed_alert_level} kt",
            "Worst forecast": _format_worst(
                wind_target.loc[
                    wind_target["source"] == source, "wind_speed_at_land"
                ].max(),
                "kt",
            ),
            "Status": (
                "REACHED"
                if not wind_data.get(("adm3", source), pd.DataFrame()).empty
                else "NOT REACHED"
            ),
        }
        for source in sources
    }
    table["Rainfall"] = {
        "Threshold": f"{constants.rainfall_alert_level_forecast} mm (3-day sum)",
        "Worst forecast": _format_worst(
            df_rainfall_township.loc[
                df_rainfall_township["area"] == target, "max_rolling_sum_3"
            ].max(),
            "mm",
        ),
        "Status": "REACHED" if not df_rainfall.empty else "NOT REACHED",
    }
    return table


def build_townships_reached(
    df_wind_township: pd.DataFrame, df_rainfall_township: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """List the townships reaching a threshold, by area.

    Args:
        df_wind_township: Output of ``check_wind_township_data``.
        df_rainfall_township: Output of ``check_rainfall_township_data``.

    Returns:
        Mapping of area label to a DataFrame with the 'township', 'wind'
        (worst across sources, knots) and 'rainfall' (mm, 3-day sum) columns,
        restricted to the townships reaching the wind or rainfall threshold.
    """
    keys = ["township", "group"]
    wind = (
        df_wind_township.groupby(keys, as_index=False)["wind_speed_at_land"]
        .max()
        .rename(columns={"wind_speed_at_land": "wind"})
    )
    rainfall = (
        df_rainfall_township[df_rainfall_township["is_township"].astype(bool)]
        .rename(columns={"area": "township", "max_rolling_sum_3": "rainfall"})
        [[*keys, "rainfall"]]
    )
    summary = wind.merge(rainfall, on=keys, how="outer")
    reached = summary[
        (summary["wind"] >= constants.wind_speed_alert_level)
        | (summary["rainfall"] >= constants.rainfall_alert_level_forecast)
    ]
    return {
        label: reached[reached["group"] == label].sort_values("township")
        for label in AREA_LABELS.values()
    }


def check_rainfall_data(prefix: str = "rainfall_exceedance") -> pd.DataFrame:
    """Load today's rainfall exceedance data (empty if threshold not reached)."""
    xx = _todays_blobs(prefix)
    if not xx:
        logger.info(f"Rainfall threshold not reached {constants.PROJECT_PREFIX}")
        return pd.DataFrame(None)

    logger.info(f"Rainfall threshold reached for {constants.PROJECT_PREFIX}")
    return stratus.load_csv_from_blob(blob_name=xx[-1])

def check_cyclone_presence():
    xx = _todays_blobs("monitoring")
    if not xx:
        logger.info("No storms in the area of interest.")
        return pd.DataFrame(None)
    df = drop_unnamed_storms(stratus.load_csv_from_blob(blob_name=xx[-1]), "storm_name")
    if df.empty:
        logger.info("Only unnamed storms (e.g. 70B) in the area of interest.")
    return df

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
        'readiness' (72–120 h), 'action' (up to 72 h), 'observational'
        (no upcoming approach to base the action phase on, e.g. already passed),
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
    if lead_hours >= 0:
        return "action"
    return "observational"


def get_latest_monitoring_plot(plot_type:str = "storm_track") -> bytes | None:
    """Return today's plot image bytes, or None if unavailable.

    Returns:
        Raw PNG bytes if a plot exists for today, otherwise None.
    """
    xx = _todays_blobs(f"{plot_type}_plot")
    if not xx:
        return None
    return stratus.load_blob_data(blob_name=xx[-1])


myanmar_time = datetime.datetime.now(ZoneInfo("Asia/Yangon")).strftime("%Hh00 %d %b. %Y")
wind_data = check_wind_speed_trigger_data()
df_wind_speed = pd.concat(wind_data.values(), ignore_index=True) if wind_data else pd.DataFrame()
df_rainfall_target = check_rainfall_data()
df_rainfall_rest = check_rainfall_data("rest_rainfall_exceedance")
df_wind_township = check_wind_township_data()
df_rainfall_township = check_rainfall_township_data()
threshold_table = build_threshold_table(
    wind_data, df_rainfall_target, df_wind_township, df_rainfall_township
)
townships_reached = build_townships_reached(df_wind_township, df_rainfall_township)
df_rainfall = pd.concat([df_rainfall_target, df_rainfall_rest], ignore_index=True)
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
            phase=None,
            threshold_table=threshold_table,
            townships_reached=townships_reached,
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
        threshold_table=threshold_table,
        townships_reached=townships_reached,
    )
    campaign_name = f"MMR_{phase}_email" if phase else "MMR_trigger_email"
    campaign_id = create_campaign(
        name=campaign_name,
        body=campaign_body,
        subject=f"Anticipatory Action Myanmar - {myanmar_time}",
    )
    send_campaign(campaign_id=campaign_id)
    logger.info(f"Trigger email sent successfully (phase: {phase})!")



