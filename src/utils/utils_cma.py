"""
Processing functions for CMA BABJ tropical cyclone forecast files.

Supports two formats:
  - Diamond-7 (.dat): space-delimited rows with year/month/day/hour/fhr/lon/lat/wind/pres/...
  - WMO WTPQ bulletin (.TXT): subjective forecast bulletins with human-readable position lines
"""

import re

import ocha_stratus as stratus
import pandas as pd
from src.utils.logging import get_logger

logger = get_logger(__name__)

_TYPHOON_BLOB_PREFIX = "ds-cma-datasharing/cma_ftp/data_out/typhoon/"

_DIRECTION_TO_DEG: dict[str, float] = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
}

_STORM_INFO_RE = re.compile(
    r"(?:TD|TS|TY|STY|STS|TC)\s+(\w+)\s+(\w+)\s+.*"
    r"INITIAL TIME\s+(\d{2})(\d{2})\d{2}\s+UTC",
    re.IGNORECASE,
)
_FORECAST_ROW_RE = re.compile(
    r"(?:P\+)?(\d+)HR\s+([\d.]+)([NS])\s+([\d.]+)([EW])\s+(\d+)HPA\s+([\d.]+)M/S",
    re.IGNORECASE,
)
_MOTION_RE = re.compile(r"MOVE\s+([A-Z]+)\s+([\d.]+)KM/H", re.IGNORECASE)


def _infer_bulletin_datetime(day: int, hour: int) -> pd.Timestamp:
    """Infer full UTC datetime from bulletin day/hour, assuming recent data.

    Uses the current UTC month/year, rolling back one month if the resulting
    date is more than two days in the future (handles month-boundary files).

    Args:
        day: Day-of-month from the bulletin header.
        hour: Hour (UTC) from the bulletin header.

    Returns:
        Timezone-naive UTC Timestamp.
    """
    now = pd.Timestamp.now(tz="UTC").replace(tzinfo=None)
    try:
        dt = pd.Timestamp(year=now.year, month=now.month, day=day, hour=hour)
    except ValueError:
        dt = None
    if dt is None or dt > now + pd.Timedelta(days=2):
        prev = now.replace(day=1) - pd.Timedelta(days=1)
        dt = pd.Timestamp(year=prev.year, month=prev.month, day=day, hour=hour)
    return dt


def _parse_wmo_wtpq_lines(lines: list[str]) -> tuple[str, str, pd.DataFrame]:
    """Parse a WMO WTPQ subjective tropical cyclone forecast bulletin.

    Expected format::

        WTPQ20 BABJ DDHHMM [suffix]
        SUBJECTIVE FORECAST
        TD/TS/TY STORMNAME STORMID (...) INITIAL TIME DDHHMM UTC
        00HR {lat}N {lon}E {pres}HPA {wind}M/S
        MOVE {dir} {speed}KM/H
        P+{fhr}HR {lat}N {lon}E {pres}HPA {wind}M/S[=]

    Args:
        lines: File content split into lines.

    Returns:
        Tuple of (storm_name, storm_id, DataFrame with one row per forecast step).
        Returns empty strings and an empty DataFrame if the bulletin cannot be parsed.
    """
    storm_name = storm_id = None
    analysis_datetime = None

    for line in lines:
        m = _STORM_INFO_RE.search(line)
        if m:
            storm_name = m.group(1)
            storm_id = m.group(2)
            analysis_datetime = _infer_bulletin_datetime(
                day=int(m.group(3)), hour=int(m.group(4))
            )
            break

    if storm_name is None or analysis_datetime is None:
        return "", "", pd.DataFrame()

    motion_dir = float("nan")
    motion_spd = float("nan")
    for line in lines:
        m = _MOTION_RE.search(line)
        if m:
            motion_dir = _DIRECTION_TO_DEG.get(m.group(1).upper(), float("nan"))
            motion_spd = float(m.group(2))
            break

    rows = []
    for line in lines:
        m = _FORECAST_ROW_RE.search(line)
        if not m:
            continue
        fhr = int(m.group(1))
        lat = float(m.group(2)) * (1.0 if m.group(3).upper() == "N" else -1.0)
        lon = float(m.group(4)) * (1.0 if m.group(5).upper() == "E" else -1.0)
        rows.append(
            {
                "storm_id": storm_id,
                "storm_name": storm_name,
                "analysis_datetime": analysis_datetime,
                "forecast_hour": fhr,
                "valid_datetime": analysis_datetime + pd.Timedelta(hours=fhr),
                "lon": lon,
                "lat": lat,
                "wind_speed_ms": float(m.group(7)),
                "pressure_hpa": float(m.group(6)),
                "radius_gale_km": float("nan"),
                "radius_storm_km": float("nan"),
                "motion_direction_deg": motion_dir if fhr == 0 else float("nan"),
                "motion_speed_kmh": motion_spd if fhr == 0 else float("nan"),
            }
        )

    return storm_name, storm_id, pd.DataFrame(rows)


def _parse_diamond7_lines(lines: list[str]) -> tuple[str, str, pd.DataFrame]:
    """Parse diamond-7 formatted lines into storm metadata and a DataFrame.

    Expects the standard two-line header followed by space-delimited data rows:
      Line 0: "diamond 7 YYYY..."  (informational, may contain garbled chars)
      Line 1: "STORMNAME STORMID ..."
      Lines 2+: data rows with at least 13 space-delimited columns

    Args:
        lines: File content split into lines.

    Returns:
        Tuple of (storm_name, storm_id, DataFrame of parsed rows).

    Raises:
        IndexError: If the file has fewer than two header lines.
        ValueError: If the storm metadata line cannot be parsed.
    """
    meta = lines[1].split()
    storm_name, storm_id = meta[0], meta[1]

    rows = []
    for line in lines[2:]:
        parts = line.split()
        if len(parts) < 13:
            continue
        try:
            year, month, day, hour, fhr = (int(x) for x in parts[:5])
            lon, lat = float(parts[5]), float(parts[6])
            wind_ms = float(parts[7])
            pres_raw = float(parts[8])
            rad_gale = float(parts[9])
            rad_storm = float(parts[10])
            motion_dir = float(parts[11])
            motion_spd = float(parts[12])
        except ValueError:
            continue

        analysis_dt = pd.Timestamp(year=year, month=month, day=day, hour=hour)
        valid_dt = analysis_dt + pd.Timedelta(hours=fhr)

        rows.append(
            {
                "storm_id": storm_id,
                "storm_name": storm_name,
                "analysis_datetime": analysis_dt,
                "forecast_hour": fhr,
                "valid_datetime": valid_dt,
                "lon": lon,
                "lat": lat,
                "wind_speed_ms": wind_ms,
                "pressure_hpa": pres_raw if pres_raw != 0 else float("nan"),
                "radius_gale_km": rad_gale if rad_gale != 0 else float("nan"),
                "radius_storm_km": (
                    rad_storm if rad_storm != 0 else float("nan")
                ),
                "motion_direction_deg": (
                    motion_dir
                    if (fhr == 0 and motion_dir != 0)
                    else float("nan")
                ),
                "motion_speed_kmh": (
                    motion_spd
                    if (fhr == 0 and motion_spd != 0)
                    else float("nan")
                ),
            }
        )

    return storm_name, storm_id, pd.DataFrame(rows)


def parse_dat_file(blob_name: str, content: bytes) -> pd.DataFrame:
    """Parse a single CMA BABJ diamond-7 .dat file into a tidy DataFrame.

    Args:
        blob_name: Blob path (used for logging only).
        content: Raw file bytes.

    Returns:
        DataFrame with one row per forecast step.
    """
    lines = content.decode("utf-8", errors="replace").splitlines()
    _, _, df = _parse_diamond7_lines(lines)
    return df


def parse_txt_file(content: bytes) -> pd.DataFrame | None:
    """Parse a CMA FTP .TXT bulletin file into a tidy DataFrame.

    Dispatches to the appropriate parser based on the WMO bulletin type header:
      - ``WT...`` bulletins: parsed as WTPQ subjective forecasts.
      - Other types (e.g. ``WS...`` coded bulletins): not supported; returns None.

    Args:
        content: Raw file bytes.

    Returns:
        DataFrame with one row per forecast step, or None if the bulletin
        type is not supported.
    """
    lines = content.decode("utf-8", errors="replace").splitlines()
    for line in lines:
        if re.search(r"\bWT\w+\b", line):
            _, _, df = _parse_wmo_wtpq_lines(lines)
            return df
    return None


def load_bob_tc_forecasts(blob_prefix: str) -> pd.DataFrame:
    """List and parse all .dat blobs under blob_prefix into one DataFrame.

    Args:
        blob_prefix: Blob path prefix to search for .dat files.

    Returns:
        Concatenated DataFrame of all parsed forecast files.
    """
    logger.info(f"Listing blobs under {blob_prefix} ...")
    all_blobs = stratus.list_container_blobs(blob_prefix)
    dat_blobs = [b for b in all_blobs if b.endswith(".dat")]
    logger.info(f"Found {len(dat_blobs)} .dat files")

    frames = []
    for blob_name in sorted(dat_blobs):
        logger.info(f"  Parsing {blob_name}")
        content = stratus.load_blob_data(blob_name)
        frames.append(parse_dat_file(blob_name, content))

    return pd.concat(frames, ignore_index=True)


def load_typhoon_tc_forecasts(
    blob_prefix: str = _TYPHOON_BLOB_PREFIX,
) -> pd.DataFrame:
    """List and parse all .txt blobs under blob_prefix into one DataFrame.

    Reads diamond-7 formatted .txt files from the CMA FTP typhoon output
    path and returns a single concatenated DataFrame.

    Args:
        blob_prefix: Blob path prefix to search for .txt files. Defaults to
            the standard CMA FTP typhoon output path.

    Returns:
        Concatenated DataFrame of all parsed forecast files.

    Raises:
        ValueError: If no .txt files are found under blob_prefix.
    """
    logger.info(f"Listing blobs under {blob_prefix} ...")
    all_blobs = stratus.list_container_blobs(blob_prefix)
    txt_blobs = [b for b in all_blobs if b.endswith(".TXT")]
    logger.info(f"Found {len(txt_blobs)} .txt files")

    if not txt_blobs:
        raise ValueError(f"No .txt files found under blob prefix: {blob_prefix}")

    frames = []
    for blob_name in sorted(txt_blobs):
        logger.info(f"  Parsing {blob_name}")
        content = stratus.load_blob_data(blob_name)
        df = parse_txt_file(content)
        if df is None:
            logger.warning(f"  Skipping unrecognized bulletin format: {blob_name}")
        elif not df.empty:
            frames.append(df)

    if not frames:
        logger.warning("No parseable WTPQ bulletins found under %s", blob_prefix)
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
