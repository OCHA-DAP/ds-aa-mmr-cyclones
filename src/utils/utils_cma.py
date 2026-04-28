"""
Processing functions for CMA BABJ "diamond 7" tropical cyclone forecast files.

Diamond-7 row columns:
  year month day hour  forecast_hour  lon  lat  wind_ms  pres_hpa
  radius_gale_km  radius_storm_km  motion_dir_deg  motion_speed_kmh
"""

import ocha_stratus as stratus
import pandas as pd
from src.utils.logging import get_logger

logger = get_logger(__name__)

def parse_dat_file(blob_name: str, content: bytes) -> pd.DataFrame:
    """Parse a single CMA BABJ diamond-7 .dat file into a tidy DataFrame."""
    lines = content.decode("utf-8", errors="replace").splitlines()
    # Line 0: "diamond 7 YYYY..."  (may have garbled chars — skip)
    # Line 1: "STORMNAME STORMID ..."
    meta = lines[1].split()
    storm_name = meta[0]
    storm_id = meta[1]

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

    return pd.DataFrame(rows)


def load_bob_tc_forecasts(blob_prefix: str) -> pd.DataFrame:
    """List and parse all .dat blobs under blob_prefix into one DataFrame."""
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