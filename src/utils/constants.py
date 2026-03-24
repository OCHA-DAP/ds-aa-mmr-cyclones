import os

import numpy as np
import os
import pytz
from datetime import datetime, timezone

PROJECT_PREFIX = "ds-aa-mmr-cyclones"
ISO3 = "mmr"
adm_level = 1

adm_column = f"ADM{adm_level}_EN"
adm_pcode = f"ADM{adm_level}_PCODE"

buffer_km = 1000
windspeed_alert_level = 47
rainfall_alert_level = 200
chirps_gefs_lead_time = 16
# Monitoring start date - only process data from this date forward
# Set to Myanmar timezone so that dummy emails show intended date
mmr_tz = pytz.timezone("Asia/Yangon")
MONITORING_START_DATE = mmr_tz.localize(datetime(2026, 3, 1)).astimezone(
    timezone.utc
)


# Runtime control flags - centralized configuration
def _parse_bool_env(env_var: str, default: bool = False) -> bool:
    """Parse environment variable as boolean with proper defaults."""
    value = os.getenv(env_var)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


# Saffir-Simpson scale (knots)
TS = 34
CAT1 = 64
CAT2 = 83
CAT3 = 96
CAT4 = 113
CAT5 = 137

CAT_LIMITS = [
    (TS, "Trop. Storm"),
    (CAT1, "Cat. 1"),
    (CAT2, "Cat. 2"),
    (CAT3, "Cat. 3"),
    (CAT4, "Cat. 4"),
    (CAT5, "Cat. 5"),
]


MIN_EMAIL_DISTANCE = 1000

NUMERIC_NAME_REGEX = r"\b(?:One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|Eleven|Twelve|Thirteen|Fourteen|Fifteen|Sixteen|Seventeen|Eighteen|Nineteen|Twenty)\b"  # noqa: E501


# Environment variables for data directories
AA_DATA_DIR = os.getenv("AA_DATA_DIR")
AA_DATA_DIR_NEW = os.getenv("AA_DATA_DIR_NEW")

# CRS
MMR_UTM = 32647
ADM_LIST = ["Rakhine"]
