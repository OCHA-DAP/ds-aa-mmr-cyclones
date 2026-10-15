from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True), override=True)

from historical_analysis.process_hres import process_hres_folder
from src.datasources import codab
from src.utils import constants
from src.utils.utils_fun import make_run_metadata, make_suffix
from src.utils.utils_plot import overview_situation_two_areas

HRES_CSV = Path("historical_analysis/MMR_HRES_daily_rain_two_areas.csv")
COLUMN_KEYS = {"AA Target area": "aa_target", "Rest of Rakhine": "rest_rakhine"}
FORECAST_KEYS = ["storm_name", "issued_date", "lead_time_landfall"]


def load_hres_two_areas(areas: dict) -> pd.DataFrame:
    """Load daily ECMWF HRES rainfall per area, processing the GRIBs if needed.

    Args:
        areas: Mapping of area name to a single-row GeoDataFrame with an
            'area' column.

    Returns:
        DataFrame with one row per issue time, valid time and area (column
        'admin'), and the daily rainfall in 'rain_mm'.
    """
    if HRES_CSV.exists():
        return pd.read_csv(HRES_CSV)
    grib_files = [
        blob
        for blob in stratus.list_container_blobs(f"{constants.PROJECT_PREFIX}/raw/hres/")
        if blob.endswith(".grib")
    ]
    df = process_hres_folder(
        grib_files=grib_files,
        admin_file=pd.concat(areas.values(), ignore_index=True),
        admin_level_col="area",
    )
    df.to_csv(HRES_CSV, index=False)
    return df


def max_forecast_rainfall(
    forecast: pd.DataFrame, storm_dates: pd.DataFrame, landfall: pd.DataFrame
) -> pd.DataFrame:
    """Max 3-day forecasted rainfall per storm and forecast issue time.

    Only forecasts issued 1 to 3 days before landfall, and valid from 3 days
    before landfall, are kept.

    Args:
        forecast: Daily HRES rainfall of one area, with 'time' (issue time),
            'valid_time' and 'rain_mm'.
        storm_dates: Rows of 'storm_name' and 'date' of each storm period.
        landfall: Rows of 'storm_name' and 'landfall_adm0_date'.

    Returns:
        DataFrame with FORECAST_KEYS and 'rolling_sum_3', the maximum rolling
        3-day sum.
    """
    forecast = (
        forecast.assign(
            issued_date=pd.to_datetime(forecast["time"]),
            valid_date=pd.to_datetime(forecast["valid_time"]),
        )
        .merge(storm_dates, left_on="valid_date", right_on="date")
        .merge(landfall, on="storm_name")
        .sort_values(["storm_name", "issued_date", "valid_date"])
    )
    forecast["rolling_sum_3"] = forecast.groupby(["storm_name", "issued_date"])[
        "rain_mm"
    ].transform(lambda rain: rain.rolling(3, min_periods=1).sum())
    forecast["lead_time_landfall"] = (
        forecast["landfall_adm0_date"] - forecast["issued_date"]
    ).dt.total_seconds() / 3600
    kept = forecast[
        forecast["issued_date"].between(
            forecast["landfall_adm0_date"] - pd.Timedelta(days=3),
            forecast["landfall_adm0_date"] - pd.Timedelta(days=1),
        )
        & (forecast["valid_date"] >= forecast["landfall_adm0_date"] - pd.Timedelta(days=3))
    ]
    return kept.groupby(FORECAST_KEYS, as_index=False)["rolling_sum_3"].max()


meta = make_run_metadata(level=constants.adm_level, areas=constants.ADM_LIST)
suff = make_suffix(meta=meta)

df_overview = pd.read_csv(
    f"results/df_overview_two_areas_{suff}.csv", parse_dates=["landfall_adm0_date"]
)
landfall = df_overview[["storm_name", "landfall_adm0_date"]].assign(
    landfall_adm0_date=lambda df: df["landfall_adm0_date"].dt.normalize()
)
storm_dates = pd.DataFrame(
    pd.read_pickle("src/data/storms_date.pickle")
).melt(var_name="storm_name", value_name="date")
storm_dates["date"] = pd.to_datetime(storm_dates["date"])

areas = codab.load_aa_target_and_rest_areas()
hres = load_hres_two_areas(areas)

forecasts = None
for name, key in COLUMN_KEYS.items():
    area_max = max_forecast_rainfall(
        hres[hres["admin"] == name], storm_dates, landfall
    ).rename(columns={"rolling_sum_3": f"forecast_rain_{key}"})
    forecasts = (
        area_max if forecasts is None else forecasts.merge(area_max, on=FORECAST_KEYS)
    )

df_all = df_overview.merge(forecasts, on="storm_name")
for lead_time, df_lead in df_all.groupby("lead_time_landfall"):
    overview_situation_two_areas(
        df_lead,
        analysis_suff=f"{suff}_ecmwf_forecast_{lead_time:.0f}h",
        panels={
            name: (
                f"max_wind_speed_land_{key}",
                f"landfall_{key}",
                f"forecast_rain_{key}",
            )
            for name, key in COLUMN_KEYS.items()
        },
        cerf=True,
        adm_level=constants.adm_level,
        save=True,
        ylabel="Max forecasted 3 Days precipitation [mm]",
    )
