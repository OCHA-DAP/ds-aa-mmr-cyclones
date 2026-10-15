import pandas as pd
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True), override=True)

from src.datasources import codab, imerg
from src.utils import constants
from src.utils.utils_fun import (
    make_run_metadata,
    make_suffix,
    max_wind_speed_land_per_storm,
)
from src.utils.utils_plot import overview_situation_two_areas

meta = make_run_metadata(level=constants.adm_level, areas=constants.ADM_LIST)
suff = make_suffix(meta=meta)

df = pd.read_csv(f"results/df_full_{suff}.csv", parse_dates=["time"])
df_track = pd.read_csv(f"results/ibtracs_data_track_{suff}.csv")

# Duplicated rows (several nearest ADM3 per storm) would inflate the 3-day sums
daily = df.drop_duplicates(["sid", "time"])
dates = daily["time"].unique()

areas = codab.load_aa_target_and_rest_areas()
COLUMN_KEYS = {"AA Target area": "aa_target", "Rest of Rakhine": "rest_rakhine"}

per_area = []
for name, area in areas.items():
    key = COLUMN_KEYS[name]
    area_daily = imerg.mean_rainfall_over_area(dates, area.geometry)
    rain_sum = (
        daily.assign(rain=daily["time"].map(area_daily))
        .groupby("sid")["rain"]
        .sum()
        .rename(f"rain_{key}")
    )
    wind = max_wind_speed_land_per_storm(df_track, area).add_suffix(f"_{key}")
    per_area.extend([rain_sum, wind])

df_plot = df.drop_duplicates("sid").merge(
    pd.concat(per_area, axis=1), left_on="sid", right_index=True
)
df_plot.to_csv(f"results/df_overview_two_areas_{suff}.csv", index=False)

overview_situation_two_areas(
    df_plot,
    analysis_suff=suff,
    panels={
        name: (
            f"max_wind_speed_land_{key}",
            f"landfall_{key}",
            f"rain_{key}",
        )
        for name, key in COLUMN_KEYS.items()
    },
    cerf=True,
    adm_level=constants.adm_level,
    save=True,
)
