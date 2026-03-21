import pandas as pd
from dotenv import load_dotenv
# Load .env file into environment variables
load_dotenv()

from src.datasources import imerg
from src.datasources import codab
from src.utils.utils_fun import *
from src.utils.utils_plot import *
from src.utils.constants import *

# Define offsets in days
offsets = [-1, 0, 1]

pcode = ISO3.upper()

meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)

def get_adm_level_rainfall(adm_level):
    if adm_level>2:
        adm_level_rainfall = 2
    else:
        adm_level_rainfall = adm_level
    return adm_level_rainfall
adm_level_rainfall = get_adm_level_rainfall(adm_level)
adm_column = f"ADM{adm_level}_EN"
adm_pcode_column = f"ADM{adm_level_rainfall}_PCODE"

adm0 = codab.load_codab_from_blob(admin_level=0)
adm_boundaries = codab.load_codab_from_blob(admin_level=adm_level)

adm_filtered = adm_boundaries.loc[adm_boundaries[adm_column].isin(ADM_LIST)]
adm_pcodes= adm_filtered[adm_pcode_column]

# load all IMERG data - bit slow
df_imerg = imerg.load_imerg(pcode)
df_imerg = df_imerg.loc[(df_imerg["adm_level"] == adm_level_rainfall)&(df_imerg["pcode"].isin(adm_pcodes)), :]
df_imerg["valid_date"] = pd.to_datetime(df_imerg["valid_date"]).dt.date

df = pd.read_csv(f"results/ibtracs_data_{suff}.csv")

# Expand
df_expanded = (
    df.assign(key=1)  # helper column
      .merge(pd.DataFrame({"t_delta": offsets, "key": 1}), on="key")
      .drop("key", axis=1)
)

# Compute shifted valid_time
df_expanded["time"] = (pd.to_datetime(df_expanded["valid_time"]) + pd.to_timedelta(df_expanded["t_delta"], unit="D")).dt.date
df_expanded = df_expanded.merge(df_imerg, left_on=["time", adm_pcode_column], right_on=["valid_date", "pcode"], how="left")

df_expanded = df_expanded[ pd.to_datetime(df_expanded["time"]) > pd.to_datetime("2000-01-01")]
df_rain_sum = df_expanded.groupby("sid", as_index=False)["mean"].sum()
df_rain_sum.rename(columns={"mean":"3days_rain_mean"}, inplace=True)
df_expanded = df_expanded.merge(df_rain_sum, on="sid", how="left")
df_expanded["storm_name"] = (df_expanded["storm_id"].apply(lambda x: x.split('_')[0])).str.title()

df_cerf = pd.read_csv(f"src/data/cerf_data.csv")
df_full = df_expanded.merge(df_cerf[["sid", "Amount Approved"]], how="left", on="sid")
df_full["cerf_allocation"] = np.where(df_full["Amount Approved"].isna(), "NO Allocation", "CERF allocation")

# Load EMDAT data
df_emdat = pd.read_csv(f"src/data/emdat_mmr.csv")
df_full = df_full.merge(df_emdat[["sid", "Total Deaths", "Total Affected"]], how="left", on="sid")
overview_situation(df_full, analysis_suff=suff, save=True, cerf=True, adm_level=adm_level)

# Return period stuff
# Save Data
df_full.to_csv(f"results/df_full_{suff}.csv", index=False)