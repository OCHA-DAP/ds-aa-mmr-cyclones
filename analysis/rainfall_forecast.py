from dotenv import load_dotenv
from src.utils.utils_fun import *
from src.utils.utils_plot import plot_rainfall_forecast, overview_situation

# Load .env file into environment variables
load_dotenv()
import ocha_stratus as stratus
import pandas as pd
import pickle

meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)

def load_rainfall_forecast():
    query = """
    SELECT * FROM projects.ds_aa_mmr_cyclones_chirps_gefs
    """
    df = pd.read_sql(query, stratus.get_engine("dev"))
    return df

with open('src/data/storms_date.pickle', 'rb') as file:
    storms_dict = pickle.load(file)

storms_date = pd.DataFrame.from_dict(storms_dict)
df_long = storms_date.melt(
    var_name="storm_name",
    value_name="date"
)
df_long["date"] = pd.to_datetime(df_long["date"])
rainfall_forecast = load_rainfall_forecast()
rainfall_forecast ["valid_date"] = pd.to_datetime(rainfall_forecast["valid_date"])
rainfall_forecast_full = df_long.merge(rainfall_forecast, how="right", right_on="valid_date", left_on="date")
rainfall_forecast_full.dropna(inplace=True)
rainfall_forecast_full.drop(columns=["date"], inplace=True)

rainfall_forecast_full["valid_date"] = pd.to_datetime(rainfall_forecast_full["valid_date"])
rainfall_forecast_full["issued_date"] = pd.to_datetime(rainfall_forecast_full["issued_date"])

rainfall_forecast_full = rainfall_forecast_full.sort_values(["storm_name", "issued_date", "valid_date"])

rainfall_forecast_full["rolling_sum_3"] = (
    rainfall_forecast_full.groupby(["storm_name", "issued_date"])["mean"]
      .rolling(3, min_periods=1)
      .sum()
      .reset_index(level=[0,1], drop=True)
)

df_observed = pd.read_csv(f"results/df_full_{suff}.csv")
df_observed["landfall_adm0_date"] = pd.to_datetime(df_observed["landfall_adm0_date"])
rainfall_forecast_full=rainfall_forecast_full.merge(df_observed[["storm_name", "landfall_adm0_date", '3days_rain_mean']], how="left", left_on="storm_name", right_on="storm_name")
rainfall_forecast_full["landfall_adm0_date"] = pd.to_datetime(rainfall_forecast_full["landfall_adm0_date"])
rainfall_forecast_full["landfall_adm0_date"]=rainfall_forecast_full["landfall_adm0_date"].dt.date
rainfall_forecast_full["landfall_adm0_date"] = pd.to_datetime(rainfall_forecast_full["landfall_adm0_date"])
for storm in rainfall_forecast_full["storm_name"].unique():
    df_plot = rainfall_forecast_full[rainfall_forecast_full["storm_name"] == str(storm)]
    plot_rainfall_forecast(df_plot, save=True)

rainfall_forecast_full["lead_time_landfall"] = (rainfall_forecast_full["landfall_adm0_date"] - rainfall_forecast_full["issued_date"]).dt.days
rainfall_forecast_filtered = rainfall_forecast_full[(rainfall_forecast_full["issued_date"] >= rainfall_forecast_full["landfall_adm0_date"] - pd.Timedelta(days=3)) &
    (rainfall_forecast_full["issued_date"] <= rainfall_forecast_full["landfall_adm0_date"] - pd.Timedelta(days=1))]

rainfall_forecast_filtered = rainfall_forecast_filtered[(rainfall_forecast_filtered["valid_date"] >= rainfall_forecast_filtered["landfall_adm0_date"] - pd.Timedelta(days=3)) &
    (rainfall_forecast_filtered["valid_date"] <= rainfall_forecast_filtered["valid_date"] + pd.Timedelta(days=1))]

max_rainfall = rainfall_forecast_filtered.groupby(["storm_name", "issued_date", "lead_time_landfall"], as_index=False)["rolling_sum_3"].max()

df_all = df_observed.merge(max_rainfall, how="left", on="storm_name")
for lead_time in df_all["lead_time_landfall"].unique():
    df_plot=df_all[df_all["lead_time_landfall"] == lead_time]
    overview_situation(df_plot, analysis_suff=suff, y_column='rolling_sum_3', save=True, cerf=True, title_suff=f"forecast_{lead_time}")

