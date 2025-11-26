from dotenv import load_dotenv
from shapely import wkt

from src.datasources import codab
# Load .env file into environment variables
load_dotenv()

from src.utils.utils_fun import *
from src.utils.utils_fun import from_ms_to_knots
from src.utils.utils_plot import *

meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)

adm_boundaries = codab.load_codab_from_blob(admin_level=adm_level)
adm_buffer = 0  # Use 0, 50 or 100: these are in km
gdf_adm_projected = adm_boundaries.to_crs(epsg=MMR_UTM)
gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
    adm_buffer * 1000
)
gdf_adm_projected_filtered= gdf_adm_projected.loc[adm_boundaries[adm_column].isin(ADM_LIST)]

df_historical = pd.read_csv(f"results/df_full_{suff}.csv")
df_historical = df_historical.loc[df_historical["t_delta"]==0, :]
df_historical.sort_values(by="time", ascending=True, inplace=True)
list_hist_storms_forecast = []
# for row, dd in df_historical.iterrows():
#     print(f"processing {dd.time}")
#     start_date = pd.to_datetime(dd.time) - timedelta(days=7)
#     end_date = pd.to_datetime(dd.time) + timedelta(days=3)
#     storm_name = dd.storm_name
#     # Load ECMWF forecasts as a pandas dataframe
#     df = lens.ecmwf_storm.load_hindcasts(
#         start_date=start_date,
#         end_date=end_date,
#         skip_if_missing=True
#     )
#     if df is not None:
#         df.dropna(subset=["name"], inplace=True)
#         df_storm = df[df["name"].str.contains(storm_name, case=False, na=False)]
#         storm_ids = df_storm.id.unique()
#         list_hist_storms_forecast.append(df_storm)
#     print(f" {dd.time} Done")
#     print("---------------")

#
# df_hist_storms_forecast = pd.concat(list_hist_storms_forecast, ignore_index=True)
# #Get track data
# gdf_tracks = lens.ecmwf_storm.get_tracks(df_hist_storms_forecast)
# gdf_tracks.to_csv(f"results/ecmw_tracks_{suff}.csv", index=False)
gdf_tracks = pd.read_csv(f"results/ecmw_tracks_{suff}.csv")
gdf_tracks["geometry"] = gdf_tracks["geometry"].apply(wkt.loads)
gdf_tracks = gpd.GeoDataFrame(gdf_tracks, geometry="geometry", crs="EPSG:4326")
gdf_result = find_nearest_polygon(gdf_tracks , gdf_adm_projected_filtered)

gdf_result["min_dist_km"] = gdf_result["min_dist"]/1000

# Apply to each row of gdf1
gdf_result["wind_reduction_factor"] = gdf_result["min_dist_km"].apply(
     lambda dists: 0.9807 * np.exp(-0.003 * np.array(dists)))
# Convert to knots
gdf_result["wind_speed"]=from_ms_to_knots(gdf_result["wind_speed"])
gdf_result["wind_speed"]=convert_10m_wind_to_3m(gdf_result["wind_speed"])
gdf_result["wind_speed_at_land_forecasted"] = gdf_result.apply(
    lambda row: row["wind_reduction_factor"] * row["wind_speed"], axis=1
)
gdf_result["3days_rain_mean"]=0
gdf_result["trigger_at_land"]=run_trigger(gdf_result, windspeed_alert_level=windspeed_alert_level, rainfall_alert_level=rainfall_alert_level, windspeed_column="wind_speed_at_land_forecasted")
gdf_result.rename(columns={"leadtime":"leadtime_forecast"}, inplace=True)
gdf_result.drop_duplicates(inplace=True)


df_ibtracs = pd.read_csv(f"results/df_full_{suff}.csv")
df_ibtracs = df_ibtracs.loc[df_ibtracs["t_delta"]==0]
df_comparison = gdf_result.merge(df_ibtracs[["storm_id", "max_wind_speed_land", "IMD_SCALE", "storm_name", "cerf_allocation", "valid_time", "landfall_adm0_date", "landfall_adm0"]], how="inner", on="storm_id", suffixes=("_forecasted", "_observed"))
df_comparison["geometry"]= df_comparison["geometry"].to_crs(epsg=MMR_UTM)
df_comparison["longitude"] = df_comparison.geometry.x
df_comparison["latitude"]  = df_comparison.geometry.y
df_comparison.rename(columns={"max_wind_speed_land":"wind_speed_observed"}, inplace=True)
df_comparison["leadtime_landfall"] = np.where(df_comparison["landfall_adm0"], pd.to_datetime(df_comparison["valid_time_observed"]) - pd.to_datetime(df_comparison["issued_time"]), pd.to_datetime(df_comparison["landfall_adm0_date"]) - pd.to_datetime(df_comparison["issued_time"]))
df_comparison.drop_duplicates(inplace=True)
df_comparison.to_csv(f"results/hist_forecast_trigger_{suff}.csv", index=False)

result = (
    df_comparison[df_comparison["trigger_at_land"] == True]
    .sort_values(["storm_id", "issued_time", "leadtime_landfall"], ascending=[True, True, False])
    .groupby("storm_id", as_index=False)
    .first()
)
result.to_csv(f"results/hist_forecast_trigger_True_{suff}.csv", index=False)

df_ibtracs_trace  = pd.read_csv(f"results/ibtracs_data_track_{suff}.csv")
# Convert the geometry column from string to shapely geometries

df_ibtracs_trace["geometry"] = df_ibtracs_trace["geometry"].apply(wkt.loads)
df_ibtracs_trace = gpd.GeoDataFrame(df_ibtracs_trace, geometry="geometry", crs=f"EPSG:{MMR_UTM}")
df_ibtracs_trace["longitude"] = df_ibtracs_trace.geometry.x
df_ibtracs_trace["latitude"] = df_ibtracs_trace.geometry.y
for storm in df_comparison["storm_id"].unique():
    df_observed = df_ibtracs_trace.loc[df_ibtracs_trace["storm_id"]==storm,:]
    df_forecasted = df_comparison.loc[df_comparison["storm_id"] == storm, :]
    plot_storm_track_comparison(df_observed, df_forecasted, gdf_adm_projected , save=True)