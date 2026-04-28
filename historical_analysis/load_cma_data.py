from dotenv import load_dotenv
from shapely import wkt

from src.datasources import codab
# Load .env file into environment variables
load_dotenv()

from src.utils.utils_fun import *
from src.utils.utils_fun import from_ms_to_knots
from src.utils.utils_plot import *
import ocha_stratus as stratus
import  ocha_lens as lens
meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)

adm_boundaries = codab.load_codab_from_blob(admin_level=adm_level)
adm_buffer = 0  # Use 0, 50 or 100: these are in km
gdf_adm_projected = adm_boundaries.to_crs(epsg=MMR_UTM)
gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
    adm_buffer * 1000
)
gdf_adm_projected_filtered= gdf_adm_projected.loc[adm_boundaries[adm_column].isin(ADM_LIST)]

df_historical = stratus.load_parquet_from_blob("ds-cma-datasharing/processed/2022-2025_BoB_TC.parquet")
df_historical.sort_values(by="analysis_datetime", ascending=True, inplace=True)

df_historical = df_historical.rename(columns={
    "wind_speed_ms": "wind_speed",
    "valid_datetime": "valid_time",
    "analysis_datetime": "issued_time",
    "forecast_hour": "leadtime",
    "lat": "latitude",
    "lon": "longitude",
})
df_historical["sid"] = df_historical["storm_id"]

gdf_tracks = gpd.GeoDataFrame(
    df_historical,
    geometry=gpd.points_from_xy(df_historical["longitude"], df_historical["latitude"]),
    crs="EPSG:4326",
)

gdf_result = find_nearest_polygon(gdf_tracks, gdf_adm_projected_filtered)
gdf_result["min_dist_km"] = gdf_result["min_dist"] / 1000

# Apply to each row of gdf1
gdf_result["wind_reduction_factor"] = gdf_result["min_dist_km"].apply(
     lambda dists: 0.9807 * np.exp(-0.003 * np.array(dists)))
# Convert to knots
gdf_result["wind_speed"] = from_ms_to_knots(gdf_result["wind_speed"])
gdf_result["wind_speed"] = convert_10m_wind_to_3m(gdf_result["wind_speed"])
gdf_result["wind_speed_at_land_forecasted"] = gdf_result.apply(
    lambda row: row["wind_reduction_factor"] * row["wind_speed"], axis=1
)
gdf_result["3days_rain_mean"] = 0
gdf_result["trigger_at_land"] = run_trigger(gdf_result, wind_speed_alert_level=wind_speed_alert_level, rainfall_alert_level=rainfall_alert_level_forecast, windspeed_column="wind_speed_at_land_forecasted")
gdf_result.rename(columns={"leadtime": "leadtime_forecast"}, inplace=True)
gdf_result.drop_duplicates(inplace=True)
