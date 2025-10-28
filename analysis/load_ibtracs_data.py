from shapely import wkb
from dotenv import load_dotenv

# Load .env file into environment variables
load_dotenv()

from src.datasources.ibtracs import categorize_storms
from src.datasources import ibtracs, codab
from src.utils.utils_fun import *

from src.utils.utils_plot import *

meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)

adm0 = codab.load_codab_from_blob(admin_level=0)
adm_boundaries = codab.load_codab_from_blob(admin_level=adm_level)

adm_filtered = adm_boundaries.loc[adm_boundaries[adm_column].isin(ADM_LIST)]
adm_buffer = 200  # Use 0, 50 or 100: these are in km
gdf_adm_projected = adm0.to_crs(epsg=MMR_UTM)
gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
    adm_buffer * 1000
)
# Reproject back to lat/lon (WGS84)
gdf_adm_buffered = gdf_adm_projected.to_crs(epsg=4326)
total_bounds=gdf_adm_buffered.total_bounds

#Get all storms that in the total bounds (a large buffered area from Myanmar boundaries)
df_all = ibtracs.load_ibtracs_tracks()
df_all["geometry"] = df_all["geometry"].apply(wkb.loads)
gdf_all = gpd.GeoDataFrame(
    data=df_all,
    geometry=df_all.geometry,
    crs=4326,
)
gdf_all["longitude"] = gdf_all.geometry.x
gdf_all["latitude"]  = gdf_all.geometry.y

gdf_filtered = gpd.sjoin(
    gdf_all,
    gdf_adm_buffered,
    how="inner",        # keep only points that fall inside polygons
    predicate="within"  # check points within polygons
).drop(columns="index_right")


gdf_filtered["wind_speed"] = np.where(gdf_filtered["provider"]=="tokyo", convert_10m_wind_to_3m(["wind_speed"]), gdf_filtered["wind_speed"])
gdf_filtered = gdf_filtered.dropna(subset=["wind_speed"])
# Get all storms with landfall in adm1 selected
gdf_filtered["landfall"] = gdf_filtered.geometry.apply(
    lambda pt: adm_filtered.geometry.contains(pt).any()
)
gdf_filtered["landfall_adm0"] = gdf_filtered.geometry.apply(
    lambda pt: adm0.geometry.contains(pt).any()
)
gdf_filtered["landfall_adm0_date"] = (
    gdf_filtered.groupby("sid")
    .apply(lambda g: g.loc[g["landfall_adm0"], "valid_time"].min())
    .reindex(gdf_filtered["sid"])
    .values
)
gdf_filtered_recent = gdf_filtered[gdf_filtered["valid_time"].dt.year >= 2000]
plot_map_storms(gdf_filtered_recent, adm_boundaries, analysis_suff = suff, trace=True, save=True)
plot_map_storms(gdf_filtered_recent, adm_boundaries, analysis_suff = suff, trace=False, save=True)

gdf_all["year"] = gdf_all.valid_time.dt.year
gdf_points_1980 = gdf_all[gdf_all["year"] >= 1980]

# ====================================================
# Compute wind speed reduction at the closest point to area of interest
# 1) get Polygon of jointed geometries
# 2) Get minimum distance from each point of the trace
# 3) Get wind_speed for each point of the trace at the minimum distance
# 4) Keep point for each trace with max reduced wind speed
# ====================================================
adm_buffer = 0  # Use 0, 50 or 100: these are in km
gdf_adm_projected = adm_boundaries.to_crs(epsg=MMR_UTM)
gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
    adm_buffer * 1000
)
gdf_adm_projected= gdf_adm_projected.loc[adm_boundaries[adm_column].isin(ADM_LIST)]

gdf_result = find_nearest_polygon(gdf_filtered, gdf_adm_projected)

gdf_result["min_dist_km"] = gdf_result["min_dist"]/1000

# Apply to each row of gdf1
gdf_result["wind_reduction_factor"] = gdf_result["min_dist_km"].apply(
     lambda dists: 0.9807 * np.exp(-0.003 * np.array(dists)))

gdf_result["wind_speed_at_land"] = gdf_result.apply(
    lambda row: row["wind_reduction_factor"] * row["wind_speed"], axis=1
)
gdf_result.loc[gdf_result["storm_id"]=="mahasen:viyaru_ni_2013", "storm_id"]="mahasen_ni_2013"
gdf_result.loc[gdf_result["storm_id"]=="pabuk_wp_2018", "storm_id"]="pabuk_wp_2019"
gdf_result.to_csv(f"results/ibtracs_data_track_{suff}.csv", index=False)
idx_max_speed = gdf_result.groupby(["storm_id"])["wind_speed_at_land"].idxmax()
gdf_max_speed = gdf_result.loc[idx_max_speed]
plot_map_storms_speed_area_interest(gdf_max_speed, adm_boundaries, analysis_suff=suff, save=True)
gdf_max_speed["max_wind_speed_land"] = np.where(
    gdf_max_speed["landfall"],
    gdf_max_speed["wind_speed"],
    gdf_max_speed["wind_speed_at_land"]
)
gdf_max_speed["landfall_adm0_date"] = np.where(
    gdf_max_speed["landfall_adm0"],
    gdf_max_speed["landfall_adm0_date"],
    gdf_max_speed["valid_time"]
)
gdf_max_speed = categorize_storms(gdf_max_speed)
gdf_max_speed.to_csv(f"results/ibtracs_data_{suff}.csv", index=False)

