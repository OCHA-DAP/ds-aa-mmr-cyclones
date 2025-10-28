from dotenv import load_dotenv

from src.datasources.codab import load_codab_from_blob
from src.utils.utils_fun import *
from src.utils.constants import *

load_dotenv()
AA_DATA_DIR = os.getenv("AA_DATA_DIR")
MMR_UTM = 32647
agg_level = 3
col_name = f"ADM{agg_level}_EN"
gdf_adm = load_codab_from_blob(iso3=ISO3, admin_level=agg_level)
ibtracs_path = os.path.join(AA_DATA_DIR, "public/raw/glb/ibtracs")
points_path = os.path.join(ibtracs_path, "IBTrACS.NI.list.v04r01.points.zip")

df_cerf = pd.read_csv("cerf_data.csv")
df_cerf = df_cerf[df_cerf["Country"] == "Myanmar"]
df_cerf = df_cerf[df_cerf["Window"] == "RR"]
df_cerf = df_cerf[df_cerf["Emergency Type"] == "Storm"]

gdf_points = gpd.read_file(points_path, layer="IBTrACS.NI.list.v04r01.points")

gdf_points_1980 = gdf_points[gdf_points["year"] >= 1980]
gdf_points_1980[["NAME", "NEW_WIND"]]  # it seems some values are missing here.

# filling in missing values with those from USA and converting 1-minute to 3-minute winds.
gdf_points_1980.loc[:, "NEW_USA_WIND"] = gdf_points_1980["NEW_WIND"].fillna(
    gdf_points_1980["USA_WIND"] * 0.93
)

adm_buffer = 0  # Use 0, 50 or 100: these are in km
gdf_adm_projected = gdf_adm.to_crs(epsg=MMR_UTM)
gdf_adm_projected["geometry"] = gdf_adm_projected.geometry.buffer(
    adm_buffer * 1000
)
gdf_adm_projected["centroid"] = gdf_adm_projected["geometry"].centroid.to_crs("EPSG:4326")
gdf_adm_projected["lon_centroid"] = gdf_adm_projected["centroid"].x
gdf_adm_projected["lat_centroid"] = gdf_adm_projected["centroid"].y

gdf_adm_buffered = gdf_adm_projected.to_crs(gdf_adm.crs)
gdf_points_adm = gpd.sjoin(
    gdf_points_1980, gdf_adm_buffered, how="inner", predicate="intersects"
)

# Apply to each row of gdf1
gdf_points_adm['distances_km'] = gdf_points_adm.apply(lambda row: compute_distances_to_df2(row, gdf_adm_projected), axis=1)
gdf_points_adm["wind_reduction_factor"] = gdf_points_adm["distances_km"].apply(
    lambda dists: 0.9807 * np.exp(-0.003 * np.array(dists))
)

gdf_points_adm["wind_speed_union"] = gdf_points_adm.apply(
    lambda row: row["wind_reduction_factor"] * row["NEW_USA_WIND"], axis=1
)

records = []

for idx, row in gdf_points_adm.iterrows():
    distance = row["distances_km"]
    wind_speeds = row["wind_speed_union"]
    reduction_factors = row["wind_reduction_factor"]

    for i in range(len(gdf_adm_projected)):
        records.append({
            "SID": row["SID"],
            "ISO_TIME": row["ISO_TIME"],
            f"{col_name}_CYCLONE_CENTER": row[col_name],
            f"{col_name}": gdf_adm_projected.loc[i, col_name],
            f"ADM1_EN": gdf_adm_projected.loc[i, "ADM1_EN"],
            f"ADM2_EN": gdf_adm_projected.loc[i, "ADM2_EN"],
            "wind_speed_center": row["NEW_USA_WIND"],
            "distance": distance[i],
            "wind_speed_union": wind_speeds[i],
            "wind_reduction_factor": reduction_factors[i],
            "lat_centroid_adm2": gdf_adm_projected.loc[i, "lat_centroid"],
            "lon_centroid_adm2": gdf_adm_projected.loc[i, "lon_centroid"],
            "lat_cyclone": row["LAT"],
            "lon_cyclone": row["LON"],
        })

# Create the new DataFrame
flat_df = pd.DataFrame(records)
flat_df.loc[flat_df[col_name]==flat_df[f"{col_name}_CYCLONE_CENTER"], "distance"] = 0
flat_df.loc[flat_df[col_name]==flat_df[f"{col_name}_CYCLONE_CENTER"], "wind_reduction_factor"] = 1
flat_df.loc[flat_df[col_name]==flat_df[f"{col_name}_CYCLONE_CENTER"], "wind_speed_union"] = flat_df.loc[flat_df[col_name]==flat_df[f"{col_name}_CYCLONE_CENTER"], "wind_speed_center"]

flat_df["wind_speed_union"] = pd.to_numeric(flat_df["wind_speed_union"], errors="coerce")
flat_df = flat_df.dropna(subset=["wind_speed_union"])
flat_df = flat_df.loc[flat_df["ADM1_EN"].isin(["Rakhine", "Ayeyarwady"])]
# Get index of max wind_speed_union for each group
idx = flat_df.groupby(["SID", col_name])["wind_speed_union"].idxmax()

# Use that index to select rows from the original dataframe
flat_df_grouped = flat_df.loc[idx].reset_index(drop=True)

bins = [0, 16, 27, 33, 47, 63, 89, 119, float("inf")]
labels = [
    "Below Depression",
    "Depression",
    "Deep Depression",
    "Cyclonic Storm",
    "Severe Cyclonic Storm",
    "Very Severe Cyclonic Storm",
    "Extremely Severe Cyclonic Storm",
    "Super Cyclonic Storm",
]


flat_df_grouped["IMD_SCALE"] = pd.cut(
    flat_df_grouped["wind_speed_union"], bins=bins, labels=labels, right=True
)

flat_df_grouped["year"]=pd.to_datetime(flat_df_grouped["ISO_TIME"]).dt.year
flat_df_grouped2 = (
    flat_df_grouped.groupby([col_name, "ADM1_EN", "ADM2_EN"])
    .apply(compute_table)
    .reset_index(drop=True)
)
