import matplotlib.pyplot as plt
import calendar
from dotenv import load_dotenv

from src.datasources.codab import load_codab_from_blob
from src.utils.utils_fun import *

load_dotenv()
AA_DATA_DIR = os.getenv("AA_DATA_DIR")
MMR_UTM = 32647
agg_level = "col_name"
gdf_adm = load_codab_from_blob(admin_level=2)
ibtracs_path = os.path.join(AA_DATA_DIR, "public/raw/glb/ibtracs")
points_path = os.path.join(ibtracs_path, "IBTrACS.NI.list.v04r01.points.zip")

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

for agg in gdf_points_adm[agg_level].unique():

    gdf_points_adm_agg = gdf_points_adm[gdf_points_adm["ADM1_EN"].isin(["Rakhine", "Ayeyarwady"])]
    gdf_points_adm_agg = gdf_points_adm_agg[gdf_points_adm_agg[agg_level]== agg]
    adm1 = gdf_points_adm_agg["ADM1_EN"].unique()
    if len(gdf_points_adm_agg)>0:
        ax = gdf_adm.plot(color="lightblue", edgecolor="black", alpha=0.3)
        gdf_points_adm_agg.plot(ax=ax, column="NEW_USA_WIND", markersize=10, legend=True)

        month_counts = gdf_points_adm_agg.groupby("month")["SID"].nunique()

        total_storms = gdf_points_adm_agg["SID"].nunique()
        month_percent = (month_counts / total_storms) * 100

        month_labels = [calendar.month_name[m] for m in month_counts.index]

        plt.figure(figsize=(10, 5))
        month_percent.plot(kind="bar", color="skyblue", edgecolor="black")

        # Labels and title
        plt.xlabel("Month")
        plt.ylabel("Percentage of Total Storms (%)")
        plt.title(f"Percentage of Storms by Month in {agg} - {adm1}- Buffer: {adm_buffer}km")
        plt.xticks(ticks=range(len(month_labels)), labels=month_labels, rotation=45)
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        plt.savefig(f"percentage_storm_{agg}_{adm1}.png")
        plt.close()



min_year = gdf_points_adm["year"].min()
max_year = gdf_points_adm["year"].max()
total_years = max_year - min_year + 1
intensity_counts = (
    gdf_points_adm["IMD_SCALE"].value_counts().reindex(labels, fill_value=0)
)

return_periods = total_years / intensity_counts.replace(0, float("inf"))
cumulative_counts = intensity_counts[::-1].cumsum()[::-1]
return_periods_cumulative = total_years / cumulative_counts.replace(
    0, float("inf")
)
df_table = pd.DataFrame(
    {
        "Cyclone Intensity Category": labels,
        "Number of Observations": intensity_counts.values,
        "Return Period (years)": return_periods.round(1),
        "Return Period (≥ Intensity) (years)": return_periods_cumulative.round(
            1
        ),
    }
)
fig, ax = plt.subplots(figsize=(10, 5))
ax.axis("tight")
ax.axis("off")
table = ax.table(
    cellText=df_table.values,
    colLabels=df_table.columns,
    cellLoc="center",
    loc="center",
)

table.auto_set_font_size(False)
table.set_fontsize(10)
table.auto_set_column_width([0, 1, 2, 3])

plt.title(
    f"Cyclone Return Periods by Intensity (IMD Classification)\nBased on {total_years} Years of Data ({min_year}-{max_year}) - Rakhine State",
    fontsize=12,
    weight="bold",
)
plt.savefig("MMR_Cyclones_return_periods_Rakhine.png")
plt.show()

gdf_points_adm_filtered = gdf_points_adm[gdf_points_adm ["IMD_SCALE"].notna()]
idx = gdf_points_adm_filtered.groupby("SID")["NEW_USA_WIND"].idxmax()

# Select those rows from the original DataFrame
result = gdf_points_adm_filtered.loc[idx].reset_index(drop=True)


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

# Filter for Rakhine
gdf_points_adm = gdf_points_adm[gdf_points_adm["ADM1_EN"] == "Ayeyarwady"]

# Assign intensity category
gdf_points_adm["IMD_SCALE"] = pd.cut(
    gdf_points_adm["NEW_USA_WIND"], bins=bins, labels=labels, right=True
)

# Apply per col_name
df_grouped = (
    gdf_points_adm.groupby("col_name")
    .apply(compute_table)
    .reset_index(drop=True)
)

df_grouped.to_csv("MMR_cyclones_rp_Ayeyarwady.csv")

df_cyclone_counts = (
    gdf_points_adm
    .groupby("col_name")
    .size()
    .reset_index(name="Number of Cyclones")
)