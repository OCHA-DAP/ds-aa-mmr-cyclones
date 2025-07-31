from pyproj import Geod
geod = Geod(ellps="WGS84")
import pandas as pd
# Function to compute distances from one point to all points in gdf2
def compute_distances_to_df2(row, gdf2):
    lon1, lat1 = row.LON, row.LAT
    # Repeat scalar to match array shape
    lon1_array = [lon1] * len(gdf2)
    lat1_array = [lat1] * len(gdf2)
    lons2 = gdf2.lon_centroid.values
    lats2 = gdf2.lat_centroid.values
    _, _, distances = geod.inv(lon1_array, lat1_array, lons2, lats2)
    return distances / 1000  # in kilometers

# Function to calculate df_table per ADM2
def compute_table(group):
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

    min_year = group["year"].min()
    max_year = group["year"].max()
    total_years = max_year - min_year + 1

    intensity_counts = group["IMD_SCALE"].value_counts().reindex(labels, fill_value=0)
    return_periods = total_years / intensity_counts.replace(0, float("inf"))
    cumulative_counts = intensity_counts[::-1].cumsum()[::-1]
    return_periods_cumulative = total_years / cumulative_counts.replace(0, float("inf"))

    return pd.DataFrame({
        "ADM3_EN": group["ADM3_EN"].iloc[0],
        "ADM2_EN": group["ADM2_EN"].iloc[0],
        "ADM1_EN": group["ADM1_EN"].iloc[0],
        "Cyclone Intensity Category": labels,
        "Number of Observations": intensity_counts.values,
        "Return Period (years)": return_periods.round(1),
        "Return Period (≥ Intensity) (years)": return_periods_cumulative.round(1),
    })