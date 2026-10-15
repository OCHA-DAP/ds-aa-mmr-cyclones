import geopandas as gpd
from pyproj import Geod
from shapely.ops import nearest_points
from src.utils.constants import *
geod = Geod(ellps="WGS84")
import numpy as np
import pandas as pd
import hashlib

# Function to compute distances from one point to all points in gdf2
def compute_distances_to_df2(row, gdf2):
    lon1, lat1 = row.longitude, row.latitude
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


def find_nearest_polygon(gdf_points, gdf_polygons, crs_projected=MMR_UTM):
    """
    For each point, find the nearest polygon, its distance, and the closest coordinates.

    Parameters
    ----------
    gdf_points : GeoDataFrame
        Points GeoDataFrame (must have geometry column)
    gdf_polygons : GeoDataFrame
        Polygons GeoDataFrame (must have geometry column + ID column)
    crs_projected : int
        Projected CRS EPSG code for distance calculation (default = 3857, meters)

    Returns
    -------
    GeoDataFrame with columns:
        - original point geometry
        - min_dist (meters)
        - nearest_point (geometry in projected CRS)
        - nearest_lon / nearest_lat (in EPSG:4326)
        - polygon ID (from id_col)
    """

    # Project to metric CRS
    gdf_points_proj = gdf_points.to_crs(epsg=crs_projected)
    gdf_polygons_proj = gdf_polygons.to_crs(epsg=crs_projected)

    cols_to_drop = ["ADM0_MY", "ADM0_EN", "ADM0_PCODE"]
    gdf_points_proj.drop(columns=[c for c in cols_to_drop if c in gdf_points_proj.columns], inplace=True)
    # Nearest spatial join
    gdf_joined = gdf_points_proj.sjoin_nearest(
        gdf_polygons_proj, how="left", distance_col="min_dist"
    )

    # Compute nearest boundary point
    def get_nearest_point(row):
        poly_geom = gdf_polygons_proj.loc[row["index_right"], "geometry"]
        _, p2 = nearest_points(row.geometry, poly_geom)
        return p2

    gdf_joined["nearest_point"] = gdf_joined.apply(get_nearest_point, axis=1)

    # Convert nearest_point to WGS84 (lat/lon)
    nearest_gs = gpd.GeoSeries(gdf_joined["nearest_point"], crs=gdf_joined.crs).to_crs(epsg=4326)
    gdf_joined["nearest_lon"] = nearest_gs.x
    gdf_joined["nearest_lat"] = nearest_gs.y

    # Keep only useful columns
    keep_cols = ["ADM", "min_dist", "nearest_lon", "nearest_lat", "geometry", "wind_speed", "valid_time", "landfall", "nature", "sid", "storm_id", "latitude", "longitude", "issued_time", "leadtime", "provider"]
    gdf_result = gdf_joined.loc[:, gdf_joined.columns.str.startswith(tuple(keep_cols))].copy()

    return gdf_result

def max_wind_speed_land_per_storm(
    df_track: pd.DataFrame, area: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Max wind speed at land per storm for an area, plus a landfall flag.

    Each track point's wind is reduced with its distance to the area
    (0.9807 * exp(-0.003 * distance_km)). Points inside the area keep their
    raw wind speed. The point with the highest reduced wind speed is kept per
    storm.

    Args:
        df_track: Track points with 'sid', 'wind_speed' and a 'geometry' WKT
            column in the MMR_UTM CRS.
        area: Polygons of the area of interest.

    Returns:
        DataFrame indexed by 'sid' with columns 'max_wind_speed_land' and
        'landfall' (True when the selected point lies inside the area).
    """
    points = gpd.GeoSeries.from_wkt(df_track["geometry"], crs=MMR_UTM)
    area_union = area.to_crs(epsg=MMR_UTM).union_all()
    distance_km = points.distance(area_union) / 1000
    reduced = 0.9807 * np.exp(-0.003 * distance_km) * df_track["wind_speed"]
    per_point = pd.DataFrame(
        {
            "sid": df_track["sid"],
            "landfall": distance_km == 0,
            "wind_speed": df_track["wind_speed"],
            "reduced": reduced,
        }
    )
    best = per_point.loc[per_point.groupby("sid")["reduced"].idxmax()]
    best = best.assign(
        max_wind_speed_land=best["wind_speed"].where(best["landfall"], best["reduced"])
    )
    return best.set_index("sid")[["max_wind_speed_land", "landfall"]]


def compute_return_period(year_max:int, year_min:int, num_alerts:int ):
    return_period = (year_max - year_min +1)/num_alerts
    return return_period

def from_ms_to_knots(xx: pd.Series):
    conversion_factor = 1.9438444924
    return xx*conversion_factor

def convert_10m_wind_to_3m(xx:pd.Series):
    """
    Standard approximation used by WMO and tropical cyclone agencies
    Args:
        xx: pd.Series with 10m sustained wind speed

    Returns:
        pd.Series with 3m wind speed
    """
    xx = pd.to_numeric(xx, errors="coerce")  # convert to float, turn invalid entries into NaN
    return xx * 1.05

def run_trigger(df: pd.DataFrame, wind_speed_alert_level:int, rainfall_alert_level:int, wind_speed_column:str = "wind_speed"):

    xx = np.where((df[wind_speed_column] >= wind_speed_alert_level) | (df["3days_rain_mean"] >= rainfall_alert_level), True, False)
    return xx

def make_run_metadata(level, areas):
    # Sort areas for stable identifiers
    areas_sorted = sorted(areas)
    areas_str = "_".join(areas_sorted)

    # Hash long identifiers so filenames don’t explode
    scope_hash = hashlib.sha1(areas_str.encode()).hexdigest()[:8]

    return {
        "level": level,  # "adm1" or "adm2"
        "areas": areas_sorted,
        "scope_hash": scope_hash
    }

def make_suffix(meta):
    return f"{meta['level']}_{meta['scope_hash']}"