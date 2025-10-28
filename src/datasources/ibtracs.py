import ocha_stratus as stratus
import pandas as pd


def load_ibtracs_tracks():
    query = f"""
    SELECT * FROM storms.ibtracs_tracks_geo
    """
    df = pd.read_sql(query, stratus.get_engine("dev"))
    return df

def load_ibtracs_in_bounds(min_lon, min_lat, max_lon, max_lat):
    query = f"""
    SELECT * FROM storms.ibtracs_tracks_geo
    WHERE longitude BETWEEN {min_lon} AND {max_lon}
    AND latitude BETWEEN {min_lat} AND {max_lat}
    """
    df = pd.read_sql(query, stratus.get_engine("dev"))
    return df


def load_storms():
    query = """
    SELECT * FROM storms.storms
    """
    df = pd.read_sql(query, stratus.get_engine("dev"))
    return df


def knots2cat(knots):
    """Convert wind speed in knots to Saffir-Simpson hurricane category."""
    category = 0
    if knots >= 137:
        category = 5
    elif knots >= 113:
        category = 4
    elif knots >= 96:
        category = 3
    elif knots >= 83:
        category = 2
    elif knots >= 64:
        category = 1
    return category

def categorize_storms(df):
    """
    India Meteorological Department Tropical Cyclone Intensity Scale
    Args:
        df:

    Returns:

    """
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
    df["IMD_SCALE"] = pd.cut(
        df["max_wind_speed_land"], bins=bins, labels=labels, right=True
    )
    return df