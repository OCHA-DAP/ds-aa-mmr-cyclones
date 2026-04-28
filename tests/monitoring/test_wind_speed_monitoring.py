import pytest
import pandas as pd
from geopandas import GeoDataFrame
from dotenv import load_dotenv
from src.datasources import codab
from src.monitoring.wind_speed_monitoring import plot_storm_track
from shapely.geometry import Point
from src.utils import constants
load_dotenv()
import ocha_stratus as stratus


def test_plot_storm_track():
    df = stratus.load_csv_from_blob(blob_name=f"{constants.PROJECT_PREFIX}/processed/test_monitoring.csv")
    geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
    df = GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    adm_boundaries = codab.load_codab_from_blob(admin_level=constants.adm_level)
    plot_storm_track(storms_area_interest=df, adm_boundaries=adm_boundaries, today="test", hour="output")
