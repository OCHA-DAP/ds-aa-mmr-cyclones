import ocha_stratus as stratus
import requests

import geopandas as gpd

from src.utils.constants import (
    ADM1_LIST,
    ADM3_LIST,
    ISO3,
    PROJECT_PREFIX,
    adm_column,
)
from src.utils import blob_utils


def get_blob_name(iso3: str = ISO3):
    iso3 = iso3.lower()
    return f"{PROJECT_PREFIX}/raw/codab/{iso3}.shp.zip"


def download_codab_to_blob(iso3: str = ISO3):
    url = f"https://data.fieldmaps.io/cod/originals/{iso3.lower()}.shp.zip"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    blob_name = get_blob_name(iso3)
    blob_utils._upload_blob_data(response.content, blob_name, stage="dev")


def load_codab_from_blob(
    iso3: str = ISO3, admin_level: int = 0
):
    if admin_level not in [0,1,2,3]:
        raise ValueError("Only admin level 1 is supported")
    else:
        iso3 = iso3.lower()
        shapefile = f"{iso3}_polbnda_adm{admin_level}_250k_mimu_20240215.shp"
        gdf = stratus.load_shp_from_blob(
            blob_name=get_blob_name(iso3),
            shapefile=shapefile,
            stage="dev",
        )
    return gdf


def load_aa_target_and_rest_areas() -> dict[str, gpd.GeoDataFrame]:
    """Load the AA Target area and the rest of Rakhine as single-row polygons.

    The AA Target area is the union of the ADM3_LIST townships. The rest of
    Rakhine is the difference between the ADM1_LIST state and that area.

    Returns:
        Mapping of area name ("AA Target area", "Rest of Rakhine") to a
        GeoDataFrame in EPSG:4326 with one 'geometry' row and an 'area' column.
    """
    adm1 = load_codab_from_blob(admin_level=1)
    adm3 = load_codab_from_blob(admin_level=3)
    target = adm3.loc[adm3[adm_column].isin(ADM3_LIST)].union_all()
    rakhine = adm1.loc[adm1["ADM1_EN"].isin(ADM1_LIST)].union_all()
    geometries = {"AA Target area": target, "Rest of Rakhine": rakhine.difference(target)}
    return {
        name: gpd.GeoDataFrame({"area": [name]}, geometry=[geometry], crs=adm1.crs).to_crs(4326)
        for name, geometry in geometries.items()
    }
