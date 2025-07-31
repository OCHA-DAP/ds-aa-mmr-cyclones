import os

import geopandas as gpd

AA_DATA_DIR = os.getenv("AA_DATA_DIR")

def load_codab(admin_level: int = 0):
    AA_DATA_DIR = os.getenv("AA_DATA_DIR")
    if admin_level not in [0,1,2,3]:
        raise ValueError("Only admin level 1 is supported")
    if admin_level in [0,1,2, 3]:
        adm_path = os.path.join(AA_DATA_DIR,
            "public/raw/mmr/cod_ab/mmr_adm_250k_mimu_20240215_ab_shp.zip"
        )
    gdf = gpd.read_file(adm_path, layer = f"mmr_polbnda_adm{admin_level}_250k_mimu_20240215")
    return gdf