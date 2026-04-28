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
