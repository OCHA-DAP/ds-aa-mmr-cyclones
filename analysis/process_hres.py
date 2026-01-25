import glob
from pathlib import Path
from datetime import timedelta
import os
import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
import numpy as np
from dotenv import load_dotenv
from src.datasources import codab
load_dotenv()
adm_boundaries = codab.load_codab_from_blob(admin_level=1)
gdf_adm_projected = adm_boundaries.to_crs(epsg=32647)

def load_hres_daily_precip(grib_path):
    """
    Load HRES GRIB and return daily rainfall (mm)
    indexed by valid_time, lat, lon
    """

    ds = xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "typeOfLevel": "surface",
                "shortName": "tp"
            }
        }
    )

    # meters -> mm
    tp = ds.tp * 1000

    # cumulative -> daily increments
    daily_tp = tp.diff("step")
    daily_tp = daily_tp.assign_coords(step=tp.step[1:])

    # valid time
    init_time = pd.to_datetime(ds.time.values)
    valid_times = [
        init_time + pd.to_timedelta(int(s), unit="h")
        for s in daily_tp.step.values
    ]
    daily_tp = daily_tp.assign_coords(
        valid_time=("step", valid_times)
    )

    # fix latitude order
    daily_tp = daily_tp.sortby("latitude")

    # add CRS
    daily_tp = daily_tp.rio.write_crs("EPSG:4326")

    return daily_tp

def aggregate_to_admin(daily_tp, admin_gdf, admin_name_col):
    """
    Aggregate daily rainfall to admin units (mean)
    Returns a DataFrame
    """

    records = []

    for _, row in admin_gdf.iterrows():
        clipped = daily_tp.rio.clip(
            [row.geometry],
            all_touched=True,
            drop=True
        )

        # cosine latitude weighting (recommended)
        weights = np.cos(np.deg2rad(clipped.latitude))
        mean_tp = clipped.weighted(weights).mean(
            dim=("latitude", "longitude")
        )

        df = mean_tp.to_dataframe(name="rain_mm").reset_index()
        df["admin"] = row[admin_name_col]

        records.append(df)

    return pd.concat(records, ignore_index=True)

def process_hres_folder(
    grib_dir,
    admin_file,
    admin_level_col="NAME_1"
):
    """
    Iterate over all GRIB files and aggregate daily rainfall
    """

    admin = admin_file.to_crs("EPSG:4326")

    all_results = []

    for grib_file in os.path(f"{grib_dir}/MMR_hres_*.grib"):
        print(f"Processing {grib_file.name}")

        daily_tp = load_hres_daily_precip(grib_file)
        df = aggregate_to_admin(daily_tp, admin, admin_level_col)

        # extract init date from filename
        init_date = grib_file.stem.split("_")[-1]
        df["init_date"] = pd.to_datetime(init_date)

        all_results.append(df)

    return pd.concat(all_results, ignore_index=True)

if __name__ == "__main__":

    grib_directory = "./grib"

    df = process_hres_folder(
        grib_dir=grib_directory,
        admin_file=adm_boundaries,
        admin_level_col="ADM1_EN"
    )

    df = df.rename(columns={
        "step": "lead_time_h"
    })

    df.to_csv("MMR_HRES_daily_rain_ADM1.csv", index=False)

    print("Done ✅")
