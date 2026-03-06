import ocha_stratus
import pandas as pd
import tempfile
import os
import xarray as xr
import numpy as np
from dotenv import load_dotenv
from src.datasources import codab

load_dotenv()
adm_boundaries = codab.load_codab_from_blob(admin_level=1)
gdf_adm_projected = adm_boundaries.to_crs(epsg=32647)

def get_hres_daily_precip(ds):
    """
    Load HRES GRIB and return daily rainfall (mm)
    indexed by valid_time, lat, lon
    """

    # meters -> mm
    tp = ds.tp * 1000

    # cumulative -> daily increments
    daily_tp = tp.diff("step")
    daily_tp = daily_tp.assign_coords(step=tp.step[1:])

    # valid time (step already timedelta64)
    if isinstance(ds.time.values, np.ndarray):
        init_time = pd.to_datetime(ds.time.values[0])
        valid_times = init_time + daily_tp.step.values

        daily_tp = daily_tp.assign_coords(
            valid_time=("step", valid_times)
        )

        # fix latitude order
        daily_tp = daily_tp.sortby("latitude")

        # add CRS
        daily_tp = daily_tp.rio.write_crs("EPSG:4326")

        return daily_tp
    else:
        return None


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

import tempfile
from pathlib import Path

def process_hres_folder(
    grib_files,
    admin_file,
    admin_level_col="NAME_1"
):
    admin = admin_file.to_crs("EPSG:4326")
    all_results = []

    for file_name in grib_files:
        print(f"Processing {file_name}")
        xx = ocha_stratus.load_blob_data(blob_name=file_name)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "input.grib"

            tmp_path.write_bytes(xx)

            ds = xr.open_dataset(
                tmp_path,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "typeOfLevel": "surface",
                        "shortName": "tp"
                    }
                }
            )
            ds.load()

        daily_tp = get_hres_daily_precip(ds)
        if daily_tp is not None:
            df = aggregate_to_admin(daily_tp, admin, admin_level_col)

            init_date = Path(file_name).stem.split("_")[-1]
            df["init_date"] = pd.to_datetime(init_date)

            all_results.append(df)
        else:
            print("No data found")

    return pd.concat(all_results, ignore_index=True)

def open_grib_from_bytes(grib_bytes):
    fd, tmp_path = tempfile.mkstemp(suffix=".grib")
    os.close(fd)   # <-- VERY important on Windows

    try:
        with open(tmp_path, "wb") as f:
            f.write(grib_bytes)

        ds = xr.open_dataset(
            tmp_path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "typeOfLevel": "surface",
                    "shortName": "tp"
                }
            }
        )
        ds.load()
        return ds

    finally:
        os.remove(tmp_path)

def normalize_longitude(da):
    lon = da.longitude
    if lon.max() > 180:
        print("Normalizing longitudes from 0–360 → -180–180")
        da = da.assign_coords(
            longitude=(((lon + 180) % 360) - 180)
        ).sortby("longitude")
    return da

if __name__ == "__main__":
    grib_directory = "ds-aa-mmr-cyclones/raw/hres/"
    all_files_name = ocha_stratus.list_container_blobs(grib_directory)
    df = process_hres_folder(
        grib_files=all_files_name,
        admin_file=adm_boundaries,
        admin_level_col="ADM1_EN"
    )

    df = df.rename(columns={
        "step": "lead_time_h"
    })

    df.to_csv("MMR_HRES_daily_rain_ADM1.csv", index=False)

    print("Done ✅")
