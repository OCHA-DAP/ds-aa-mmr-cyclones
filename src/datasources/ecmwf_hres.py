import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

import ocha_stratus
load_dotenv(override=True)

from ecmwfapi import ECMWFService

bbox = [92, 9, 101, 28]  # lon_min, lat_min, lon_max, lat_max
dates_pickle = pd.read_pickle(r'src/data/storms_date.pickle')
unique_dates = sorted({
    datetime.strptime(date, "%Y-%m-%d")
    for dates in dates_pickle.values()
    for date in dates
})

server = ECMWFService("mars")

for single_date in unique_dates:
    date_str = single_date.strftime("%Y%m%d")  # ✅ IMPORTANT

    print(f"Downloading {date_str}")
    req={
        "class": "od",
        "stream": "oper",
        "grid": "0.1/0.1",
        "type": "fc",
        "expver": "1",
        "date": date_str,
        "time": "00/12",
        "step": "24/48/72/96/120",  # daily leads up to 5 days
        "area": f"{bbox[3]}/{bbox[0]}/{bbox[1]}/{bbox[2]}",
        "levtype": "sfc",
        "param": "tp",
    }
    server.execute(req, f"grib/MMR_hres_{date_str}.grib")
    print("Saved date locally")
    #ocha_stratus.upload_blob_data(
    #    f"MMR_hres_{date_str}.grib", stage="dev", blob_name="ds-aa-mmr-cyclones/raw/hres/")
    #print("Saved date on blob")
    #os. remove(f"MMR_hres_{date_str}.grib")
    #print("Deleted data locally")
