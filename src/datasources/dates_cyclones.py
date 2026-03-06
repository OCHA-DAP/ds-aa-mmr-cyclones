import pandas as pd
import pickle
from dotenv import load_dotenv
# Load .env file into environment variables
load_dotenv()

from src.utils.utils_fun import *
from src.utils.utils_plot import *
from src.utils.constants import *

pcode = ISO3.upper()

meta = make_run_metadata(level=adm_level, areas= ADM_LIST)
suff = make_suffix(meta=meta)
df = pd.read_csv(f"results/df_full_{suff}.csv")

df_0 = df[df['t_delta'] == 0]
df_unique = df_0[["storm_name", "valid_time"]]

# ensure valid_time is datetime
df_unique["valid_time"] = pd.to_datetime(df_unique["valid_time"])

# function to generate the list of 16 dates
def generate_dates(center_date):
    # generate dates from -10 to +5 days
    dates = [center_date + pd.Timedelta(days=i) for i in range(-10, 6)]
    # convert to yyyy-mm-dd strings
    return [d.strftime("%Y-%m-%d") for d in dates]


storm_dict = (
    df_unique
    .groupby("storm_name")["valid_time"]
    .apply(lambda s: generate_dates(s.iloc[0]))  # use first valid_time if multiple rows
    .to_dict()
)

# Store data (serialize)
with open('../data/storms_date.pickle', 'wb') as handle:
    pickle.dump(storm_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)