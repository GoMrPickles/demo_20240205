"""
This script generates 10 data files containing normally distributed data.
The data are centered on a value of 100, with some noise added.
10 files are written to a stated directory whenever the script is run.
"""
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

# print("Hello, world!")

# multiplier = 3*np.random.default_rng().uniform(low=-1, high=1)
# data = np.random.normal(loc=100.0, scale=1.0, size=30) \
#     + 3*np.random.default_rng().uniform(low=-1, high=1)

df_dicts = {}
for n in range(10):
    # add a flag to track outliers to compare accuracy later
    data = (np.random.normal(loc=100.0, scale=1.0, size=30) \
            + 3*np.random.default_rng().uniform(low=-1, high=1))
    df = pd.DataFrame(data)
    data_timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S.%f")[:-3]
    df.to_csv(Path(rf"~/Dropbox/Programming/tesla_demo/data/{n:03d}_{data_timestamp}.csv"))
