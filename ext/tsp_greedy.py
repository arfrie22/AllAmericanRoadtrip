import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os


distances = pd.read_parquet('data/haversine.csv')    
stores = pd.read_parquet('data/stores.csv')

lengths = []
for store in stores.iterrows():
    stores_used = []
    last_store = store[1]['store_id']
    while len(stores_used < len(stores)):
        targets = distances.loc[distances['store_from'] == last_store].sort_values('distance')
    
    exit(0)