import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os


distances = pd.read_parquet('data/haversine.csv')    
stores = pd.read_parquet('data/stores.csv')

paths = []
for store in tqdm.tqdm(stores.to_numpy()[:,0]):
    length = 0
    stores_used = []
    last_store = store
    stores_used.append(last_store)
    while len(stores_used) < len(stores):
        targets = distances.loc[(distances['store_from'] == last_store) & (~distances['store_to'].isin(stores_used))].sort_values('distance')
        last_store = targets.iloc[0]['store_to']
        stores_used.append(last_store)
        length += targets.iloc[0]['distance']
    paths.append([stores_used, length])

df = pd.DataFrame(paths, columns = ['path', 'length'])
df.to_parquet('data/stores.csv')
    
    