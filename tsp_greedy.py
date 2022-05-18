import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def calculate_length(store):
    length = 0
    stores_used = []
    last_store = store
    stores_used.append(last_store)
    while len(stores_used) < len(stores):
        targets = distances.loc[(distances['store_from'] == last_store) & (~distances['store_to'].isin(stores_used))].sort_values('distance')
        last_store = targets.iloc[0]['store_to']
        stores_used.append(last_store)
        length += targets.iloc[0]['distance']
    return [stores_used, length]

distances = pd.read_parquet('data/haversine.csv')
stores = pd.read_parquet('data/stores.csv').to_numpy()[:,0]

paths = []

with tqdm.tqdm(total=len(stores)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(calculate_length, store) for store in stores]
            for future in as_completed(futures):
                paths.append(future.result())
                pbar.update(1)

    

os.makedirs('data/unproc', exist_ok=True)
df = pd.DataFrame(paths, columns = ['path', 'length'])
df.to_parquet('data/unproc/greedy.csv')
    
    