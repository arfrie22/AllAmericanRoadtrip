import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed

ITERATIONS = 1000
ANTS = 10000


def calculate_length(store):
    # length = 0
    # stores_used = []
    # last_store = store
    # stores_used.append(last_store)
    # while len(stores_used) < len(stores):
    #     targets = distances.loc[(distances['store_from'] == last_store) & (~distances['store_to'].isin(stores_used))].sort_values('distance')
    #     last_store = targets.iloc[0]['store_to']
    #     stores_used.append(last_store)
    #     length += targets.iloc[0]['distance']
    # return [stores_used, length]
    return ""

distances = pd.read_parquet('data/haversine.csv')
stores = pd.read_parquet('data/stores.csv').to_numpy()[:,0]

paths = []

with Progress() as progress:
    iterations_bar = progress.add_task("[red]Iterations", total=ITERATIONS)
    ants_bar = progress.add_task("[green]Ants", total=ANTS)
    for i in range(ITERATIONS):
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=ANTS) as ex:
            futures = [ex.submit(calculate_length, store) for store in stores]
            for future in as_completed(futures):
                paths.append(future.result())
                progress.update(ants_bar, advance=1)
        progress.reset(ants_bar)
        progress.update(iterations_bar, advance=1)

    

os.makedirs('data/unproc', exist_ok=True)
df = pd.DataFrame(paths, columns = ['path', 'length'])
df.to_parquet('data/unproc/greedy.csv')
    
    