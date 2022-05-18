import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

stores = pd.read_parquet(f'data/stores.csv')

def generate_data(algo):
    df = pd.read_parquet(f'data/unproc/{algo}')
    shorest_path_length = 0
    shorest_path = None
    i = 0

    data = []
    for path in tqdm.tqdm(df.to_numpy()):
        x = []
        y = []

        for store in path[0]:
            x.append(stores[stores['store_id'] == store].iloc[0, :]['long'])
            y.append(stores[stores['store_id'] == store].iloc[0, :]['lat'])
    
        if path[1] < shorest_path_length or shorest_path == None:
                shorest_path = [x, y]
                shorest_path_length = path[1]

        data.append([[x,y], path[1], shorest_path, shorest_path_length])

    out = pd.DataFrame(data, columns = ['path', 'length', 'shorest_path', 'shorest_legnth'])
    out.to_parquet(f'data/proc/{algo}')


algos = os.listdir('data/unproc')
os.makedirs('data/proc', exist_ok=True)

with tqdm.tqdm(total=len(algos)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(generate_data, algo) for algo in algos]
            for future in as_completed(futures):
                pbar.update(1)

    


    
    