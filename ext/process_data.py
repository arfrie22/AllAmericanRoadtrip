import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def generate_data(store):
    df = pd.read_parquet(f'data/unproc/{store}')
    
    
    return [stores_used, length]


algos = os.listdir('data/unproc')
os.makedirs('data/proc', exist_ok=True)

with tqdm.tqdm(total=len(stores)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(generate_data, algo) for algo in algos]
            for future in as_completed(futures):
                paths.append(future.result())
                pbar.update(1)

    


df = pd.DataFrame(paths, columns = ['path', 'length'])
df.to_parquet('data/unproc/greedy.csv')
    
    