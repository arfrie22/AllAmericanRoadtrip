import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def generate_data(algo):
    df = pd.read_parquet(f'data/unproc/{algo}')

    df.to_parquet(f'data/proc/{algo}')


algos = os.listdir('data/unproc')
os.makedirs('data/proc', exist_ok=True)

with tqdm.tqdm(total=len(algos)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(generate_data, algo) for algo in algos]
            for future in as_completed(futures):
                pbar.update(1)

    


    
    