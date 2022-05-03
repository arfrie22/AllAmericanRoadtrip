import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os

con = sqlite3.connect('data/mc_donalds.db')
cur = con.cursor()

stores = []

cur.execute("SELECT * FROM 'us' WHERE sub_division IN ('ME', 'NH', 'VT', 'MA', 'CT', 'RI');")


stores = []

for result in cur:
    stores.append([
        result[0],
        result[2],
        result[1] 
    ])
 
df = pd.DataFrame(stores, columns = ['store_id', 'lat', 'long'])
df.to_parquet('data/stores.csv')
    