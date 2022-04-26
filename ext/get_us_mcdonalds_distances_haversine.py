import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os

def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
    radius = 6371; # Radius of the earth in km
    delta_lat = deg2rad(lat2 - lat1)
    delta_lon = deg2rad(lon2 - lon1)
    a = math.sin(delta_lat/2) * math.sin(delta_lat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(delta_lon/2) * math.sin(delta_lon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c; # Distance in km
    return d

def deg2rad(deg):
  return deg * (math.pi/180)


con = sqlite3.connect('data/mc_donalds.db')
cur = con.cursor()

stores = []

cur.execute('select * from us')


for result in cur:
    stores.append([
        result[0],
        result[2],
        result[1] 
    ])

os.makedirs("data/haversine", exist_ok=True)

for store_from in tqdm.tqdm(stores):
    data = []
    # csvfile = open(f'data/haversine/{store_from[0]}.csv', 'w')
    for store_to in stores:
        data.append([store_from[0], store_to[0], getDistanceFromLatLonInKm(store_from[1], store_from[2], store_to[1], store_to[2])])
    
    
    df = pd.DataFrame(data, columns = ['store_from', 'store_to', 'distance'])
    df.to_parquet(f'data/haversine/{store_from[0]}.csv')
    