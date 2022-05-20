import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

ITERATIONS = 1000
ANTS = 100
DISTANCE_POWER = 4
PHEROMONE_POWER = 1
PHEROMONE_INTENSITY = 3
PHEROMONE_INTENSITY_POWER = 10
INITIAL_PHEROMONE_INTENSITY = 1
EVAPORATION_RATE = 0.3


def simulate_ant(ant):
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
    length = ant+1
    path = [1, 2, 3, 45]
    return ant, length, path
    # return ""

distances = pd.read_parquet('data/haversine.csv')
stores = pd.read_parquet('data/stores.csv').to_numpy()[:, 0]

pheromone_trails = np.ones((len(stores), len(stores))) * INITIAL_PHEROMONE_INTENSITY

with Progress() as progress:
    iterations_bar = progress.add_task("[red]Iterations", total=ITERATIONS)
    ants_bar = progress.add_task("[green]Ants", total=ANTS)
    for i in range(ITERATIONS):
        paths = pd.DataFrame(index=np.arange(ANTS), columns=['length', 'path'])
        # with ThreadPoolExecutor(max_workers=ANTS) as ex:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(simulate_ant, ant) for ant in range(ANTS)]
            for future in as_completed(futures):
                paths.iloc[future.result()[0]]['length'] = future.result()[1]
                paths.iloc[future.result()[0]]['path'] = future.result()[2]
                progress.update(ants_bar, advance=1)
        progress.reset(ants_bar)
        progress.update(iterations_bar, advance=1)
        pheromone_trails -= EVAPORATION_RATE
        shortest_path = paths.to_numpy()[:, 0].min()
        for index, path in paths.iterrows():
            strength = pow((shortest_path / path["length"]), PHEROMONE_INTENSITY_POWER) * PHEROMONE_INTENSITY
            for node in range(len(path) - 1):
                pheromone_trails[path[node], path[node+1]] += strength
                pheromone_trails[path[node+1], path[node]] += strength

            pheromone_trails[path[0], path[-1]] += strength
            pheromone_trails[path[-1], path[0]] += strength


    

os.makedirs('data/unproc', exist_ok=True)
df = pd.DataFrame(paths, columns = ['path', 'length'])
df.to_parquet('data/unproc/greedy.csv')
    
    