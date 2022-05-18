import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

stores = pd.read_parquet(f'data/stores.csv')
image = mpl.image.imread('data/world.200412.3x5400x2700.jpg')
# PIL.Image.MAX_IMAGE_PIXELS = None
# data = image.imread('data/Full Map.jpg')
height = len(image)
width = len(image[0])
mpl.use('agg')

stores['x'] = ((stores.to_numpy()[:,2] + 180) / 360) * width
stores['y'] = ((90 - stores.to_numpy()[:,1]) / 180) * height 
points = np.array(stores.to_numpy())


def generate_frame(data, algo, index):
        fig = plt.figure(figsize=(width/100, height/100))
        splt = fig.add_subplot()
        splt.imshow(image)
        for point in points:
            splt.plot(point[3], point[4], marker='v', color="red")

        splt.set_ylim([points[:, 4].min(), points[:, 4].max()])
        splt.set_xlim([points[:, 3].min(), points[:, 3].max()])
        
        splt.plot(((data["path"][0] + 180) / 360) * width, ((90 - data["path"][1]) / 180) * height, color = 'blue', linewidth=3, linestyle='-.')
        splt.plot(((data["shorest_path"][0] + 180) / 360) * width, ((90 - data["shorest_path"][1]) / 180) * height, color = 'green', linewidth=1, linestyle='-.')

        # create file name and append it to a list
        filename = f'frames/{algo.replace(".csv", "")}/{index}.png'
        
        # save frame
        fig.savefig(filename, dpi=100)
        plt.close(fig)
        return filename


def render_video(algo):
    paths = pd.read_parquet(f'data/proc/{algo}')

    os.makedirs(f'frames/{algo.replace(".csv", "")}', exist_ok=True)  

    

    filenames = []
    with tqdm.tqdm(total=len(paths.index)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(generate_frame, data, algo, index) for index, data in paths.iterrows()]
            for future in as_completed(futures):
                filenames.append(future.result())
                pbar.update(1)
    # for path in tqdm.tqdm(paths.to_numpy()[0:20]):
        
    # build video
    os.system(f'ffmpeg -framerate 30 -i frames/{algo.replace(".csv", "")}/%d.png -c:v libx264 -r 30 output.mp4')

    # Remove files
    for filename in set(filenames):
        os.remove(filename)


algos = os.listdir('data/proc')

for algo in tqdm.tqdm(algos):
    render_video(algo)