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
        filename = f'frames/{algo.replace(".csv", "")}/{index}.png'
        if os.path.exists(filename):
            return filename

        y_range = [points[:, 4].min(), points[:, 4].max()]
        x_range = [points[:, 3].min(), points[:, 3].max()]

        fig = plt.figure(figsize=((x_range[1] - x_range[0]), (y_range[1] - y_range[0])))
        splt = fig.add_subplot()
        splt.imshow(image)
        for point in points:
            splt.plot(point[3], point[4], marker='v', color="red", markersize=40)

        splt.set_ylim(y_range)
        splt.set_xlim(x_range)
        
        splt.plot(((data["path"][0] + 180) / 360) * width, ((90 - data["path"][1]) / 180) * height, color='blue', linewidth=10, linestyle='-')
        splt.plot(((data["shorest_path"][0] + 180) / 360) * width, ((90 - data["shorest_path"][1]) / 180) * height, color='green', linewidth=7, linestyle='-.')
        
        # save frame
        extent = splt.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
        fig.savefig(filename, bbox_inches=extent.expanded(1.0, 1.0), dpi=100)

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
    os.system(f'ffmpeg -framerate 30 -i frames/{algo.replace(".csv", "")}/%d.png -c:v libx264 -r 30 {algo.replace(".csv", "")}.mp4')

    # Remove files
    # for filename in set(filenames):
    #     os.remove(filename)


algos = os.listdir('data/proc')

for algo in tqdm.tqdm(algos):
    render_video(algo)