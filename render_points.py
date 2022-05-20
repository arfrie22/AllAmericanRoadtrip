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
fps = 15

stores['x'] = ((stores.to_numpy()[:,2] + 180) / 360) * width
stores['y'] = ((90 - stores.to_numpy()[:,1]) / 180) * height 
points = np.array(stores.to_numpy())


def generate_frame(data, algo, index, final=False):
        filename = f'frames/{algo.replace(".csv", "")}/{index}.png'
        if os.path.exists(filename):
            return filename

        y_range = [points[:, 4].min() - 10, points[:, 4].max() + 10]
        x_range = [points[:, 3].min() - 10, points[:, 3].max() + 10]

        fig = plt.figure(figsize=((x_range[1] - x_range[0]), (y_range[1] - y_range[0])))
        splt = fig.add_subplot()
        splt.imshow(image, origin='upper')
        for point in points:
            splt.plot(point[3], point[4], marker='v', color="red", markersize=40)

        splt.set_ylim(np.flip(y_range, 0))
        splt.set_xlim(x_range)

        xy = (x_range[1] - 1, y_range[1] - 1)

        s_path = splt.plot(((data["shorest_path"][0] + 180) / 360) * width, ((90 - data["shorest_path"][1]) / 180) * height, color='green', linewidth=7, linestyle='-.', label='Shortest Path')[0]
        if not final:
            c_path = \
            splt.plot(((data["path"][0] + 180) / 360) * width, ((90 - data["path"][1]) / 180) * height, color='blue',
                      linewidth=10, linestyle='-', label='Current Path')[0]
            splt.legend(handles=[c_path, s_path], loc='upper left', fontsize=90)
            splt.annotate(f'Iteration: {index}\n Current Path Length: {round(data["length"], 3)} km\n Shortest Path Length: {round(data["shorest_legnth"], 3)} km', xy=xy, xycoords="data", fontsize=90,
                      va="bottom", ha="right",
                      bbox=dict(boxstyle="round", alpha=0.8, fc="w"))
        else:
            splt.legend(handles=[s_path], loc='upper left', fontsize=90)
            splt.annotate(
                f'Shortest Path Length: {round(data["shorest_legnth"], 3)} km',
                xy=xy, xycoords="data", fontsize=90,
                va="bottom", ha="right",
                bbox=dict(boxstyle="round", alpha=0.8, fc="w"))
        # save frame
        extent = splt.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
        fig.savefig(filename, bbox_inches=extent.expanded(1.0, 1.0), dpi=30)

        plt.close(fig)
        return filename


def render_video(algo):
    paths = pd.read_parquet(f'data/proc/{algo}')

    os.makedirs(f'frames/{algo.replace(".csv", "")}', exist_ok=True)



    filenames = []
    with tqdm.tqdm(total=len(paths.index)) as pbar:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(generate_frame, data, algo, index) for index, data in paths.iterrows()]
            for future in as_completed(futures):
                filenames.append(future.result())
                pbar.update(1)
    filenames.append(generate_frame(paths.iloc[-1, :], algo, len(paths.index), final=True))
    # build video
    os.makedirs('videos', exist_ok=True)
    os.system(f'ffmpeg -y -framerate {fps} -i frames/{algo.replace(".csv", "")}/%d.png -c:v libx264 -r {fps} videos/{algo.replace(".csv", "")}.mp4')

    # Remove files
    # for filename in set(filenames):
    #     os.remove(filename)


algos = os.listdir('data/proc')

for algo in tqdm.tqdm(algos):
    render_video(algo)
