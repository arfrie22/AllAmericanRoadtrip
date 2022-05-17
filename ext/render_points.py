import tqdm
import sqlite3
import pandas as pd
import pyarrow
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

stores = pd.read_parquet(f'data/stores.csv')

def generate_frame(data):
        lines = []
        
        x = []
        y = []
        
        for store in path[0]:
            x.append(stores[stores['store_id'] == store]['x'])
            y.append(stores[stores['store_id'] == store]['y'])
        
        if path[1] < shorest_path_length or shorest_path == None:
                shorest_path = [x, y]
                shorest_path_length = path[1]

        lines.extend(
            plt.plot(x, y, color = 'blue', linewidth=3, linestyle='-.')
        )

        lines.extend(
            plt.plot(shorest_path[0], shorest_path[1], color = 'green', linewidth=1, linestyle='-.')
        )

        # create file name and append it to a list
        filename = f'frames/{i}.png'
        filenames.append(filename)
        i += 1
        
        # save frame
        plt.savefig(filename, dpi=100)

        for line in lines:
            line.remove()


def render_video(algo):
    paths = pd.read_parquet('data/unproc/greedy.csv')

    os.makedirs('frames', exist_ok=True)  
    data = mpl.image.imread('data/world.200412.3x5400x2700.jpg')
    # PIL.Image.MAX_IMAGE_PIXELS = None
    # data = image.imread('data/Full Map.jpg')
    height = len(data)
    width = len(data[0])

    plt.figure(figsize=(width/100, height/100))

    stores['x'] = ((stores.to_numpy()[:,2] + 180) / 360) * width
    stores['y'] = ((90 - stores.to_numpy()[:,1]) / 180) * height 
    points = np.array(stores.to_numpy())

    for point in points:
        plt.plot(point[3], point[4], marker='v', color="red")

    plt.imshow(data)
    plt.ylim([points[:, 4].min(), points[:, 4].max()])
    plt.xlim([points[:, 3].min(), points[:, 3].max()])

    filenames = []
    shorest_path_length = 0
    shorest_path = None
    i = 0
    with tqdm.tqdm(total=len(algos)) as pbar:
        # with ThreadPoolExecutor(max_workers=len(stores)) as ex:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(generate_data, algo) for algo in algos]
            for future in as_completed(futures):
                pbar.update(1)
    for path in tqdm.tqdm(paths.to_numpy()[0:20]):
        
    # build video
    os.system("ffmpeg -framerate 30 -i frames/%d.png -c:v libx264 -r 30 output.mp4")

    # Remove files
    for filename in set(filenames):
        os.remove(filename)


algos = os.listdir('data/proc')

for algo in tqdm.tqdm(algos):
    render_video(algo)