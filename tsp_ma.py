import sqlite3
import matplotlib as mpl
from matplotlib import pyplot as plt
import numpy as np
import PIL
import imageio
import os
import tqdm

os.makedirs('frames', exist_ok=True)  
data = mpl.image.imread('data/world.200412.3x5400x2700.jpg')
# PIL.Image.MAX_IMAGE_PIXELS = None
# data = image.imread('data/Full Map.jpg')
height = len(data)
width = len(data[0])

con = sqlite3.connect('data/mc_donalds.db')
cur = con.cursor()
cur.execute('select * from us where sub_division="MA"')
plt.figure(figsize=(width/100, height/100))

points = []

for result in cur:
    points.append([
        result[0],
        ((result[2] + 180) / 360) * width,
        ((90 - result[1]) / 180) * height 
    ])

points = np.array(points)

for point in points:
    plt.plot(point[1], point[2], marker='v', color="red")

plt.imshow(data)
plt.ylim([points[:, 2].min(), points[:, 2].max()])
plt.xlim([points[:, 1].min(), points[:, 1].max()])

filenames = []
for i in tqdm.tqdm(range(0, 360, 10)):
    lines = []
    
    y = np.random.randint(low=points[:, 2].min(), high=points[:, 2].max(), size=25)
    x = np.random.randint(low=points[:, 1].min(), high=points[:, 1].max(), size=25)
    
    lines.extend(
        plt.plot(x, y, color = 'blue', linewidth=3, linestyle='-.')
    )

    # create file name and append it to a list
    filename = f'frames/{i}.png'
    filenames.append(filename)
    
    # save frame
    plt.savefig(filename, dpi=100)

    for line in lines:
        line.remove()
# build gif
with imageio.get_writer('mygif.gif', mode='I') as writer:
    for filename in filenames:
        image = imageio.imread(filename)
        writer.append_data(image)
        
# Remove files
for filename in set(filenames):
    os.remove(filename)