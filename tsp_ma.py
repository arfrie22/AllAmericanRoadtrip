import sqlite3
from matplotlib import image
from matplotlib import pyplot as plt
import numpy as np
import PIL.Image as Image
  
data = image.imread('data/world.200412.3x5400x2700.jpg')
# Image.MAX_IMAGE_PIXELS = None
# data = image.imread('data/Full Map.jpg')
height = len(data)
width = len(data[0])

con = sqlite3.connect('data/mc_donalds.db')
cur = con.cursor()
cur.execute('select * from us where sub_division="MA"')

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
plt.show()