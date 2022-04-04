import sqlite3
from matplotlib import image
from matplotlib import pyplot as plt
  
data = image.imread('data/world.200412.3x5400x2700.jpg')
con = sqlite3.connect('data/mc_donalds.db')
cur = con.cursor()
cur.execute('select * from us')
for result in cur:
    x = ((result[2] + 180) / 360) * 5400
    y = ((90 - result[1]) / 180) * 2700
    plt.plot(x, y, marker='v', color="red")

plt.imshow(data)
plt.show()