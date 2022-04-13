import csv
import json
import requests
import time
import tqdm
import sqlite3

centroids_f = open("../data/us_centroids_50mile_radius.csv")
centroids = []


def get_if_exists(dictionary, key, default_value=None):
    value = default_value
    if key in dictionary.keys():
        value = dictionary[key]
    return value


csv_reader = csv.reader(centroids_f, delimiter=',')
line_count = 0
for row in csv_reader:
    if line_count > 0:
        centroids.append([row[1], row[2]])
    line_count += 1

con = sqlite3.connect('../data/mc_donalds.db')
cur = con.cursor()
cur.execute('''
create table if not exists us (
	identifier_value int not null
		constraint us_pk
			primary key,
	latitude double not null,
	longitude double not null,
	short_description text,
	long_description text,
	address_line_1 text,
	address_line_2 text,
	address_line_3 text,
	address_line_4 text,
	sub_division text,
	postcode text,
	custom_address text,
	telephone int
);
''')

cur.execute('''
create unique index if not exists us_identifier_value_uindex
	on us (identifier_value);
''')

stores = []

start_time = time.time()
for centroid in tqdm.tqdm(centroids):

    lat = centroid[0]
    lng = centroid[1]
    url = f'https://www.mcdonalds.com/googleappsv2/geolocation?latitude={lat}&longitude={lng}&radius=100&maxResults=250&country=us&language=en-us'

    r = requests.get(url)

    if r.text.startswith("{"):
        mcdonalds = r.json()["features"]
        for mcdonald in mcdonalds:
            stores.append((mcdonald["properties"]["identifierValue"], mcdonald["geometry"]["coordinates"][1],
                           mcdonald["geometry"]["coordinates"][0],
                           get_if_exists(mcdonald["properties"], "shortDescription"),
                           get_if_exists(mcdonald["properties"], "longDescription"),
                           get_if_exists(mcdonald["properties"], "addressLine1"),
                           get_if_exists(mcdonald["properties"], "addressLine2"),
                           get_if_exists(mcdonald["properties"], "addressLine3"),
                           get_if_exists(mcdonald["properties"], "addressLine4"),
                           get_if_exists(mcdonald["properties"], "subDivision"),
                           get_if_exists(mcdonald["properties"], "postcode"),
                           get_if_exists(mcdonald["properties"], "customAddress"),
                           get_if_exists(mcdonald["properties"], "telephone")))

            if len(stores) > 30:
                cur.executemany("insert or ignore into us values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stores)
                con.commit()
                stores = []
    else:
        print(centroid)

    while time.time() < start_time + 0.05:
        pass
    start_time = time.time()

con.commit()
con.close()
# https://www.mcdonalds.com/googleappsv2/geolocation?latitude=42.5747973841&longitude=-70.2223724978&radius=100&maxResults=250&country=us&language=en-us
