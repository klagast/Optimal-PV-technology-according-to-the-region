import numpy as np
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
import psycopg2

#simple code to plot a world map of the 3596 used cities in this study

#establishing the connection
conn = psycopg2.connect(database='postgres', user='postgres', password='Pluisje1', host='127.0.0.1', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#select all the filtered cities with weather data, 3596 cities
command_cities = "SELECT DISTINCT city_id, lat, lng FROM cities_filtered cf JOIN dc_power_results dpr ON cf.city_id = dpr.cities_id GROUP BY cf.city_id;"
cursor.execute(command_cities)
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'latitude', 'longitude'))
print(my_cities)


geometry = [Point(xy) for xy in zip(my_cities['longitude'], my_cities['latitude'])]
gdf = GeoDataFrame(my_cities, geometry=geometry)   

#this is a simple map that goes with geopandas
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
fig = gdf.plot(ax=world.plot(figsize=(10, 6)), marker='o', color='red', markersize=0.05)
plt.xlabel('Longitude (in degrees)')
plt.ylabel('Latitude (in degrees)')




