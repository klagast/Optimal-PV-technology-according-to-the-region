import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
import psycopg2
import pandas as pd
import sqlalchemy
import matplotlib.colors as colors
from matplotlib.patches import Patch

#this file is able to create worldmaps with the best PV technology in the cities
#for the yearly DC energy yield
#for the yearly DC energy yield per square meter
#for the yearly DC energy yield divided by the watt-peak



#make world map for the recommended material for the yearly DC energy yield
#with weight distribution

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

# select all the filtered cities
command_cities = "SELECT cf.city_id, cf.lat, cf.lng FROM cities_filtered cf INNER JOIN map_dc_power_wpeak_2 mdp ON cf.city_id = mdp.city_id ORDER BY city_id"
cursor.execute(command_cities)
#Convert SQL to dataframe
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'latitude', 'longitude'))

for index, row in my_cities.iterrows():
    query = 'SELECT city_id, material_recommended from map_dc_power_wpeak_2 ORDER BY city_id'
    results = pd.read_sql(query, engine)
col_to_add = results['material_recommended']
my_cities = my_cities.join(col_to_add)

# Create a dictionary mapping materials to colors
color_dict = {'c-Si': 'red', 'mc-Si': 'yellow', 'HIT-Si': 'violet',
              'CdTe': 'brown'}
# Use apply method to replace each material with its corresponding color
my_cities['color'] = my_cities['material_recommended'].apply(lambda x: color_dict[x])
# Create a list of patches for each color in the dictionary
patches = [Patch(color=v, label=k) for k, v in color_dict.items()]

# Create a figure and axes object
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(1, 1, 1)

# Create a Basemap object with the Robinson projection
map = Basemap(projection='robin', resolution='c', lon_0=0, ax=ax)

# Draw coastlines, countries, and fill in the oceans
map.drawcoastlines(linewidth=0.5)
map.drawcountries(linewidth=0.5)
map.fillcontinents(color='lightgray', lake_color='white')
map.drawmapboundary(fill_color='white')

# Convert the longitude and latitude coordinates to the map projection
x, y = map(my_cities['longitude'], my_cities['latitude'])

# Plot the cities on the map with colors based on the recommended material
scatter = map.scatter(x, y, c=my_cities['color'], cmap='rainbow', edgecolor='black', linewidths=0.5)

# Add the patches to the legend
# legend = plt.legend(handles=patches, loc='lower left', title='Recommended Material')
legend = ax.legend(handles=patches, loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=6, title='Recommended Material')

# Set the legend title font size
plt.setp(legend.get_title(), fontsize='large')

# Show the map
plt.show()







#make world map for the recommended material for the yearly DC energy yield per square meter
#with weight distribution

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

# select all the filtered cities
command_cities = "SELECT cf.city_id, cf.lat, cf.lng FROM cities_filtered cf INNER JOIN map_dc_power_2 mdp ON cf.city_id = mdp.city_id ORDER BY city_id"
cursor.execute(command_cities)
#Convert SQL to dataframe
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'latitude', 'longitude'))

for index, row in my_cities.iterrows():
    query = 'SELECT city_id, material_recommended from map_dc_power_2 ORDER BY city_id'
    results = pd.read_sql(query, engine)
col_to_add = results['material_recommended']
my_cities = my_cities.join(col_to_add)

# Create a dictionary mapping materials to colors
color_dict = {'c-Si': 'red', 'mc-Si': 'yellow', 'HIT-Si': 'violet',
              'CdTe': 'brown'}
# Use apply method to replace each material with its corresponding color
my_cities['color'] = my_cities['material_recommended'].apply(lambda x: color_dict[x])
# Create a list of patches for each color in the dictionary
patches = [Patch(color=v, label=k) for k, v in color_dict.items()]

# Create a figure and axes object
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(1, 1, 1)

# Create a Basemap object with the Robinson projection
map = Basemap(projection='robin', resolution='c', lon_0=0, ax=ax)

# Draw coastlines, countries, and fill in the oceans
map.drawcoastlines(linewidth=0.5)
map.drawcountries(linewidth=0.5)
map.fillcontinents(color='lightgray', lake_color='white')
map.drawmapboundary(fill_color='white')

# Convert the longitude and latitude coordinates to the map projection
x, y = map(my_cities['longitude'], my_cities['latitude'])

# Plot the cities on the map with colors based on the recommended material
scatter = map.scatter(x, y, c=my_cities['color'], cmap='rainbow', edgecolor='black', linewidths=0.5)

# Add the patches to the legend
# legend = plt.legend(handles=patches, loc='lower left', title='Recommended Material')
legend = ax.legend(handles=patches, loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=6, title='Recommended Material')

# Set the legend title font size
plt.setp(legend.get_title(), fontsize='large')

# Show the map
plt.show()











#make world map for the recommended material for the yearly DC energy yield per watt-peak
#with weight distribution

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

# select all the filtered cities
command_cities = "SELECT cf.city_id, cf.lat, cf.lng FROM cities_filtered cf INNER JOIN map_dc_power_m2_2 mdp ON cf.city_id = mdp.city_id ORDER BY city_id"
cursor.execute(command_cities)
#Convert SQL to dataframe
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'latitude', 'longitude'))

for index, row in my_cities.iterrows():
    query = 'SELECT city_id, material_recommended from map_dc_power_m2_2 ORDER BY city_id'
    results = pd.read_sql(query, engine)
col_to_add = results['material_recommended']
my_cities = my_cities.join(col_to_add)

# Create a dictionary mapping materials to colors
color_dict = {'c-Si': 'red', 'mc-Si': 'yellow', 'HIT-Si': 'violet',
              'CdTe': 'brown'}
# Use apply method to replace each material with its corresponding color
my_cities['color'] = my_cities['material_recommended'].apply(lambda x: color_dict[x])
# Create a list of patches for each color in the dictionary
patches = [Patch(color=v, label=k) for k, v in color_dict.items()]

# Create a figure and axes object
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(1, 1, 1)

# Create a Basemap object with the Robinson projection
map = Basemap(projection='robin', resolution='c', lon_0=0, ax=ax)

# Draw coastlines, countries, and fill in the oceans
map.drawcoastlines(linewidth=0.5)
map.drawcountries(linewidth=0.5)
map.fillcontinents(color='lightgray', lake_color='white')
map.drawmapboundary(fill_color='white')

# Convert the longitude and latitude coordinates to the map projection
x, y = map(my_cities['longitude'], my_cities['latitude'])

# Plot the cities on the map with colors based on the recommended material
scatter = map.scatter(x, y, c=my_cities['color'], cmap='rainbow', edgecolor='black', linewidths=0.5)

# Add the patches to the legend
# legend = plt.legend(handles=patches, loc='lower left', title='Recommended Material')
legend = ax.legend(handles=patches, loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=6, title='Recommended Material')

# Set the legend title font size
plt.setp(legend.get_title(), fontsize='large')

# Show the map
plt.show()



























#make world map for the recommended material for all the three forms of weight distribution
#the ranked 1 material
#change yourself the tables to have all the three energy yields

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

# select all the filtered cities
command_cities = "SELECT cf.city_id, cf.lat, cf.lng FROM cities_filtered cf INNER JOIN map_dc_power_wpeak_2 mdp ON cf.city_id = mdp.city_id ORDER BY city_id"
cursor.execute(command_cities)
#Convert SQL to dataframe
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'latitude', 'longitude'))

results = pd.DataFrame(columns=['cities_id', 'material_dc_wpeak', 'dc_power_wpeak'])
for index, row in my_cities.iterrows():
    query = 'SELECT top10panelscities_2.cities_id, top10panelscities_2.material_dc_wpeak, top10panelscities_2.dc_power_wpeak \
             FROM top10panelscities_2 \
             INNER JOIN ( \
               SELECT cities_id, MAX(dc_power_wpeak) AS max_dc_power_wpeak \
               FROM top10panelscities_2 \
               GROUP BY cities_id \
             ) AS max_dc_power_wpeak \
             ON top10panelscities_2.cities_id = max_dc_power_wpeak.cities_id \
             AND top10panelscities_2.dc_power_wpeak = max_dc_power_wpeak.max_dc_power_wpeak \
             WHERE top10panelscities_2.cities_id = %s \
             ORDER BY top10panelscities_2.cities_id'
    city_results = pd.read_sql(query, engine, params=[row['city_id']])
    results = results.append(city_results, ignore_index=True)
col_to_add = results['material_dc_wpeak']
my_cities = my_cities.join(col_to_add)

# Create a dictionary mapping materials to colors
color_dict = {'c-Si': 'red', 'mc-Si': 'yellow', 'HIT-Si': 'violet',
              'CdTe': 'brown'}
# Use apply method to replace each material with its corresponding color
my_cities['color'] = my_cities['material_dc_wpeak'].apply(lambda x: color_dict[x])
# Create a list of patches for each color in the dictionary
patches = [Patch(color=v, label=k) for k, v in color_dict.items()]

# Create a figure and axes object
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(1, 1, 1)

# Create a Basemap object with the Robinson projection
map = Basemap(projection='robin', resolution='c', lon_0=0, ax=ax)

# Draw coastlines, countries, and fill in the oceans
map.drawcoastlines(linewidth=0.5)
map.drawcountries(linewidth=0.5)
map.fillcontinents(color='lightgray', lake_color='white')
map.drawmapboundary(fill_color='white')

# Convert the longitude and latitude coordinates to the map projection
x, y = map(my_cities['longitude'], my_cities['latitude'])

# Plot the cities on the map with colors based on the recommended material
scatter = map.scatter(x, y, c=my_cities['color'], cmap='rainbow', edgecolor='black', linewidths=0.5)

# Add the patches to the legend
legend = ax.legend(handles=patches, loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=6, title='Recommended Material')

# Set the legend title font size
plt.setp(legend.get_title(), fontsize='large')

# Show the map
plt.show()
