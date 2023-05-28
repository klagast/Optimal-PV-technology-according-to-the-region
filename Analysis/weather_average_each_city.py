import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np

#file to add the average weather data to a new database to start the cluster analysis


#establishing the connection
conn = psycopg2.connect(database='postgres', user='postgres', password='Pluisje1', host='127.0.0.1', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

command_cities = "SELECT cf.city_id, cf.lat, cf.lng FROM cities_filtered cf WHERE cf.city_id IN (SELECT tc.cities_id FROM top10panelscities tc) ORDER by cf.city_id"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy

for index in arr_cities:
    city_id = int(index[0])
    lat = float(index[1])
    lng = float(index[2])
    #select from PostgreSQL database
    query = "SELECT temp_air, relative_humidity, ghi, wind_speed FROM tmy_cities WHERE city_id = %s ORDER BY city_id"
    weather = pd.read_sql(query, engine, params=[city_id])
    

    #calculate the variables
    ghi_sum = weather['ghi'].sum()
    mean_ws = weather['wind_speed'].mean()
    max_tamb = weather['temp_air'].max()
    min_tamb = weather['temp_air'].min()
    mean_tamb = weather['temp_air'].mean()
    mean_hum = weather['relative_humidity'].mean()


    query_insert = 'INSERT INTO weather_average_each_city(city_id, sum_ghi, mean_ws, max_tamb, min_tamb, mean_tamb, mean_hum, lat, lng) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data = (city_id, ghi_sum, mean_ws, max_tamb, min_tamb, mean_tamb, mean_hum, lat, lng)
    cursor.execute(query_insert, data)
    conn.commit()
   