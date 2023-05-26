import psycopg2
import numpy as np
import function_that_returns_energy_yield_for_nmpy
import time
import sqlalchemy
import pandas as pd


#This is the file to calculate the 3 forms of the energy yield for every panel, for every city


#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()


#select from PostgreSQL database
command_cities = "SELECT city_id, lat, lng, altitude, opt_tilt_angle FROM cities_filtered WHERE city_id > 10300 ORDER BY city_id"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy
command_modules = "SELECT id_modules, name FROM modules ORDER BY id_modules"
cursor.execute(command_modules)
arr_modules = np.array(cursor.fetchall())
#command to check if weather data exists, select only first 5 rows
command_weather = "SELECT time_utc, temp_air, relative_humidity, ghi, dni, dhi, ir_h, wind_speed, wind_direction, pressure FROM tmy_cities WHERE city_id = %s LIMIT 5"

#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

for index1 in arr_cities:
    start = time.time()
    city_id = int(index1[0])
    lati = index1[1]
    longi = index1[2]
    alti = index1[3]
    opt_tilt_angle = index1[4]
    #check if weather data exist for the city
    weather = pd.read_sql(command_weather, engine, params=[city_id])
    weather = weather.set_index('time_utc')
    weather.index.name = "time(UTC)"
    row_count, column_count = weather.shape
    try: 
        if row_count == 5: 
            for index2 in arr_modules:
                mod_id = index2[0]
                mod_name = index2[1]

                #number of records to check if weather data exists, sql limited
                dc_power , dc_power_m2, dc_power_wpeak = function_that_returns_energy_yield_for_nmpy.energy_yield(city_id, longi, lati, alti, mod_name, opt_tilt_angle)
                command_DC_power = "INSERT INTO dc_results(cities_id, id_modules, dc_power, dc_power_m2, dc_power_wpeak) VALUES (%s, %s, %s, %s, %s)"
                data = (city_id, mod_id, dc_power, dc_power_m2, dc_power_wpeak)
                # print(city_id, mod_id, dc_power)
                cursor.execute(command_DC_power, data)
                conn.commit()
    except psycopg2.errors.UniqueViolation:
        print(f'Skipped duplicate key for city_id {city_id}')
        break

    print(f'done for {index1}')
    end = time.time()
    elapsed = end - start
    print("Elapsed time: ", elapsed, "seconds")

#Commiting the code
conn.commit() 
#Closing the connection
conn.close()

