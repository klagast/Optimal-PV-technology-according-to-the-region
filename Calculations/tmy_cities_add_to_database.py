import psycopg2
import numpy as np
import pvlib
import time

#file for adding TMY to the table tmy_city


#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

command_cities = "SELECT city, id_city, lat, lng FROM cities ORDER BY id_city"
cursor.execute(command_cities)
#Convert PostgreSQL to numpy
arr_cities = np.array(cursor.fetchall())
print(arr_cities)

for index1 in arr_cities:
    city_name = index1[0]
    city_id = index1[1]
    print(city_id)
    lati = index1[2]
    longi = index1[3]
    try:
        weather = pvlib.iotools.get_pvgis_tmy(lati, longi, map_variables=True,  url='https://re.jrc.ec.europa.eu/api/v5_2/')[0]
    except:
        print('Issue looking up', city_name)
        continue
    for index2 in weather.index: 
        time_utc = str(index2)
        temp_air = weather['temp_air'][index2]
        rh = weather['relative_humidity'][index2]
        ghi = weather['ghi'][index2]
        dni = weather['dni'][index2]
        dhi = weather['dhi'][index2]
        ir_h = weather['IR(h)'][index2]
        wind_speed = weather['wind_speed'][index2]
        wind_direction = weather['wind_direction'][index2]
        pressure = weather['pressure'][index2]
        command_insert_weather = "INSERT INTO tmy_cities(city_name, city_id, time_utc, temp_air, relative_humidity, ghi, dni, dhi, ir_h, wind_speed, wind_direction, pressure) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data = (city_name, city_id, time_utc, temp_air, rh, ghi, dni, dhi, ir_h, wind_speed, wind_direction, pressure)
        
        #start = time.time()
        cursor.execute(command_insert_weather, data)
        conn.commit()
        #end = time.time()
        #print(f'db took {end-start}')
    #time.sleep(1)  

 
#Closing the connection
conn.close()