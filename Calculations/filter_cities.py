#imports
import psycopg2
import numpy as np
import function_haversine_formula

# file to filter the cities from 42432 to 3596 cities

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

command_cities = "SELECT id_city, city, lat, lng, country, population, altitude, opt_tilt_angle FROM cities ORDER BY id_city"
cursor.execute(command_cities)

#Convert PostgreSQL to numpy
arr_cities = np.array(cursor.fetchall())

#Create a new array to hold filtered cities, cities that are not to close to another city
filtered_cities = []
max_distance = 200
max_alt_diff = 100

#filtering 
#7minutes normally, reverse to avoid index issues
for index1 in reversed(range(len(arr_cities))):
    print(index1)
    city_id = int(arr_cities[index1][0])
    city_name = arr_cities[index1][1]
    lati = float(arr_cities[index1][2])
    longi = float(arr_cities[index1][3])
    country = arr_cities[index1][4]
    population = arr_cities[index1][5]
    alti = int(arr_cities[index1][6])
    opt_tilt = arr_cities[index1][7]

    # Flag to determine if current city is too close to another
    city_is_close = False

    for index2 in range(index1 + 1, len(arr_cities)):
        lati2 = float(arr_cities[index2][2])
        longi2 = float(arr_cities[index2][3])
        alti2 = int(arr_cities[index2][6])

        if function_haversine_formula.distance(lati, longi, lati2, longi2) < max_distance and abs(alti - alti2) < max_alt_diff:
            city_is_close = True
            break

    # Add city to filtered list if it's not too close to another
    if not city_is_close:
        filtered_cities.append(arr_cities[index1])

        #insert city in new database
        command_insert_city = "INSERT INTO cities_filtered(city_id, city_name, lat, lng, country, altitude, opt_tilt_angle) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data = (city_id, city_name, lati, longi, country, alti, opt_tilt)
        cursor.execute(command_insert_city, data)
        conn.commit()




#now adding the most populated cities from each country to the database
command_cities_population = '''SELECT id_city, city, lat, lng, country, population, altitude, opt_tilt_angle
                                FROM (
                                SELECT id_city, city, lat, lng, country, population, altitude, opt_tilt_angle,
                                    ROW_NUMBER() OVER (PARTITION BY country ORDER BY population DESC) AS row_num
                                FROM cities
                                WHERE population IS NOT NULL
                                ) subquery
                                WHERE row_num <= 5
                                ORDER BY country, population DESC;'''
cursor.execute(command_cities_population)
#Convert PostgreSQL to numpy
arr_cities_population = np.array(cursor.fetchall())
for index in reversed(range(len(arr_cities_population))):
    print(index)
    city_id = int(arr_cities_population[index][0])
    city_name = arr_cities_population[index][1]
    lati = float(arr_cities_population[index][2])
    longi = float(arr_cities_population[index][3])
    country = arr_cities_population[index][4]
    population = arr_cities_population[index][5]
    alti = int(arr_cities_population[index][6])
    opt_tilt = arr_cities_population[index][7]

    command_insert_city_population = "INSERT INTO cities_filtered(city_id, city_name, lat, lng, country, altitude, opt_tilt_angle, population) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (city_id) DO UPDATE SET city_name = EXCLUDED.city_name, lat = EXCLUDED.lat, lng = EXCLUDED.lng, country = EXCLUDED.country, altitude = EXCLUDED.altitude, opt_tilt_angle = EXCLUDED.opt_tilt_angle, population = EXCLUDED.population"
    data = (city_id, city_name, lati, longi, country, alti, opt_tilt, population)
    cursor.execute(command_insert_city_population, data)
    conn.commit()
    


