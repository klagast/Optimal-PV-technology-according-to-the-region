import psycopg2
import numpy as np
import pandas as pd
import sqlalchemy
import time

#file to add the top 10 best panels for each city to a database

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)
#select from PostgreSQL database
command_cities = "SELECT city_id FROM cities_filtered ORDER BY city_id"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy

for index1 in arr_cities:
    start = time.time()
    city_id = int(index1[0])
    query1 = 'SELECT modules_2.id_modules, modules_2.name, modules_2.material, dc_results.dc_power FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power DESC LIMIT 10'
    table_dc_results = pd.read_sql(query1, engine, params=[city_id])
    query2 = 'SELECT modules_2.id_modules, modules_2.name, modules_2.material, dc_results.dc_power_m2 FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power_m2 DESC LIMIT 10'
    table_dc_results_m2 = pd.read_sql(query2, engine, params=[city_id])
    query3 = 'SELECT modules_2.id_modules, modules_2.name, modules_2.material, dc_results.dc_power_wpeak FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power_wpeak DESC LIMIT 10'
    table_dc_results_wpeak = pd.read_sql(query3, engine, params=[city_id])


    table_dc_results = table_dc_results.add_prefix('dc_')
    table_dc_results_m2 = table_dc_results_m2.add_prefix('dc_m2_')
    table_dc_results_wpeak = table_dc_results_wpeak.add_prefix('dc_wpeak_')
    # concatenate the three dataframes column-wise
    top10_df = pd.concat([table_dc_results, table_dc_results_m2, table_dc_results_wpeak], axis=1)

    for index, row in top10_df.iterrows():
        command_DC_power = "INSERT INTO top10panelscities_2(cities_id, id_modules_dc, name_dc, material_dc, dc_power, id_modules_dc_m2, name_dc_m2, material_dc_m2, dc_power_m2, id_modules_dc_wpeak, name_dc_wpeak, material_dc_wpeak, dc_power_wpeak) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data = (city_id, row["dc_id_modules"], row['dc_name'], row['dc_material'], row["dc_dc_power"], row["dc_m2_id_modules"], row['dc_m2_name'], row['dc_m2_material'], row["dc_m2_dc_power_m2"], row["dc_wpeak_id_modules"], row['dc_wpeak_name'], row['dc_wpeak_material'], row["dc_wpeak_dc_power_wpeak"])
        cursor.execute(command_DC_power, data)
        conn.commit()
    end = time.time()
    elapsed = end - start
    print("Elapsed time: ", elapsed, "seconds")


