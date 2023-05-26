import psycopg2
import numpy as np
import pandas as pd
import sqlalchemy
import time

#code to create the 3 tables with the weights for the top10 best moudles





#for the DC yearly energy yield
#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)
#select from PostgreSQL database
command_cities = "SELECT cf.city_id FROM cities_filtered cf WHERE EXISTS (SELECT 1 FROM tmy_cities tc WHERE tc.city_id = cf.city_id) ORDER BY cf.city_id"
# command_cities = "SELECT city_id FROM cities_filtered WHERE city_id = 1"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy

for index in arr_cities:
    start = time.time()
    city_id = int(index[0])
    query1 = 'SELECT modules_2.material, dc_results.dc_power FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power DESC LIMIT 10'
    table_dc_results = pd.read_sql(query1, engine, params=[city_id])
    # cursor.execute(query1)
    # table_dc_results = pd.DataFrame(cursor.fetchall(), columns=(('material', 'dc_power')))
    table_dc_results['weight'] = pd.Series([])
    highest_value = table_dc_results.loc[0, 'dc_power']
    for index, row in table_dc_results.iterrows():
        #calculate the weight for every row
        table_dc_results.loc[index, 'weight'] = row['dc_power'] / highest_value
    
    # group the dataframe by 'material' and sum the 'weight' values
    material_weights = table_dc_results.groupby('material')['weight'].sum()
    # find the material with the highest cumulative weight
    max_material = material_weights.idxmax()

    #find the weights for all the materials
    weight_1 = material_weights.get('c-Si', default=0)
    weight_2 = material_weights.get('a-Si / mono-Si', default=0)
    weight_3 = material_weights.get('mc-Si', default=0)
    weight_4 = material_weights.get('EFG mc-Si', default=0)
    weight_5 = material_weights.get('2-a-Si', default=0)
    weight_6 = material_weights.get('3-a-Si', default=0)
    weight_7 = material_weights.get('HIT-Si', default=0)
    weight_8 = material_weights.get('Si-Film', default=0)
    weight_9 = material_weights.get('CdTe', default=0)
    weight_10 = material_weights.get('CIS', default=0)
    weight_11 = material_weights.get('GaAs', default=0)
    
    query_insert = 'INSERT INTO map_dc_power_2(city_id, c_si, a_si_mono_si, mc_si, efg_mc_si, a_si_2, a_si_3, hit_si, si_film, cdte, cis, gaas, material_recommended) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data = (city_id, weight_1, weight_2, weight_3, weight_4, weight_5, weight_6, weight_7, weight_8, weight_9, weight_10, weight_11, max_material)
    cursor.execute(query_insert, data)
    conn.commit()
    end = time.time()
    elapsed = end - start
    print("Elapsed time: ", elapsed, "seconds")

















#for the yield per m²
#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)
#select from PostgreSQL database
command_cities = "SELECT cf.city_id FROM cities_filtered cf WHERE EXISTS (SELECT 1 FROM tmy_cities tc WHERE tc.city_id = cf.city_id) ORDER BY cf.city_id"
# command_cities = "SELECT city_id FROM cities_filtered WHERE city_id = 1"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy

for index in arr_cities:
    start = time.time()
    city_id = int(index[0])
    query1 = 'SELECT modules_2.material, dc_results.dc_power_m2 FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power_m2 DESC LIMIT 10'
    table_dc_results = pd.read_sql(query1, engine, params=[city_id])
    # cursor.execute(query1)
    # table_dc_results = pd.DataFrame(cursor.fetchall(), columns=(('material', 'dc_power')))
    table_dc_results['weight'] = pd.Series([])
    highest_value = table_dc_results.loc[0, 'dc_power_m2']
    for index, row in table_dc_results.iterrows():
        #calculate the weight for every row
        table_dc_results.loc[index, 'weight'] = row['dc_power_m2'] / highest_value
    
    # group the dataframe by 'material' and sum the 'weight' values
    material_weights = table_dc_results.groupby('material')['weight'].sum()
    # find the material with the highest cumulative weight
    max_material = material_weights.idxmax()

    #find the weights for all the materials
    weight_1 = material_weights.get('c-Si', default=0)
    weight_2 = material_weights.get('a-Si / mono-Si', default=0)
    weight_3 = material_weights.get('mc-Si', default=0)
    weight_4 = material_weights.get('EFG mc-Si', default=0)
    weight_5 = material_weights.get('2-a-Si', default=0)
    weight_6 = material_weights.get('3-a-Si', default=0)
    weight_7 = material_weights.get('HIT-Si', default=0)
    weight_8 = material_weights.get('Si-Film', default=0)
    weight_9 = material_weights.get('CdTe', default=0)
    weight_10 = material_weights.get('CIS', default=0)
    weight_11 = material_weights.get('GaAs', default=0)
    
    query_insert = 'INSERT INTO map_dc_power_m2_2(city_id, c_si, a_si_mono_si, mc_si, efg_mc_si, a_si_2, a_si_3, hit_si, si_film, cdte, cis, gaas, material_recommended) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data = (city_id, weight_1, weight_2, weight_3, weight_4, weight_5, weight_6, weight_7, weight_8, weight_9, weight_10, weight_11, max_material)
    cursor.execute(query_insert, data)
    conn.commit()
    end = time.time()
    elapsed = end - start
    print("Elapsed time: ", elapsed, "seconds")

















#for the yield divided by the watt-peak
#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)
#select from PostgreSQL database
command_cities = "SELECT cf.city_id FROM cities_filtered cf WHERE EXISTS (SELECT 1 FROM tmy_cities tc WHERE tc.city_id = cf.city_id) ORDER BY cf.city_id"
# command_cities = "SELECT city_id FROM cities_filtered WHERE city_id = 1"
cursor.execute(command_cities)
arr_cities = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy

for index in arr_cities:
    start = time.time()
    city_id = int(index[0])
    query1 = 'SELECT modules_2.material, dc_results.dc_power_wpeak FROM dc_results JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules WHERE cities_id = %s ORDER BY dc_power_wpeak DESC LIMIT 10'
    table_dc_results = pd.read_sql(query1, engine, params=[city_id])
    # cursor.execute(query1)
    # table_dc_results = pd.DataFrame(cursor.fetchall(), columns=(('material', 'dc_power')))
    table_dc_results['weight'] = pd.Series([])
    highest_value = table_dc_results.loc[0, 'dc_power_wpeak']
    for index, row in table_dc_results.iterrows():
        #calculate the weight for every row
        table_dc_results.loc[index, 'weight'] = row['dc_power_wpeak'] / highest_value
    
    # group the dataframe by 'material' and sum the 'weight' values
    material_weights = table_dc_results.groupby('material')['weight'].sum()
    # find the material with the highest cumulative weight
    max_material = material_weights.idxmax()

    #find the weights for all the materials
    weight_1 = material_weights.get('c-Si', default=0)
    weight_2 = material_weights.get('a-Si / mono-Si', default=0)
    weight_3 = material_weights.get('mc-Si', default=0)
    weight_4 = material_weights.get('EFG mc-Si', default=0)
    weight_5 = material_weights.get('2-a-Si', default=0)
    weight_6 = material_weights.get('3-a-Si', default=0)
    weight_7 = material_weights.get('HIT-Si', default=0)
    weight_8 = material_weights.get('Si-Film', default=0)
    weight_9 = material_weights.get('CdTe', default=0)
    weight_10 = material_weights.get('CIS', default=0)
    weight_11 = material_weights.get('GaAs', default=0)
    
    query_insert = 'INSERT INTO map_dc_power_wpeak_2(city_id, c_si, a_si_mono_si, mc_si, efg_mc_si, a_si_2, a_si_3, hit_si, si_film, cdte, cis, gaas, material_recommended) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data = (city_id, weight_1, weight_2, weight_3, weight_4, weight_5, weight_6, weight_7, weight_8, weight_9, weight_10, weight_11, max_material)
    cursor.execute(query_insert, data)
    conn.commit()
    end = time.time()
    elapsed = end - start
    print("Elapsed time: ", elapsed, "seconds")


