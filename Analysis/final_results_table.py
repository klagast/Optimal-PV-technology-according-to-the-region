import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np


#this is the code to add for all the 8 most respresentative cities for every cluster data in the final_results_2 table
#you have to change the city_id to the city id of the most representative city for each cluster
#the final_results_2 table is then used to make the heatmap for the correlations between dc_power_wpeak and the module parameters


#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)


query2 = 'SELECT city_id, sum_ghi, mean_ws, max_tamb, min_tamb, mean_tamb, mean_hum, lat, lng FROM weather_average_each_city WHERE city_id = 3671'
cursor.execute(query2)
x = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'lat', 'lng'))


query = '''SELECT id_modules, name, material, cells_in_series, parallel_strings, isco, voco, impo, vmpo, aisc, aimp,
    c0, c1, bvoco, mbvoc, bvmpo, mbvmp, n, c2, c3, a0, a1, a2, a3, a4, b0, b1, b2, b3, b4, b5, 
    dtc, fd, a, b, c4, c5, ixo, ixxo, c6, c7
    FROM modules_2
    ORDER BY id_modules'''
cursor.execute(query)
y = pd.DataFrame(cursor.fetchall(), columns = ('id_modules', 'name', 'material', 'cells_in_series', 'parallel_strings', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp',
                                                    'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a0', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 
                                                    'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7'))



query5 = '''SELECT dc_results.id_modules, dc_results.dc_power_m2, dc_results.dc_power_wpeak FROM dc_results
INNER JOIN modules_2 ON dc_results.id_modules = modules_2.id_modules
WHERE dc_results.cities_id = 3671
ORDER BY dc_results.id_modules'''
cursor.execute(query5)
z = pd.DataFrame(cursor.fetchall(), columns = ('id_modules', 'dc_power_m2', 'dc_power_wpeak'))



for index, row in y.iterrows():
    query3 = '''INSERT INTO final_results_2(cluster_id, city_id, sum_ghi, mean_ws, max_tamb, min_tamb, mean_tamb, mean_hum, lat, long, 
    name_module, id_module, tech_module, cells_in_series, parallel_strings, isco, voco, impo, vmpo, aisc, aimp,
                                                    c0, c1, bvoco, mbvoc, bvmpo, mbvmp, n, c2, c3, a1, a2, a3, a4, b0, b1, b2, b3, b4, b5, 
                                                    dtc, fd, a, b, c4, c5, ixo, ixxo, c6, c7, a0,
                                                    dc_yield_m2, dc_power_wpeak ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    data = (str('8'), str('3671'), float(x['sum_ghi'].iloc[0]), float(x['mean_ws'].iloc[0]), float(x['max_tamb'].iloc[0]), float(x['min_tamb'].iloc[0]), float(x['mean_tamb'].iloc[0]), float(x['mean_hum'].iloc[0]), float(x['lat'].iloc[0]), float(x['lng'].iloc[0]), 
            y['name'].iloc[index], int(y['id_modules'].iloc[index]), y['material'].iloc[index],
            int(y['cells_in_series'].iloc[index]), int(y['parallel_strings'].iloc[index]), float(y['isco'].iloc[index]), float(y['voco'].iloc[index]), float(y['impo'].iloc[index]), 
            float(y['vmpo'].iloc[index]), float(y['aisc'].iloc[index]), float(y['aimp'].iloc[index]), float(y['c0'].iloc[index]), float(y['c1'].iloc[index]),
            float(y['bvoco'].iloc[index]), float(y['mbvoc'].iloc[index]), float(y['bvmpo'].iloc[index]), float(y['mbvmp'].iloc[index]), float(y['n'].iloc[index]),
            float(y['c2'].iloc[index]), float(y['c3'].iloc[index]), float(y['a1'].iloc[index]), float(y['a2'].iloc[index]), float(y['a3'].iloc[index]),
            float(y['a4'].iloc[index]), float(y['b0'].iloc[index]), float(y['b1'].iloc[index]), float(y['b2'].iloc[index]), float(y['b3'].iloc[index]),
            float(y['b4'].iloc[index]), float(y['b5'].iloc[index]), float(y['dtc'].iloc[index]), float(y['fd'].iloc[index]), float(y['a'].iloc[index]),
            float(y['b'].iloc[index]), float(y['c4'].iloc[index]), float(y['c5'].iloc[index]), float(y['ixo'].iloc[index]), float(y['ixxo'].iloc[index]),
            float(y['c6'].iloc[index]), float(y['c7'].iloc[index]), float(y['a0'].iloc[index]),
            float(z['dc_power_m2'].iloc[index]), float(z['dc_power_wpeak'].iloc[index]))
    cursor.execute(query3, data)
    conn.commit()

