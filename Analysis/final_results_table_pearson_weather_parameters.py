import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np
from scipy.stats.stats import pearsonr
import seaborn as sns
import matplotlib.pyplot as plt
import function_that_returns_energy_yield_everytimestamp
import warnings


#file to make the heatmap of the Pearson correlations
#between the dc_power and each of the weather parameters from the TMY table

# Suppress all warnings
warnings.filterwarnings("ignore")


#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

#selection tmy for cluster 1
query = 'SELECT * from tmy_cities WHERE city_id = 2283'
cursor.execute(query)
my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy1['index'] = range(1, len(df_tmy1) + 1)
#calculating energy yield
energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(2283, 3.25, 34.6667, 1155, 'Kyocera_Solar_KS20__2008__E__', 29.06117878488231)
# add a column with integers from 1 to the number of rows
energy_yield1['index'] = range(1, len(energy_yield1) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
df_tmy1 = df_tmy1.drop('index', axis=1)
pearson1 = df_tmy1.corr().iloc[:,7] #select the 8th row and calculate pearsonR




#selection tmy for cluster 2
query = 'SELECT * from tmy_cities WHERE city_id = 1001'
cursor.execute(query)
my_tmy2 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy2 = my_tmy2[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy2['index'] = range(1, len(df_tmy2) + 1)
#calculating energy yield
energy_yield2 = function_that_returns_energy_yield_everytimestamp.energy_yield(1001, 19.9372, 50.0614, 219, 'Kyocera_Solar_KS20__2008__E__', 35.51029761798848)
# add a column with integers from 1 to the number of rows
energy_yield2['index'] = range(1, len(energy_yield2) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield2 = energy_yield2[energy_yield2['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy2 = df_tmy2.merge(energy_yield2[['index', 'p_mp']], on='index')
df_tmy2 = df_tmy2.drop('index', axis=1)
pearson2 = df_tmy2.corr().iloc[:,7] #select the 8th row and calculate pearsonR

#selection tmy for cluster 3
query = 'SELECT * from tmy_cities WHERE city_id = 2439'
cursor.execute(query)
my_tmy3 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy3 = my_tmy3[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy3['index'] = range(1, len(df_tmy3) + 1)
#calculating energy yield
energy_yield3 = function_that_returns_energy_yield_everytimestamp.energy_yield(2439, -8.6108, 41.1495, 89, 'Kyocera_Solar_KS20__2008__E__', 32.02362029435735)
# add a column with integers from 1 to the number of rows
energy_yield3['index'] = range(1, len(energy_yield3) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield3 = energy_yield3[energy_yield3['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy3 = df_tmy3.merge(energy_yield3[['index', 'p_mp']], on='index')
df_tmy3 = df_tmy3.drop('index', axis=1)
pearson3 = df_tmy3.corr().iloc[:,7] #select the 8th row and calculate pearsonR


#selection tmy for cluster 4
query = 'SELECT * from tmy_cities WHERE city_id = 6904'
cursor.execute(query)
my_tmy4 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy4 = my_tmy4[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy4['index'] = range(1, len(df_tmy4) + 1)
#calculating energy yield
energy_yield4 = function_that_returns_energy_yield_everytimestamp.energy_yield(6904, 39.056, 3.527, 1069, 'Kyocera_Solar_KS20__2008__E__', 5.4399301697550495)
# add a column with integers from 1 to the number of rows
energy_yield4['index'] = range(1, len(energy_yield4) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield4 = energy_yield4[energy_yield4['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy4 = df_tmy4.merge(energy_yield4[['index', 'p_mp']], on='index')
df_tmy4 = df_tmy4.drop('index', axis=1)
pearson4 = df_tmy4.corr().iloc[:,7] #select the 8th row and calculate pearsonR



#selection tmy for cluster 5
query = 'SELECT * from tmy_cities WHERE city_id = 8133'
cursor.execute(query)
my_tmy5 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy5 = my_tmy4[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy5['index'] = range(1, len(df_tmy5) + 1)
#calculating energy yield
energy_yield5 = function_that_returns_energy_yield_everytimestamp.energy_yield(8133, -100.3205, 44.3748, 518, 'Kyocera_Solar_KS20__2008__E__', 33.3494572201313)
# add a column with integers from 1 to the number of rows
energy_yield5['index'] = range(1, len(energy_yield5) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield5 = energy_yield5[energy_yield5['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy5 = df_tmy5.merge(energy_yield5[['index', 'p_mp']], on='index')
df_tmy5 = df_tmy5.drop('index', axis=1)
pearson5 = df_tmy5.corr().iloc[:,7] #select the 8th row and calculate pearsonR



#selection tmy for cluster 6
query = 'SELECT * from tmy_cities WHERE city_id = 42230'
cursor.execute(query)
my_tmy6 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy6 = my_tmy6[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy6['index'] = range(1, len(df_tmy6) + 1)
#calculating energy yield
energy_yield6 = function_that_returns_energy_yield_everytimestamp.energy_yield(42230, 117.846, -28.06, 430, 'Kyocera_Solar_KS20__2008__E__', -26.192336345020475)
# add a column with integers from 1 to the number of rows
energy_yield6['index'] = range(1, len(energy_yield6) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield6 = energy_yield6[energy_yield6['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy6 = df_tmy6.merge(energy_yield6[['index', 'p_mp']], on='index')
df_tmy6 = df_tmy6.drop('index', axis=1)
pearson6 = df_tmy6.corr().iloc[:,7] #select the 8th row and calculate pearsonR


#selection tmy for cluster 7
query = 'SELECT * from tmy_cities WHERE city_id = 42199'
cursor.execute(query)
my_tmy7 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy7 = my_tmy7[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy7['index'] = range(1, len(df_tmy7) + 1)
#calculating energy yield
energy_yield7 = function_that_returns_energy_yield_everytimestamp.energy_yield(42199, 135.3, 54.7, 4, 'Kyocera_Solar_KS20__2008__E__', 37.158098147407)
# add a column with integers from 1 to the number of rows
energy_yield7['index'] = range(1, len(energy_yield7) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield7 = energy_yield7[energy_yield7['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy7 = df_tmy7.merge(energy_yield7[['index', 'p_mp']], on='index')
df_tmy7 = df_tmy7.drop('index', axis=1)
pearson7 = df_tmy7.corr().iloc[:,7] #select the 8th row and calculate pearsonR


#selection tmy for cluster 8
query = 'SELECT * from tmy_cities WHERE city_id = 3671'
cursor.execute(query)
my_tmy8 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
df_tmy8 = my_tmy8[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
df_tmy8['index'] = range(1, len(df_tmy8) + 1)
#calculating energy yield
energy_yield8 = function_that_returns_energy_yield_everytimestamp.energy_yield(3671, 27.99, 2.1404, 779, 'Kyocera_Solar_KS20__2008__E__', 3.8849346780639795)
# add a column with integers from 1 to the number of rows
energy_yield8['index'] = range(1, len(energy_yield8) + 1)
# Filter out rows with p_mp = 0 from energy_yield
energy_yield8 = energy_yield8[energy_yield8['p_mp'] != 0]
# Merge the two dataframes based on the "index" column
df_tmy8 = df_tmy8.merge(energy_yield8[['index', 'p_mp']], on='index')
df_tmy8 = df_tmy8.drop('index', axis=1)
pearson8 = df_tmy8.corr().iloc[:,7] #select the 8th row and calculate pearsonR



# rename columns
pearson1.name ='cluster1'
pearson2.name ='cluster2'
pearson3.name ='cluster3'
pearson4.name ='cluster4'
pearson5.name ='cluster5'
pearson6.name ='cluster6'
pearson7.name ='cluster7'
pearson8.name ='cluster8'


pearson_total = pd.concat([pearson1, pearson2, pearson3, pearson4, pearson5, pearson6, pearson7, pearson8], axis=1)
pearson_total.columns = ['cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5', 'cluster6', 'cluster7', 'cluster8',]
print('')

#show first heatmap
# Create heatmap
sns.heatmap(pearson_total, cmap='coolwarm')
# Show the plot
plt.show()






























query_select_modules = 'SELECT name FROM modules_2'
cursor.execute(query_select_modules)
# arr_modules = np.array(cursor.fetchall()) #Convert PostgreSQL to numpy
arr_modules = np.array(cursor.fetchall()).flatten()




# Initialize an empty dataframe to store the results
total = pd.DataFrame()
# all_tmy = pd.DataFrame()


#cluster1
for index1 in arr_modules:

    #selection tmy for cluster 1
    query = 'SELECT * from tmy_cities WHERE city_id = 2283'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    #calculating energy yield
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(2283, 3.25, 34.6667, 1155, index1, 29.06117878488231)
    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # pearson1 = df_tmy1.corr().iloc[:,7] #select the 8th row and calculate pearsonR

    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    # pearson1 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR
    print('')
pearson1 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR


#cluster2
for index1 in arr_modules:

    #selection tmy for cluster 2
    query = 'SELECT * from tmy_cities WHERE city_id = 1001'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    #calculating energy yield
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(1001, 19.9372, 50.0614, 219, index1 , 35.51029761798848)

    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # pearson1 = df_tmy1.corr().iloc[:,7] #select the 8th row and calculate pearsonR

    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    # pearson1 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR
    print('')

pearson2 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR

#cluster 3
for index1 in arr_modules:
    #selection tmy for cluster 3
    query = 'SELECT * from tmy_cities WHERE city_id = 2439'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(2439, -8.6108, 41.1495, 89, index1, 32.02362029435735)
    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson3 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR





#cluster 4
for index1 in arr_modules:
    #selection tmy for cluster 4
    query = 'SELECT * from tmy_cities WHERE city_id = 6904'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(6904, 39.056, 3.527, 1069, index1 , 5.4399301697550495)

    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson4 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR




#cluster 5
for index1 in arr_modules:
    #selection tmy for cluster 5
    query = 'SELECT * from tmy_cities WHERE city_id = 8133'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(8133, -100.3205, 44.3748, 518, index1, 33.3494572201313)

    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson5 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR



#cluster 6
for index1 in arr_modules:
    #selection tmy for cluster 5
    query = 'SELECT * from tmy_cities WHERE city_id = 42230'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(42230, 117.846, -28.06, 430, index1 , -26.192336345020475)

    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson6 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR




#cluster 7
for index1 in arr_modules:
    #selection tmy for cluster 7
    query = 'SELECT * from tmy_cities WHERE city_id = 42199'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield1 = function_that_returns_energy_yield_everytimestamp.energy_yield(42199, 135.3, 54.7, 4, index1, 37.158098147407)

    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson7 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR




#cluster 8
for index1 in arr_modules:
    #selection tmy for cluster 8
    query = 'SELECT * from tmy_cities WHERE city_id = 3671'
    cursor.execute(query)
    my_tmy1 = pd.DataFrame(cursor.fetchall(), columns = ('city_id', 'time_utc', 'temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'wind_direction', 'pressure'))
    df_tmy1 = my_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure']].copy()
    df_tmy1['index'] = range(1, len(df_tmy1) + 1)
    # # Append the tmy data to the all_tmy datafram
    energy_yield8 = function_that_returns_energy_yield_everytimestamp.energy_yield(3671, 27.99, 2.1404, 779, index1, 3.8849346780639795)


    # add a column with integers from 1 to the number of rows
    energy_yield1['index'] = range(1, len(energy_yield1) + 1)
    # Filter out rows with p_mp = 0 from energy_yield
    energy_yield1 = energy_yield1[energy_yield1['p_mp'] != 0]
    
    # Merge the two dataframes based on the "index" column
    df_tmy1 = df_tmy1.merge(energy_yield1[['index', 'p_mp']], on='index')
    df_tmy1 = df_tmy1.drop('index', axis=1)
    # Append
    total = total.append(df_tmy1[['temp_air', 'relative_humidity', 'ghi', 'dni', 'dhi', 'ir_h', 'wind_speed', 'pressure', 'p_mp']])
    print('')

pearson8 = total.corr().iloc[:,7] #select the 8th row and calculate pearsonR #select the 8th row and calculate pearsonR






# rename columns
pearson1.name ='cluster1'
pearson2.name ='cluster2'
pearson3.name ='cluster3'
pearson4.name ='cluster4'
pearson5.name ='cluster5'
pearson6.name ='cluster6'
pearson7.name ='cluster7'
pearson8.name ='cluster8'


pearson_total = pd.concat([pearson1, pearson2, pearson3, pearson4, pearson5, pearson6, pearson7, pearson8], axis=1)
pearson_total = pearson_total.drop(index = 'p_mp')

pearson_total.columns = ['cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5', 'cluster6', 'cluster7', 'cluster8',]
print('')

#show first heatmap
# Create heatmap
# sns.heatmap(pearson_total, cmap='coolwarm', vmin = -1, vmax = 1)
sns.heatmap(pearson_total, cmap='coolwarm', vmin = -0.8, vmax = 0.8)
# sns.heatmap(pearson_total, cmap='coolwarm')
# Show the plot
plt.show()