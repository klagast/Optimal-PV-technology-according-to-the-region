import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np
from scipy.stats.stats import pearsonr
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap


#file to make the heatmap of the Pearson correlations
#between the dc_power_wpeak and each of the module parameters from the final_results_2 table

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

query = 'SELECT * from final_results_2 WHERE cluster_id = 1'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak'))
new_dataframe1 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# we left out columns mbvoc, mbvmp and b0 
clusters = new_dataframe1.corr().iloc[:,36]
# clusters = new_dataframe1.corr().iloc[:,0] #select first column

query = 'SELECT * from final_results_2 WHERE cluster_id = 2'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe2 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster2 = new_dataframe2.corr().iloc[:,0]
cluster2 = new_dataframe2.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 3'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe3 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster3 = new_dataframe3.corr().iloc[:,0]
cluster3 = new_dataframe3.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 4'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe4 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster4 = new_dataframe4.corr().iloc[:,0]
cluster4 = new_dataframe4.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 5'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe5 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster5 = new_dataframe5.corr().iloc[:,0]
cluster5 = new_dataframe5.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 6'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe6 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster6 = new_dataframe6.corr().iloc[:,0]
cluster6 = new_dataframe6.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 7'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe7 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster7 = new_dataframe7.corr().iloc[:,0]
cluster7 = new_dataframe7.corr().iloc[:,36]


query = 'SELECT * from final_results_2 WHERE cluster_id = 8'
cursor.execute(query)
my_final_results = pd.DataFrame(cursor.fetchall(), columns = ('cluster_id', 'city_id', 'lat', 'long', 'tech_module', 'name_module', 'dc_yield_m2', 'sum_ghi', 'mean_ws', 'max_tamb', 'min_tamb', 'mean_tamb', 'mean_hum', 'id_module', 'auto_index', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'mbvoc', 'bvmpo', 'mbvmp', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak' ))
new_dataframe8 = my_final_results[['dc_yield_m2', 'cells_in_series', 'parallel_string', 'isco', 'voco', 'impo', 'vmpo', 'aisc', 'aimp', 'c0', 'c1', 'bvoco', 'bvmpo', 'n', 'c2', 'c3', 'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4', 'b5', 'dtc', 'fd', 'a', 'b', 'c4', 'c5', 'ixo', 'ixxo', 'c6', 'c7', 'a0', 'dc_power_wpeak']].copy()
# cluster8 = new_dataframe8.corr().iloc[:,0]
cluster8 = new_dataframe8.corr().iloc[:,36]



clusters.name = 'cluster1'
cluster2.name = 'cluster2'
cluster3.name = 'cluster3'
cluster4.name = 'cluster4'
cluster5.name = 'cluster5'
cluster6.name = 'cluster6'
cluster7.name = 'cluster7'
cluster8.name = 'cluster8'
clusters = pd.concat([clusters, cluster2, cluster3, cluster4, cluster5, cluster6, cluster7, cluster8], axis=1)
clusters.columns = ['cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5', 'cluster6', 'cluster7', 'cluster8', ]


# Create heatmap
sns.heatmap(clusters, cmap='coolwarm')
# Show the plot
plt.show()


# Leave out module parameters that don't have a high standard deviation to reduce the parameters
clusters_transposed = clusters.transpose()
# Calculate the standard deviation of all columns
std = clusters_transposed.std()
# Filter out rows with std < 0.15
std_filtered = std[std >= 0.15]
# Extract the rows with std >= 0.15 from the original dataframe
clusters_filtered = clusters_transposed.loc[:, std_filtered.index]
clusters = clusters_filtered.transpose()
new_clusters = clusters.drop('dc_yield_m2', axis=0)


# Create heatmap
sns.heatmap(new_clusters, cmap='coolwarm', vmin = -0.8, vmax = 0.8) #maybe change this later on in custom color map

# Show the plot
plt.show()

# Close the database connection
cursor.close()
conn.close()



