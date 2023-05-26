import folium
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import sqlalchemy
from mpl_toolkits.basemap import Basemap
from sklearn.preprocessing import MaxAbsScaler

# steps followed in the data analysis, dividing the cities into 8 clusters

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

query = "SELECT city_id, sum_ghi, mean_ws, mean_tamb, min_tamb, max_tamb, mean_hum, lat, lng FROM weather_average_each_city ORDER by city_id"
cursor.execute(query)
x = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'sum_ghi', 'mean_ws', 'mean_tamb', 'min_tamb', 'max_tamb', 'mean_hum', 'lat', 'lng') )


#scale things between 0 and 1 and make boxplot every parameter
from sklearn.preprocessing import MinMaxScaler    
scaler = MinMaxScaler()
# scaler = MaxAbsScaler()


xx=x.drop(['city_id', 'lng','lat'],axis=1)
xscaled=pd.DataFrame(scaler.fit_transform(xx),columns=xx.columns)
xscaled.plot.box()
plt.ylabel('Scaled values between 0 and 1')

plt.show()

#density plot, not all the variables are distributed normally
xscaled.plot.density()
plt.xlabel('Scaled values in the density plot')
plt.show()


#PCA
#solving outlier problem solved by PCA
from sklearn.decomposition import PCA
pca=PCA(n_components=2)
PC=pca.fit_transform(xscaled)
pdf=pd.DataFrame(data=PC,columns=['A','B'])
plt.scatter(pdf['A'],pdf['B'])

# PCA for outlier detection
## We overcame the problem of outliers using PCA
#and we eliminated dimensions, now only have 2 dimensions
pdf.plot.box()
plt.ylabel('Scaled values')
plt.show()

#density plot again, should have more normal distributed values
pdf.plot.density()
plt.xlabel('Scaled values in the density plot')
plt.show()


color_list = ['red', 'blue', 'green', 'yellow', 'orange', 'purple', 'pink', 'brown']
np.random.seed(22)  # set random seed so that it every time shows the same color on the scatterplot
#Kmeans
from sklearn.cluster import KMeans
km = KMeans(n_clusters = 8).fit(pdf)
y_kmeans=km.predict(pdf)
plt.scatter(pdf['A'], pdf['B'], c=[color_list[i] for i in y_kmeans], s=50)


centers = km.cluster_centers_
plt.scatter(centers[:, 0], centers[:, 1], c='black', s=50, alpha=1) #add cluster centers to the plot, black dots
plt.ylabel('B')
plt.xlabel('A')
plt.show()





x=pd.DataFrame.from_dict(x,dtype='float')
labels=pd.DataFrame.from_dict(y_kmeans,dtype='float')
x.dtypes

x=x.join(labels)
x=x.rename(columns={0:'labels'})
x=pd.DataFrame.from_dict(x)

locations = x[['lat', 'lng']]
locationlist = locations.values.tolist()


def regioncolors(counter):
    if counter['labels'] == 0:
        return 'red'
    elif counter['labels'] == 1:
        return 'blue'
    elif counter['labels'] == 2:
        return 'green'
    elif counter['labels'] == 3:
        return 'yellow'
    elif counter['labels'] == 4:
        return 'orange'
    elif counter['labels'] == 5:
        return 'purple'
    elif counter['labels'] == 6:
        return 'pink'
    elif counter['labels'] == 7:
        return 'brown'
    else:
        return 'darkblue'
    
x["color"] = x.apply(regioncolors, axis=1)
print('jaja')

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
a, b = map(x['lng'], x['lat'])


# Plot the cities on the map with colors based on the recommended material
scatter = map.scatter(a, b, c=x['color'], edgecolor='black', linewidths=0.5)

# Show the map
plt.show()


for i, center in enumerate(centers):
    closest_row_idx = ((pdf['A'] - center[0])**2 + (pdf['B'] - center[1])**2).argmin()
    city_id = x.iloc[closest_row_idx]['city_id']
    print(f"Cluster {i+1}: city_id {city_id}")












#add data to new table cities_clustered
for index, row in x.iterrows():
    command = "INSERT INTO cities_clustered(city_id, sum_ghi, mean_ws, mean_tamb, min_tamb, max_tamb, mean_hum, lat, lng, labels, color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # data = (x['city_id'].values, x['sum_ghi'].values, x['mean_ws'].values, x['mean_tamb'].values, x['min_tamb'].values, x['max_tamb'].values, x['mean_hum'].values, x['lat'].values, x['lng'].values, x['labels'].values, x['color'].values)
    data = (row['city_id'], row['sum_ghi'], row['mean_ws'], row['mean_tamb'], row['min_tamb'], row['max_tamb'], row['mean_hum'], row['lat'], row['lng'], row['labels'], row['color'])
    cursor.execute(command, data)
    conn.commit()















#code to find the ideal number of clusters
#with elbow method
def find_best_clusters(df, maximum_K):
    clusters_centers = []
    k_values = []
    for k in range(1, maximum_K):
        kmeans_model = KMeans(n_clusters = k)
        kmeans_model.fit(df)
        clusters_centers.append(kmeans_model.inertia_)
        k_values.append(k)
    
    return clusters_centers, k_values
def generate_elbow_plot(clusters_centers, k_values):
    figure = plt.subplots(figsize = (12, 6))
    plt.plot(k_values, clusters_centers, 'o-', color = 'green')
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("Sum of squared errors")
    plt.title("Elbow Plot of KMeans")
    plt.show()

clusters_centers, k_values = find_best_clusters(xscaled, 24)
generate_elbow_plot(clusters_centers, k_values)

