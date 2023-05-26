import psycopg2
import sqlalchemy
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# file for making boxplots of the 6 weather parameters for the 8 clusters

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)
query = "SELECT city_id, sum_ghi, mean_ws, mean_tamb, min_tamb, max_tamb, mean_hum, lat, lng, labels, color FROM cities_clustered ORDER by city_id"
cursor.execute(query)
# x = pd.read_sql(query, engine)
x = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'sum_ghi', 'mean_ws', 'mean_tamb', 'min_tamb', 'max_tamb', 'mean_hum', 'lat', 'lng', 'labels', 'color') )

#definition to select the rows from each cluster
def select_rows_by_color(df, color):
    return df[df["color"] == color]

color_list = ['red', 'blue', 'green', 'yellow', 'orange', 'purple', 'pink', 'brown']

cluster1 = select_rows_by_color(x, 'red')
cluster2 = select_rows_by_color(x, 'blue')
cluster3 = select_rows_by_color(x, 'green')
cluster4 = select_rows_by_color(x, 'yellow')
cluster5 = select_rows_by_color(x, 'orange')
cluster6 = select_rows_by_color(x, 'purple')
cluster7 = select_rows_by_color(x, 'pink')
cluster8 = select_rows_by_color(x, 'brown')


# Create a list of the 8 clusters
clusters = [cluster1, cluster2, cluster3, cluster4, cluster5, cluster6, cluster7, cluster8]

# Create a list of the colors for the 8 clusters
colors = ['red', 'blue', 'green', 'yellow', 'orange', 'purple', 'pink', 'brown']

# Create a new DataFrame to combine all the clusters
combined = pd.concat(clusters)

fig, (ax1, ax2, ax3, ax4, ax5, ax6) = plt.subplots(nrows=6, ncols=1, sharex=True, figsize=(8, 16))
fig.subplots_adjust(hspace=0)  # Add this line to remove horizontal space


# Create the boxplot using Seaborn on the first axis
sns.boxplot(x='labels', y='sum_ghi', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax1)
# Set the title and axis labels for the first subplot
ax1.set_title('Parameters boxplots for all the clusters')
ax1.set_ylabel('Sum GHI [W/m²]')
ax1.set_xlabel('')


sns.boxplot(x='labels', y='mean_ws', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax2)
ax2.set_xlabel('Clusters')
ax2.set_ylabel('Mean wind speed [m/s]')
ax2.set_xlabel('')

sns.boxplot(x='labels', y='mean_tamb', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax3)
ax3.set_xlabel('Clusters')
ax3.set_ylabel('Mean Tamb [°C]')
ax3.set_xlabel('')

sns.boxplot(x='labels', y='min_tamb', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax4)
ax4.set_xlabel('Clusters')
ax4.set_ylabel('Minimum Tamb [°C]')
ax4.set_xlabel('')

sns.boxplot(x='labels', y='max_tamb', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax5)
ax5.set_xlabel('Clusters')
ax5.set_ylabel('Maximum Tamb [°C]')
ax5.set_xlabel('')

sns.boxplot(x='labels', y='mean_hum', data=combined, palette=dict(zip(range(len(colors)), colors)), ax=ax6)
ax6.set_xlabel('Clusters')
ax6.set_ylabel('Mean humidity [%]')


# Update the x-axis tick labels with cluster numbers
ax6.set_xticklabels([f'{i+1}' for i in range(len(clusters))])

# Show the plot
plt.show()




# Group the data by cluster labels and compute summary statistics for 'mean_hum'
summary = x.groupby('labels')['mean_hum'].describe()
# Extract the Q1, median, and Q3 values from the summary statistics
q1 = summary['25%'].values
median = summary['50%'].values
q3 = summary['75%'].values
# Print the values
print(f'Q1: {q1}')
print(f'Median: {median}')
print(f'Q3: {q3}')





