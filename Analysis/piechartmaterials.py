import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import sqlalchemy
import numpy as np

#file to make a piechart of the used modules in the Sandia Module Database

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)


command = "SELECT material FROM modules_2 ORDER BY id_modules"
cursor.execute(command)
#Convert SQL to dataframe
my_modules = pd.DataFrame(cursor.fetchall(), columns=['material'])


color_dict = {'c-Si': 'red', 'mc-Si': 'yellow', 'HIT-Si': 'violet', 'CdTe': 'brown'}

# Count the number of occurrences of each material type in the dataframe
counts = my_modules['material'].value_counts()
# Subtract 1 from the count of 'c-Si' because one panel is unusable
counts['c-Si'] -= 1
# Sort the counts in descending order
counts = counts.sort_values(ascending=False)
# Create a list of colors corresponding to the materials
colors = [color_dict[m] for m in counts.index]


# Create the pie chart
plt.figure(figsize=(12, 12))
plt.pie(counts, labels=counts.index, colors=colors, autopct=lambda pct: f"{pct:.1f}% ({int(pct / 100 * counts.sum())})", startangle=90)
# Show the pie chart
plt.show()

x = np.char.array(['mc-Si', 'c-Si', 'HIT-Si', 'CdTe']) 

porcent = 100.*counts/counts.sum()
#Create the pie chart
patches, texts = plt.pie(counts, colors=colors, startangle=90, radius=1.2)

labels = ['{0} - {1} ({2:1.2f}%)'.format(i, counts[i], porcent[i]) for i in x]

sort_legend = False
if sort_legend:
    patches, labels, dummy = zip(*sorted(zip(patches, labels, counts),
                                        key=lambda x: x[2],
                                        reverse=True))

plt.legend(patches, labels, loc = 'upper right', bbox_to_anchor=(0.5, 0., 1., 1.,),
           fontsize=8)

plt.savefig('piechart.png', bbox_inches='tight')