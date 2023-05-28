import dash
import dash_leaflet as dl
from dash.dependencies import Input, Output
import psycopg2
import pandas as pd
from dash import html, dcc, Dash, callback

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

command_cities = "SELECT city_id, city_name, lat, lng FROM cities_filtered cf JOIN dc_results dpr ON cf.city_id = dpr.cities_id GROUP BY cf.city_id;"
cursor.execute(command_cities)
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'city_name', 'latitude', 'longitude'))
command_results = "SELECT cities_id, dc_power_m2 FROM dc_results WHERE id_modules = 1"
cursor.execute(command_results)
my_results = pd.DataFrame(cursor.fetchall(), columns= ('cities_id', 'dc_power_m2'))



dash.register_page(__name__, path='/')

#creating the icon
icon = {
    "iconUrl": "https://ippc.int/static/leaflet/images/marker-icon.png",
    # "shadowUrl": "https://ippc.int/static/leaflet/images/marker-shadow.png",
    "iconSize": [20, 30],  # size of the icon
    # "shadowSize": [50, 50],  # size of the shadow
    # "iconAnchor": [1, 1,],  # point of the icon which will correspond to marker's location
    # "shadowAnchor": [4, 62],  # the same for the shadow
    # "popupAnchor": [-3,-76,],  # point from which the popup should open relative to the iconAnchor
}

#function of retrieving the data
def get_data():
    markers = []
    for index in my_cities.index:
        city_id = my_cities['city_id'][index]
        city_name = my_cities['city_name'][index]
        lati = my_cities['latitude'][index]
        longi = my_cities['longitude'][index]
        dc_power = my_results['dc_power_m2'].where(my_results['cities_id'] == my_cities['city_id'][index])
        dc_power = dc_power.sum() #from a series to one value
        markers.append(
            dl.Marker(
            # id ="marker " + str(index),
            id = city_name,
            title="marker",
            position=[lati, longi],
            icon = icon,
            n_clicks=0,
            children=[
                    dl.Tooltip(city_name),
                    dl.Popup(html.A(city_name, href='http://127.0.0.1:8052/cities/' + str(city_id))),
                ],)                
        )
    cluster = dl.MarkerClusterGroup(id="markerscluster", children=markers) #can use supercluster in the future, for more markers

    return cluster

#trying to change the languague of the map
tile_url = "https://{s}.tile.openstreetmap.de/{z}/{x}/{y}.png" #german
# tile_url = "https://{s}.tile.thunderforest.com/spinal-map/{z}/{x}/{y}.png"

layout = html.Div([
    dl.Map(center=[0, 0], zoom=2, children=[dl.TileLayer(url=tile_url), get_data(), dl.EasyButton(icon='fa-home', n_clicks=0, id="btn")], #home file doesn't appear, change later on
           style={'width': '100%', 'height': '100vh'}, id="worldmap"),
    dl.LayerGroup(id="layer"),
    ])


#creating a home button to go back to the center
@callback(Output("worldmap", "center"),
              Output("worldmap", "zoom"),
              [Input("btn", "n_clicks")])
def easy_btn(n_clicks):
    center = [0, 0]
    zoom = 2
    return center, zoom