import dash
from dash import html, dcc, callback, Input, Output, dash_table
import sqlalchemy
import psycopg2
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc 
import function_that_returns_energy_yield_everytimestamp
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime
from dateutil import tz
from timezonefinder import TimezoneFinder

dash.register_page(__name__, path_template="cities/<c_id>")



#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)


cursor.execute('SELECT DISTINCT city_id, city_name FROM cities_filtered ORDER BY city_name')
my_cities = pd.DataFrame(cursor.fetchall(), columns= ('city_id', 'city_name'))
my_cities = my_cities.to_dict(orient='records')


cursor.execute('SELECT DISTINCT country FROM cities_filtered ORDER BY country')
countries = [country[0] for country in cursor.fetchall()]
cursor.execute('SELECT DISTINCT name FROM modules ORDER BY name')
modules = [module[0] for module in cursor.fetchall()]


# Use the fancy layout
app = dash.Dash(external_stylesheets=[dbc.themes.SLATE])



# figure by default
def drawFigure_new():

    # Create a pandas DataFrame with hourly timestamps for the year 2023
    dates = pd.date_range("2019-01-01 00:00:00", "2019-12-31 23:00:00", freq="H")

    fig = px.line(title="The DC energy yield for a typical year")
    # fig=px.line(df, x="new_timestamp", y=dc_power_hourly['p_mp'], title="The DC energy yield for a typical year")
    fig.update_xaxes(title_text="Time"),
    fig.update_yaxes(title_text="DC energy yield in Whr")

    return  html.Div([
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(figure=fig, id='dc_energy_yield_graph' 
                ) 
            ])
        ),  
    ])




def layout(c_id = "1"):
    ref_to_table = '/tables/' + c_id
    ref_to_dc_map = '/dc_map/' + c_id
    return html.Div(children=[
        dcc.Link("Home", id='home link', href='/', refresh= True),
        dbc.Card(
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Div([
                            dbc.Card(
                                dbc.CardBody([
                                    html.Div([
                                        dmc.Title(c_id, id="selected_city_title", order=1),
                                        # html.H2("Tokio", style={'color': 'black'}),
                                    ], style={'textAlign': 'center'}) 
                ]), style={'background-color': 'white', 'color': 'black'}
            ),
        ])
                    ], width=3),
                    
                    dbc.Col([
                        html.Div(
                                    [   
                                        dcc.Dropdown(id="framework_select_cities", 
                                                     options=[{'label': i['city_name'], 'value': i['city_id']} for i in my_cities],
                                                      value = c_id),
                                        
                                        dmc.Text(id="selected_value_cities"),
                                    ])
                    ], width=3),
                    
                    dbc.Col([
                        html.Div(
                                    [
                                        dmc.Select(
                                            label="Select a country:",
                                            placeholder="Select one",
                                            id="framework_select_countries",
                                            data=[{'label': country, 'value': country} for country in countries],
                                            value=None,
                                            searchable=True, nothingFound='The selected country not found',
                                            clearable=True,
                                            style={"width": 200, "marginBottom": 10}, 
                                        ),
                                        dmc.Text(id="selected_value_countries"),
                                    ])
                    ], width=3),

                    dbc.Col([
                        html.Div(
                                    [
                                    

                                    dcc.Link("Table details", id='home link', href=ref_to_table, refresh= False),
                                    html.Br(style={"line-height": "5"}),
                                    dcc.Link("World map DC Energy yield", id='home link', href=ref_to_dc_map, refresh= False),
                                    html.Br(style={"line-height": "5"}),

                                    
                                    ])
                    ], width=3),
                ], align='center', justify='center'), 
                html.Br(),

                dbc.Row([
                    dbc.Col([
                        html.Div(
                                    [
                                        dmc.Select(
                                            label="Select a module",
                                            placeholder="Select one",
                                            id="framework_select_modules",
                                            data=[{'label': module, 'value': module} for module in modules],
                                            # value=None,
                                            value = 'Advent_Solar_AS160___2006_',
                                            searchable=True, nothingFound='The selected module not found',
                                            clearable=True,
                                            style={"width": 200, "marginBottom": 10}, 
                                        ),
                                        dmc.Text(id="selected_value_modules"),
                                    ])
                    ], width=3),

                    dbc.Col([
                        drawFigure_new()
                    ], width=9),
                    
                ], align='center'), 
                    
            ]), color = 'light'
        )
    ])




#Callback dynamic graph    
@callback(Output('dc_energy_yield_graph', 'figure'), [Input('framework_select_cities', 'value'), Input('framework_select_modules', 'value')])
def update_figure(selected_cityid, selected_module):
    query = 'SELECT city_id, lng, lat, altitude, opt_tilt_angle FROM cities_filtered WHERE city_id = %s'
    energy_yield = pd.read_sql(query, engine, params=[selected_cityid])
    dc_power_hourly = function_that_returns_energy_yield_everytimestamp.energy_yield(
        int(energy_yield['city_id']),
        float(energy_yield['lng']),
        float(energy_yield['lat']),
        int(energy_yield['altitude']),
        selected_module,
        float(energy_yield['opt_tilt_angle']))
    
    #find timezone
    tf = TimezoneFinder()  
    timezone_city = tf.timezone_at(lng=float(energy_yield['lng']), lat=float(energy_yield['lat']))  
    #zones:
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(timezone_city)
    # Get the UTC offsets for the two timezones:
    utc_offset_from = from_zone.utcoffset(datetime.utcnow())
    utc_offset_to = to_zone.utcoffset(datetime.utcnow())
    # Calculate the timezone difference in hours:
    timezone_diff = (utc_offset_to - utc_offset_from).total_seconds() / 3600

    #Create hourly timespamps for all 2020
    dates = pd.date_range("2019-01-01 00:00:00", "2019-12-31 23:00:00", freq="H")
    # Add x hours to the timestamps
    dates_plus_x_hours = dates + pd.Timedelta(hours=timezone_diff)

    df = pd.DataFrame({'new_timestamp': dates_plus_x_hours})

    fig=px.line(df, x="new_timestamp", y=dc_power_hourly['p_mp'], title="The DC energy yield for a typical meteorological year")
    fig.update_xaxes(title_text="Date"),
    fig.update_yaxes(title_text="DC energy yield in Whr")



    return fig


