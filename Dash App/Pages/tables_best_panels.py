import dash
from dash import Dash, dcc, html, Input, Output, callback, dash_table
import psycopg2
import dash_bootstrap_components as dbc
import sqlalchemy
import pandas as pd

dash.register_page(__name__, path_template='/tables/<c_id>')
suppress_callback_exceptions=True

#establishing the connection
conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)

#table by default 
def maketable_yearly(c_id):
    query = 'SELECT dc_power, modules.name, modules.material FROM dc_results JOIN modules ON dc_results.id_modules = modules.id_modules WHERE cities_id = %s ORDER BY dc_power DESC LIMIT 20'
    table_dc_results = pd.read_sql(query, engine, params=[c_id])
    data1 = table_dc_results.to_dict('records')
    columns1 = [{"name": i, "id": i} for i in table_dc_results.columns]

    return html.Div([
        dbc.Label('Table top 20 highest yearly DC energy yield'),  
        dash_table.DataTable(data=data1, columns=columns1, style_cell={'fontSize':12},)  
                    ])

#table by default 
def maketable_m2(c_id):
    query = 'SELECT dc_power_m2, modules.name, modules.material FROM dc_results JOIN modules ON dc_results.id_modules = modules.id_modules WHERE cities_id = %s ORDER BY dc_power_m2 DESC LIMIT 20'
    table_dc_results = pd.read_sql(query, engine, params=[c_id])
    data1 = table_dc_results.to_dict('records')
    columns1 = [{"name": i, "id": i} for i in table_dc_results.columns]

    return html.Div([
        dbc.Label('Table top 20 highest yearly DC energy yield per square meter'), 
        dash_table.DataTable(data=data1, columns=columns1, style_cell={'fontSize':12},)  
                    ])

#table by default 
def maketable_wpeak(c_id):
    query = 'SELECT dc_power_wpeak, modules.name, modules.material FROM dc_results JOIN modules ON dc_results.id_modules = modules.id_modules WHERE cities_id = %s ORDER BY dc_power_wpeak DESC LIMIT 10'
    table_dc_results = pd.read_sql(query, engine, params=[c_id])
    data1 = table_dc_results.to_dict('records')
    columns1 = [{"name": i, "id": i} for i in table_dc_results.columns]

    return html.Div([
        dbc.Label('Table top 10 highest yearly DC energy yield per Watt peak'),  
        dash_table.DataTable(data=data1, columns=columns1, style_cell={'fontSize':12},)  
                    ])

def layout (c_id= '1'):
    href_to_give = '/cities/' + c_id
    return html.Div([
    dcc.Link("Home", id='home link', href=href_to_give, refresh= False),
    html.Br(style={"line-height": "5"}),
    f"The user requested report ID: {c_id}.",
    maketable_yearly(c_id),
    maketable_m2(c_id),
    maketable_wpeak(c_id)
])

def layout(c_id = '1'):
    href_to_give = '/cities/' + c_id
    return html.Div(children=[
    dcc.Link("Home", id='home link', href=href_to_give, refresh= False),
        dbc.Card(
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Div(
                                    [
                                    maketable_yearly(c_id),
                                    ])
                            
                    ], width=4),
                    
                    dbc.Col([
                        html.Div(
                                    [   
                                    maketable_m2(c_id),   
                                    ])
                    ], width=4),
                    
                    dbc.Col([
                        html.Div(
                                    [
                                    maketable_wpeak(c_id)    
                                    ])
                    ], width=4),
                ], align='center', justify='center'), 
                html.Br(),
                ]), color = 'light'
        )
    ])




