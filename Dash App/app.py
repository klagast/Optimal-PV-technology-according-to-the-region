from dash import Dash, html, dcc
import dash
import dash_bootstrap_components as dbc
from PIL import Image


app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.SLATE])



app.layout = html.Div([
    html.Div(
        [
        
        ]
    ),
	dash.page_container
])


if __name__ == '__main__':
	app.run_server(port = 8052)


