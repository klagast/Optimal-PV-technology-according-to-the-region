import requests
import pandas as pd

# script for returning elevation from lat, long, based on open elevation data
# from site "Open Elevation"
# Google elevation API key you have to pay for 
# which in turn is based on SRTM
# attention that you don't request to much from the site
def get_elevation(lat, long):
    query = ('https://api.open-elevation.com/api/v1/lookup'
             f'?locations={lat},{long}')
    r = requests.get(query).json()  # json object, various ways you can extract value
    # one approach is to use pandas json functionality:
    elevation = pd.io.json.json_normalize(r, 'results')['elevation'].values[0]
    return elevation


