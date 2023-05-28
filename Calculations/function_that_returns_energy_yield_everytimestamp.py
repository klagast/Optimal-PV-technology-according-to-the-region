import pvlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import function_optimal_solar_panel_tilt
import psycopg2
import sqlalchemy

#file to calculate the energy yield every hour

#definition calculating yearly energy yield  
def energy_yield(id_city, long, lat, alt, mod, opt_tilt_angle):
    id_city = int(id_city)

    #inserting the parameters
    coordinates = [(lat, long, id_city , alt),]
    
    #entering module and temp
    sandia_modules = pvlib.pvsystem.retrieve_sam('SandiaMod') #returns the Sandia Module database
    module = sandia_modules[mod] 

    temperature_model_parameters = pvlib.temperature.TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_polymer']
    #gives temp of cell of the Sandia Array Performance Model


    # #establishing the connection
    conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
    engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)


    query = "SELECT time_utc, temp_air, relative_humidity, ghi, dni, dhi, ir_h, wind_speed, wind_direction, pressure FROM tmy_cities WHERE city_id = %s"
    weather = pd.read_sql(query, engine, params=[id_city])
    weather = weather.set_index('time_utc')
    weather.index.name = "time(UTC)"

    #tmy from PVGIS site
    tmys = []
    tmys.append(weather)
   

    surf_azimuth = 0
    if lat > 0:
        surf_azimuth = 180
    if lat < 0:
        surf_azimuth = 0

    #defining a pv system
    system = {'module': module,
          'surface_azimuth': surf_azimuth} #module is facing south 
          #can be switched to optimal surface_azimuth later on
          #0: north, 90: east, 180: south, 270: west


    #calculating 
    for location, weather in zip(coordinates, tmys):
        #The zip() function takes iterables (can be zero or more), aggregates them in a tuple, and returns it.
        lat, long, id_city, alt = location
        system['surface_tilt'] = opt_tilt_angle
        solpos = pvlib.solarposition.get_solarposition(
            time=weather.index,
            latitude=lat,
            longitude=long,
            altitude=alt,
            temperature=weather["temp_air"],
            pressure=pvlib.atmosphere.alt2pres(alt),
        )
        dni_extra = pvlib.irradiance.get_extra_radiation(weather.index)
        airmass = pvlib.atmosphere.get_relative_airmass(solpos['apparent_zenith'])
        pressure = pvlib.atmosphere.alt2pres(alt)
        am_abs = pvlib.atmosphere.get_absolute_airmass(airmass, pressure)
        aoi = pvlib.irradiance.aoi(
            system['surface_tilt'],
            system['surface_azimuth'],
            solpos["apparent_zenith"],
            solpos["azimuth"],
        )
        total_irradiance = pvlib.irradiance.get_total_irradiance(
            system['surface_tilt'],
            system['surface_azimuth'],
            solpos['apparent_zenith'],
            solpos['azimuth'],
            weather['dni'],
            weather['ghi'],
            weather['dhi'],
            dni_extra=dni_extra,
            model='haydavies',
        )
        cell_temperature = pvlib.temperature.sapm_cell(
            total_irradiance['poa_global'],
            weather["temp_air"],
            weather["wind_speed"],
            **temperature_model_parameters, 
            #The **kwargs will give you all keyword arguments except
            # for those corresponding to a formal parameter as a dictionary.
        )
        effective_irradiance = pvlib.pvsystem.sapm_effective_irradiance(
            total_irradiance['poa_direct'],
            total_irradiance['poa_diffuse'],
            am_abs,
            aoi,
            module,
        )
        dc = pvlib.pvsystem.sapm(effective_irradiance, cell_temperature, module) #generates 5 points on a PV module
        dc = dc.fillna(0) #NaN values to 0 values
        energy_dc_timestamp = dc[['p_mp']]
        return energy_dc_timestamp