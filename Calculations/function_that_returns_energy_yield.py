import pvlib
import pandas as pd
import psycopg2
import sqlalchemy

#code is based on the a code on the pvlib website


#Function calculating yearly energy DC yield, energy yield per square meter and energy yield divided by the watt-peak
def energy_yield(id_city, long, lat, alt, mod, opt_tilt_angle):
    
    
    id_city = int(id_city)
    coordinates = [(lat, long, id_city , alt),]
    
    #module and temperature of PV cell of the Sandia Array Performance Model
    sandia_modules = pvlib.pvsystem.retrieve_sam('SandiaMod') #returns the Sandia Module database
    module = sandia_modules[mod] 
    # the temperature_model_parmaters are chosen the same for every PV module
    temperature_model_parameters = pvlib.temperature.TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_polymer']


    #establishing the connection with the database, make your own connection
    conn = psycopg2.connect(database='?', user='?', password='?', host='?', port= '?')
    engine = sqlalchemy.create_engine('postgresql+psycopg2://', creator=lambda: conn)    
    query = "SELECT time_utc, temp_air, relative_humidity, ghi, dni, dhi, ir_h, wind_speed, wind_direction, pressure FROM tmy_cities WHERE city_id = %s"
    weather = pd.read_sql(query, engine, params=[id_city])
    weather = weather.set_index('time_utc')
    weather.index.name = "time(UTC)"

    #making an empty list to store the Typical Meteorological Year
    tmys = []
    tmys.append(weather)

        
    #define the surface azimuth
    #northern hemisphere -> panel facing south
    #southern hemisphere -> panel facing north
    surf_azimuth = 0
    if lat > 0:
        surf_azimuth = 180
    if lat < 0:
        surf_azimuth = 0

    #defining a PV system
    system = {'module': module,
          'surface_azimuth': surf_azimuth} 
        


    #calculations
    for location, weather in zip(coordinates, tmys):
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
        )
        effective_irradiance = pvlib.pvsystem.sapm_effective_irradiance(
            total_irradiance['poa_direct'],
            total_irradiance['poa_diffuse'],
            am_abs,
            aoi,
            module,
        )
        dc = pvlib.pvsystem.sapm(effective_irradiance, cell_temperature, module) #generates 5 points on a I/V curve of the PV module
        
        dc = dc.fillna(0) #NaN values to 0 values
        dc_maximum_power = dc[['p_mp']].values
        annual_energy_dc = dc_maximum_power.sum()

        #dc yield per square meter
        annual_energy_dc_m2 = annual_energy_dc / module["Area"]

        #dc yield per square meter divided by watt peak
        watt_peak = module["Impo"] * module["Vmpo"]
        annual_energy_dc_wpeak = annual_energy_dc / watt_peak


        return annual_energy_dc, annual_energy_dc_m2, annual_energy_dc_wpeak