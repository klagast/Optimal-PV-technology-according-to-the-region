#based on a research at Stanford University 
#called "World estimates of PV optimal tilt angles and ratios of sunlight incident upon tilted and tracked PV panels relative to horizontal panels"
#optimal tilt angle of a pv panel
#they estimate that the surface azimuth is 0° in southern hemisphere or 180° in northern hempisphere


def optimal_tilt_angle(lat):
    if lat > 0: #northern hemisphere
        tilt_angle = 1.3793 + lat*(1.2011 + lat*(-0.014404 + lat*0.000080509))
    if lat < 0: #southern hemisphere
        tilt_angle = -0.41657 + lat*(1.4216 + lat*(0.024051 + lat*0.00021828))
    
    return tilt_angle

