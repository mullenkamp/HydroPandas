# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 10:42:42 2017

@author: MichaelEK
"""

from core.ecan_io import rd_niwa_vcsn, rd_sql
from core.ts.met.interp import poly_interp_agg
from pandas import merge, concat, to_datetime, DataFrame
from geopandas import read_file, GeoDataFrame, overlay
from core.spatial.vector import xy_to_gpd, points_grid_to_poly
from shapely.geometry import Point, Polygon
from numpy import tile, nan, exp, full, arange, array, seterr
from time import time
from core.ts.gw.lsrm import poly_import, input_processing, lsrm

#############################################
#### Parameters

### Reading data
irr_type_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'AQUALINC_NZTM_IRRIGATED_AREA_20160629', 'column': 'type'}
paw_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'LAND_NZTM_NEWZEALANDFUNDAMENTALSOILS', 'column': 'PAW_MID'}

bound_shp = r'E:\ecan\shared\projects\lsrm\gis\waipara.shp'

### input data processing
rain_name = 'rain'
pet_name = 'pe'

time_agg = 'W'
agg_ts_fun = 'sum'
buffer_dis = 10000
grid_res = 1000
crs = 4326
interp_fun = 'cubic'

min_irr_area_ratio = 0.01

irr_mons = [10, 11, 12, 1, 2, 3, 4]

irr_eff_dict = {'Drip/micro': 1, 'Unknown': 0.8, 'Gun': 0.8, 'Pivot': 0.8, 'K-line/Long lateral': 0.8, 'Rotorainer': 0.8, 'Solid set': 0.8}
irr_trig_dict = {'Drip/micro': 0.7, 'Unknown': 0.5, 'Gun': 0.5, 'Pivot': 0.5, 'K-line/Long lateral': 0.5, 'Rotorainer': 0.5, 'Solid set': 0.5}

### Model parameters
A = 6

### Output parameters
output_csv = r'E:\ecan\shared\projects\lsrm\output_waipara1.csv'
output_shp = r'E:\ecan\shared\projects\lsrm\output_waipara1.shp'

##########################################
### Functions

def AET(pet, A, paw_now, paw_max):
    """
    Minhas et al. (1974) function used by David Scott to estimate 'actual ET' from PET and PAW. All inputs must be as floats.
    """

    aet = pet * ((1 - exp(-A*paw_now/paw_max))/(1 - 2*exp(-A) + exp(-A*paw_now/paw_max)))
    return(aet)


###########################################
### Extract data

print('Read in the input data')

irr1, paw1 = poly_import(irr_type_dict, paw_dict)

precip_et = rd_niwa_vcsn(['precip', 'PET'], bound_shp, buffer_dis=buffer_dis)

##########################################
### Process data
## Resample met data data

print('Process the input data for the LSR model')

model_var = input_processing(precip_et, crs, irr1, paw1, bound_shp, rain_name, pet_name, grid_res, buffer_dis, interp_fun, agg_ts_fun, time_agg, irr_eff_dict, irr_trig_dict, min_irr_area_ratio, irr_mons)

#########################################################
#### Run the model

print('Run the LSR model')

output1 = lsrm(model_var, A)

#######################################################
#### Output data

print('Saving data')

output1.to_csv(output_csv, index=False)
sites_poly2.reset_index().to_file(output_shp)






