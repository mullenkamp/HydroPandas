# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 1/08/2017 11:36 AM
"""

from __future__ import division
from core import env
from core.ecan_io import rd_niwa_vcsn
from time import time
from core.ts.gw.lsrm import poly_import, input_processing, lsrm

#############################################
#### Parameters

### Reading data
irr_type_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'AQUALINC_NZTM_IRRIGATED_AREA_20160629', 'column': 'type'}
paw_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'LAND_NZTM_NEWZEALANDFUNDAMENTALSOILS', 'column': 'PAW_MID'}

bound_shp = r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\supporting_data_for_scripts\ex_bd_va_sdp\m_ex_bd_inputs\shp\model_grid_bound.shp" #todo

### input data processing
rain_name = 'rain'
pet_name = 'pe'

time_agg = 'A-JUN'
agg_ts_fun = 'sum'
buffer_dis = 10000
grid_res = 200
crs = 4326
interp_fun = 'cubic'
paw_ratio = 0.67
precip_correction = 1.1
min_irr_area_ratio = 0.01

irr_mons = [10, 11, 12, 1, 2, 3, 4]

irr_eff_dict = {'Drip/micro': 1, 'Unknown': 0.8, 'Gun': 0.8, 'Pivot': 0.8, 'K-line/Long lateral': 0.8, 'Rotorainer': 0.8, 'Solid set': 0.8}
irr_trig_dict = {'Drip/micro': 0.7, 'Unknown': 0.5, 'Gun': 0.5, 'Pivot': 0.5, 'K-line/Long lateral': 0.5, 'Rotorainer': 0.5, 'Solid set': 0.5}

### Model parameters
A = 6

### Output parameters
out_dir = r"C:\Users\MattH\Downloads\david_scott_model"
output_csv = '{}\output_model_domain.csv'.format(out_dir) #todo
output_shp = r'E:\ecan\shared\projects\lsrm\output_model_domain.shp' #todo

###########################################
### Extract data

print('Read in the input data')

irr1, paw1 = poly_import(irr_type_dict, paw_dict, paw_ratio)

precip_et = rd_niwa_vcsn(['precip', 'PET'], bound_shp, buffer_dis=buffer_dis)

##########################################
### Process data
## Resample met data data

print('Process the input data for the LSR model')

model_var, sites_poly = input_processing(precip_et, crs, irr1, paw1, bound_shp, rain_name, pet_name, grid_res, buffer_dis, interp_fun, agg_ts_fun, time_agg, irr_eff_dict, irr_trig_dict, min_irr_area_ratio, irr_mons, precip_correction)

#########################################################
#### Run the model

print('Run the LSR model')

output1 = lsrm(model_var, A)

#######################################################
#### Output data

print('Saving data')

output1.to_csv(output_csv, index=False)
sites_poly.reset_index().to_file(output_shp)






