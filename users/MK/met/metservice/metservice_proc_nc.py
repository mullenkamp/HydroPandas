# -*- coding: utf-8 -*-
"""
Created on Fri Jul 07 09:09:11 2017

@author: MichaelEK

Script to process the metservice netcdf files.
"""

from core.ts.met.metservice import proc_metservice_nc, MetS_nc_to_df
from core.ts.met.interp import sel_interp_agg

##########################################
### Parameters

nc = r'E:\ecan\shared\base_data\metservice\testing\nz8kmN-NCEP_2_2017050918.00.nc'
poly = r'E:\ecan\shared\base_data\metservice\testing\study_area.shp'

time_col = 'time'
y_col = 'y'
x_col = 'x'
data_col = 'precip'
buffer_dis = 10000
grid_res = 1000

export = r'E:\ecan\shared\base_data\metservice\testing\out_test1.csv'

#########################################
### Processes

new_nc = proc_metservice_nc(nc)

precip, sites, start_date = MetS_nc_to_df(new_nc)

new_precip = sel_interp_agg(precip, sites.crs, poly, grid_res, data_col, time_col, x_col, y_col, buffer_dis, agg_xy=True)

new_precip.to_csv(export)
