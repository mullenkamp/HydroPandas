# -*- coding: utf-8 -*-
"""
Created on Fri Jul 07 09:09:11 2017

@author: MichaelEK

Script to process the metservice netcdf files.
Still needs to have the output of the sel_interp_agg function saved to the sql database!
"""

from core.ts.met import proc_metservice_nc, MetS_nc_to_df, sel_interp_agg

##########################################
### Parameters

nc = r'T:\Temp\MikeK\metservice\nz8kmN-NCEP_2_2017050918.00.nc'
poly = r'T:\Temp\MikeK\metservice\study_area.shp'

time_col = 'time'
y_col = 'y'
x_col = 'x'
data_col = 'precip'
buffer_dis = 10000
grid_res = 500

#########################################
### Processes

new_nc = proc_metservice_nc(nc)

precip, sites = MetS_nc_to_df(new_nc)

new_precip = sel_interp_agg(precip, sites.crs, poly, grid_res, data_col, time_col, x_col, y_col, buffer_dis, agg_xy=True)
