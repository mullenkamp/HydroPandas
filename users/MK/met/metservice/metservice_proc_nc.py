# -*- coding: utf-8 -*-
"""
Created on Fri Jul 07 09:09:11 2017

@author: MichaelEK

Script to process the metservice netcdf files.
"""

from core.ts.met.metservice import proc_metservice_nc, MetS_nc_to_df
from core.ts.met.interp import poly_interp_agg
from core.spatial import sel_sites_poly, point_interp_ts
from geopandas import read_file
from pandas import to_datetime, concat

##########################################
### Parameters

nc = r'E:\ecan\shared\base_data\metservice\testing\nz8kmN-NCEP_2_2017050918.00.nc'
poly = r'E:\ecan\shared\base_data\metservice\testing\study_area.shp'

poly_site_col = 'OBJECTID'

time_col = 'time'
y_col = 'y'
x_col = 'x'
data_col = 'precip'
buffer_dis = 10000
grid_res = 1000
digits = 2

rename_dict = {'site': 'MetConnectID', 'model_date': 'PredictionDateTime', 'time': 'ReadingDateTime', 'precip': 'HourlyRainfall'}

export = r'E:\ecan\shared\base_data\metservice\testing\out_test1.csv'

#########################################
### Processes

## preprocess the nc file to save it as a proper nc file
new_nc = proc_metservice_nc(nc)

## extract the data from the new nc file
precip, sites, start_date = MetS_nc_to_df(new_nc)

start_date1 = to_datetime(start_date, format='%Y-%m-%d_%H:%M:%S')

## Iterate through the polygons to extract the data
poly1 = read_file(poly)

data_lst = []
for i in poly1.index:
    poly2 = poly1.loc[[i]]
    site_id = poly2.loc[i, poly_site_col]

    new_precip = sel_interp_agg(precip, sites.crs, poly2, grid_res, data_col, time_col, x_col, y_col, buffer_dis, agg_xy=True, digits=digits)

    new_precip1 = new_precip.reset_index()
    new_precip1.loc[:, 'site'] = site_id
    new_precip1.loc[:, 'model_date'] = start_date1
    new_precip2 = new_precip1[['site', 'model_date', 'time', 'precip']]

    data_lst.append(new_precip2)

data = concat(data_lst)
data.rename(columns=rename_dict, inplace=True)

data.to_csv(export, index=False)
