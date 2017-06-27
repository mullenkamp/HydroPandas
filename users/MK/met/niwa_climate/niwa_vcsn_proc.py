# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 13:12:27 2017

@author: MichaelEK

Script to take the vcsn csv files and repackage them into a proper netcdf file equivelant to NIWA's netcdf files.
"""

from pandas import read_csv, concat, merge
from core.misc import rd_dir
from os import path
from numpy import tile, repeat

######################################
#### Parameters

comp_table_csv = 'Y:/VirtualClimate/VCN_id_comp_table.csv'
data_dir = 'Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
vcsn_sites_csv = r'Z:\Data\VirtualClimate\GIS\niwa_vcsn_wgs84.csv'

data_attr = {'grid_mapping': 'crs'}
nc_crs = {'inverse_flattening': 298.257223563, 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'grid_mapping_name': 'latitude_longitude'}
time_attr = {'bounds': 'time_bounds', 'standard_name': 'time', 'axis': 'T', 'long_name': 'time (end of reporting period)'}

#####################################
#### Read data

comp_table = read_csv(comp_table_csv)
vcsn_sites = read_csv(vcsn_sites_csv)[['Network', 'deg_x', 'deg_y']]
vcsn_sites.columns = ['site', 'longitude', 'latitude']
files, num = rd_dir(data_dir, 'csv', True)

####################################
#### Get and organize data

comp_table2 = comp_table.set_index('ecan_id')
sites = comp_table2.loc[num, 'net_id']

df1 = concat((read_csv(path.join(data_dir, f), index_col=0, parse_dates=True, infer_datetime_format=True) for f in files))

n_data = len(df1)/len(sites)

sites2 = repeat(sites.values, n_data)

df2 = df1.copy().reset_index()
df2['site'] = sites2

df3 = merge(df2, vcsn_sites, on='site', how='left')
df3.rename(columns={'date': 'time', 'precip': 'rain', 'ET': 'pe'}, inplace=True)
df4 = df3.set_index(['longitude', 'latitude', 'time'])

sites_df = df3[['site', 'longitude', 'latitude']].drop_duplicates().set_index(['longitude', 'latitude'])

ds_sites = sites_df.to_xarray()
ds1 = df4[[]].to_xarray()




































