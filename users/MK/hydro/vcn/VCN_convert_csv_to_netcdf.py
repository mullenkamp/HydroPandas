# -*- coding: utf-8 -*-
"""
Created on Mon Dec 05 15:37:01 2016

@author: michaelek
"""

#######################################
#### Convert VCSN data from csv to NetCDF

from pandas import read_csv, date_range, Timestamp, DataFrame
from import_fun import rd_vcn
import xarray as xr
from numpy import in1d, where, random

#######################################
#### Parameters

comp_table='Y:/VirtualClimate/VCN_id_comp_table.csv'
cant_shp = 'Y:/VirtualClimate/GIS/cant_regional_boundaries.shp'
output_nc = 'Y:/VirtualClimate/vcsn_precip_et_2016-06-06.nc'

export_path = 'Y:/VirtualClimate/vcsn_precip_et_test.nc'

######################################
#### Import data

comp_tab = read_csv(comp_table)
data = rd_vcn()

## Make new labels
labels = [comp_tab.loc[(comp_tab.ecan_id == i), 'net_id'].iloc[0] for i in data.columns]

data.index.name = 'time'
data.columns = labels
data.columns.name = 'site'

#### Make the xarray

x1 = xr.DataArray(data)











select = cant_shp
data_dir='Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
data_type='precip'
site_col='ID'
comp_table='Y:/VirtualClimate/VCN_id_comp_table.csv'
id_col=1
buffer_dis=20000
vcsn_grid_shp='S:/Surface Water/shared/GIS_base/vector/NIWA_rain_grid_Canterbury.shp'
vcsn_site_col='Data VCN_s'
export=True
export_path=output_nc
netcdf_export=True





















