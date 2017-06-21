# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, read_hdf, Grouper, to_datetime
from core.allo_use import allo_query
from core.allo_use.plot import allo_plt, allo_multi_plot
from core.misc import printf
from core.spatial import sel_sites_poly
from datetime import date
from core.ecan_io import rd_sql
from geopandas import read_file

#################################
### Parameters

## import parameters
allo_use_csv = 'E:/ecan/shared/base_data/usage/allo_use_ts_mon_results.csv'
allo_gis_file= r'E:\ecan\shared\GIS_base\vector\allocations\allo_gis.shp'
study_area_shp = 'Study_area.shp'

## query parameters
#grp_by = ['dates', 'take_type']
#cwms_zone = 'all'
#swaz = 'all'
#allo_col = ['mon_vol']
#crc2 = 'all'
#take_type = ['Take Groundwater', 'Take Surface Water']
#use_type = 'all'
#catch_num = [646, 659, 651]
#crc_rem = ['CRC020653.3', 'CRC135853']
#sd_only = True

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'
#crc_csv = r'C:\ecan\local\Projects\requests\maureen\2017-03-20\selwyn_crc1.csv'

#take_type = ['Take Surface Water']
years = [2015]

## output parameters
base_path = r'E:\ecan\local\Projects\requests'
name = 'MarkT'
date = '2017-05-16'

base_path2 = path.join(base_path, name, date)

#export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path2, name + '_allo_use_ann.csv')
#export_use_path = path.join(base_path, name, date, name + '_daily_use.csv')

#allo_name = name + '_past_allo.png'
#use_name = name + '_allo_use.png'

if not path.exists(base_path2):
    makedirs(base_path2)

#export_shp = path.join(base_path, name, date, name + '_allo_use.shp')

#################################
### Read in allocation and usage data and merge data


### Read in input data to be used in the query

#crcs = read_csv(crc_csv).crc.unique()

#################################
### Query data

allo_gis = read_file(allo_gis_file)
area = read_file(path.join(base_path2, study_area_shp))
allo_use1 = read_csv(allo_use_csv)

sites1 = sel_sites_poly(allo_gis, area)

keys = ['crc', 'take_type', 'allo_block', 'wap']
#allo_use0 = allo_use1.set_index('date')

allo_use1.loc[:, 'date'] = to_datetime(allo_use1.loc[:, 'date'])

allo_use0 = allo_use1.groupby(keys + [Grouper(key='date', freq='A-JUN')]).sum().reset_index()

allo_use2 = merge(allo_use0, sites1[keys + ['max_rate']], on=keys)

#################################
### Export

allo_use2.to_csv(export_path, index=False)






