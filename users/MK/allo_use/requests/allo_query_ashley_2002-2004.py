# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, read_hdf
from core.allo_use import allo_query, allo_ts_proc
from core.allo_use.plot import allo_plt, allo_multi_plot
from core.misc import printf
from datetime import date
from core.ecan_io import rd_sql
from geopandas import read_file

#################################
### Parameters

## import parameters
allo_gis_csv = 'E:/ecan/shared/base_data/usage/allo_gis.csv'

## query parameters
swaz_grp = 'Ashley/Rakahuri'

start = '2001-07-01'
end = '2004-06-30'

## output parameters
base_path = 'E:/ecan/local/Projects/requests'
name = 'ashley_2002-2004'
date = '2017-06-12'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo.csv')
export_use_path = path.join(base_path, name, date, name + '_daily_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)

export_shp = path.join(base_path, name, date, name + '_allo_use.shp')

#################################
### Read in allocation and usage data and merge data

allo_gis = read_csv(allo_gis_csv)

#################################
### Query data

## Select only the consents within the Ashley swaz group
allo2 = allo_gis[allo_gis.swaz_grp == swaz_grp]

## Select the consents active during the specified dates
allo_ts_ann = allo_ts_proc(allo2, start=start, end=end, freq='A')
allo_ts_ann.rename(columns={'allo': 'ann_allo_m3'}, inplace=True)

## Extract the allocation info from allo
allo3 = merge(allo_ts_ann, allo2, on=[u'crc', u'take_type', u'allo_block', u'wap'], how='left')

allo3.loc[:, 'daily_rate'] = (allo3.loc[:, 'daily_vol'] * 1000 / 24/60/60).round(2)


#################################
#### Output results

allo3.to_csv(export_path, index=False)

#############################
### Check oddities







