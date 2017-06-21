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

## output parameters
base_path = 'E:/ecan/local/Projects/requests'
name = 'between_5-10_max_rate'
date = '2017-06-14'

export_path = path.join(base_path, name, date, name + '_allo.csv')
#export_use_path = path.join(base_path, name, date, name + '_daily_use.csv')

if not path.exists(path.dirname(export_path)):
    makedirs(path.dirname(export_path))


#################################
### Read in allocation and usage data and merge data

allo_gis = read_csv(allo_gis_csv)

#################################
### Query data

## Select only the consents that are active
allo2 = allo_gis[allo_gis.status_details.isin(['Issued - Active', 'Issued - s124 Continuance']) & (allo_gis.use_type != 'hydroelectric')]

## Aggregate to crc
data_col = ['max_rate', 'daily_vol', 'cav']
allo3 = allo2.groupby(['crc', 'take_type'])[data_col].sum().reset_index()

## Select consents under 10 l/s
allo_under = allo3[allo3.max_rate <= 10]

## Aggregate all!!!
cav_under = allo_under[data_col].sum()
cav_all = allo3[data_col].sum()

## Ratios
cav_under/cav_all


#################################
#### Output results

allo3.to_csv(export_path, index=False)

#############################
### Check oddities







