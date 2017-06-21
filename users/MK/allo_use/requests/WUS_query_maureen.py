# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_v01 import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up2.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results2.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
#cwms_zone = ['Christchurch - West Melton']

path = 'C:/ecan/Projects/requests/maureen/'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

sites = read_csv(path + 'CC2.csv')
sites2 = sites['RecordNo'].drop_duplicates().values.tolist()

#################################
### Query data

for i in sites2:
    q1 = w_query(allo_use1, grp_by=['dates'], allo_col=['ann_allo_m3'], crc=[i], export_path=path + i + '.csv')





