# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results2.csv'

allo_col = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
cwms_zone = ['Orari-Opihi-Pareora']

export_path = 'C:/ecan/Projects/otop/usage/otop_results2.csv'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_col]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates', 'take_type'], cwms_zone=cwms_zone, export_path=export_path)





