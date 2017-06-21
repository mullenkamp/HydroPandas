# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

allo_col = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_col]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use1, allo_col=['ann_allo_m3'], catchment_num=688, export_path='C:/ecan/Projects/requests/suz_ash/ash_results_v02.csv')

q2 = w_query(allo_use1, allo_col=['ann_allo_m3'], years=[2014], use_type=['stockwater'], catchment_num=688, export=False,  export_path='C:/ecan/Projects/requests/suz_ash/ash_results.csv', debug=True)



