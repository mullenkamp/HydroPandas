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

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']

path = 'C:/ecan/Projects/requests/adam/'
sites_csv = 'waimakariri_crc.csv'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

sites = read_csv(path + sites_csv)
crc1 = sites['crc'].drop_duplicates().values.tolist()
wap1 = sites['wap'].drop_duplicates().values.tolist()


#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates'], allo_col=['ann_allo_m3'], crc=crc1, wap=wap1, export_path=path + 'results_v02.csv')





