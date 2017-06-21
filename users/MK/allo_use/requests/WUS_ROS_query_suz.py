# -*- coding: utf-8 -*-
"""
Script to query the updated water use and allocation results.
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_v01 import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up2.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results2.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
#cwms_zone = ['Orari-Opihi-Pareora']

export_path = 'C:/ecan/Projects/otop/usage/ash_results_up1.csv'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates', 'take_type', 'use_type'], allo_col=['ann_allo_m3', 'ann_allo_wqn_m3', 'up_allo_m3', 'up_allo_wqn_m3'], catchment_num=688, export_path=export_path)

