# -*- coding: utf-8 -*-
"""
Script to query the updated water use and allocation results.
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_v01 import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up2_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results2.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
cwms_zone = ['Ashburton']
years = [2015]
use_type = ['stockwater']
grp_by = ['dates']
allo_col = ['ann_allo_m3', 'up_allo_m3']

export_path = 'C:/ecan/Projects/requests/cwms/set2/ash_results_up_with_cav.csv'


#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use1, grp_by=grp_by, allo_col=allo_col, use_type=use_type, years=years, cwms_zone=cwms_zone, export_path=export_path, debug=True)




