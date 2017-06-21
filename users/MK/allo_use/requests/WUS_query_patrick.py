# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
#cwms_zone = ['Orari-Temuka-Opihi-Pareora']
take_type = ['Take Groundwater']

export_path = 'C:/ecan/Projects/requests/patrick/Cant_allo_use_v01.csv'
export_path2 = 'C:/ecan/Projects/requests/patrick/Cant_allo_use_gw_zone_v01.csv'

export_debug_path = 'C:/ecan/Projects/requests/patrick/Cant_allo_use_other_v01.csv'
#crc_rem = ['CRC091757', 'CRC151081']

allo_col = ['ann_allo_m3', 'ann_up_allo_m3']
#allo_col = ['ann_allo_m3']

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates', 'use_type'], take_type=take_type, allo_col=allo_col, export_path=export_path)

q3 = w_query(allo_use1, grp_by=['dates', 'use_type', 'gw_zone'], take_type=take_type, allo_col=allo_col, export_path=export_path2)

q2 = w_query(allo_use1, grp_by=['dates'], take_type=take_type, allo_col=allo_col, use_type=['other'], years=[2015], debug=True, export=False, export_path=export_debug_path)

q2.sort_values('ann_allo_m3', ascending=False)

