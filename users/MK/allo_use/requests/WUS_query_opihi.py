# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_v01 import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_mon_series_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results2.csv'

allo_col = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
cwms_zone = ['Orari-Opihi-Pareora']

export_path = 'C:/ecan/Projects/otop/usage/opihi_results1.csv'

wap_csv = 'C:/ecan/Projects/otop/usage/wap_sites/wap_69618.csv'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
series.columns.values[1] = 'dates'
allo1 = read_csv(allo_csv)[allo_col]

allo_use1 = merge(series, allo1, on=['crc', 'wap'])

### Read in input data to be used in the query

waps = read_csv(wap_csv)['wap'].unique()

#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates'], allo_col=['mon_allo'], use_col=['usage'], wap=waps.tolist(), take_type=['Take Surface Water'], export_path=export_path)

q1 = w_query(allo_use1, grp_by=['dates'], allo_col=['mon_allo'], use_col=['usage'], cwms_zone=cwms_zone, catchment_num=701, take_type=['Take Surface Water'], use_type=['irrigation'], export=False, debug=True)

q1[q1.usage.notnull()].wap.unique()
q1.wap.unique()













