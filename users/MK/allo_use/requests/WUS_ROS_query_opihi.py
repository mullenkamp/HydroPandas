# -*- coding: utf-8 -*-
"""
Script to query the updated water use and allocation results.
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_fun import w_query, ros_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
cwms_zone = ['Orari-Temuka-Opihi-Pareora']
#catchment_num = 696

export_path = 'C:/ecan/Projects/otop/usage/opihi_gauge_band_date_restr_v01.csv'
export_summ_path = 'C:/ecan/Projects/otop/usage/opihi_gauge_band_restr_v02.csv'

#crc_rem = ['CRC091757', 'CRC151081']

#allo_col = ['ann_allo_m3', 'ann_up_allo_m3']
#allo_col = ['ann_allo_m3']

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = ros_query(allo_use1, catchment_num=catchment_num, export_path=export_path)
q1 = ros_query(allo_use1, cwms_zone=cwms_zone, export_path=export_path)

q1['band_restr_vol'] = q1.ann_up_allo_m3 / q1.ann_allo_m3

q2 = q1.band_restr_vol.reset_index().groupby(['gauge_num', 'band_num']).mean().round(3)
q2.to_csv(export_summ_path)
