# -*- coding: utf-8 -*-
"""
Script to query the updated water use and allocation results.
"""

from pandas import merge, read_csv, DataFrame
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
cwms_zone = ['Orari-Temuka-Opihi-Pareora']

export_path = 'C:/ecan/Projects/requests/econ_OTOP/otop_results_up_with_cav_v04.csv'

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

q1 = w_query(allo_use1, grp_by=['dates', 'take_type', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, export_path=export_path)


q1 = w_query(allo_use1, grp_by=['dates', 'take_type', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, export_path=export_path, export=True, years=[2015], use_type=['other'], take_type=['Take Surface Water'], debug=True)



q2 = w_query(allo_use1, grp_by=['dates', 'take_type', 'use_type'], allo_col=allo_col, crc=['CRC101667'], cwms_zone=cwms_zone, export_path=export_path, export=False, years=[2014, 2015], debug=True)


cols = ['crc', 'wap', 'dates', 'usage_m3', 'ann_allo_m3']

other1 = q1.loc[(q1.use_type == 'other') & (q1.dates == '1998-06-30') & (q1.take_type == 'Take Surface Water'), cols]
other2 = q1.loc[(q1.use_type == 'other') & (q1.dates == '1999-06-30') & (q1.take_type == 'Take Surface Water'), cols]
other3 = q1.loc[(q1.use_type == 'other') & (q1.dates == '2006-06-30') & (q1.take_type == 'Take Surface Water'), cols]
other4 = q1.loc[(q1.use_type == 'other') & (q1.dates == '2007-06-30') & (q1.take_type == 'Take Surface Water'), cols]


