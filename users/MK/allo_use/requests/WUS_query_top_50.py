# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame, concat
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

path = 'C:/ecan/Projects/requests/top_50/top_100_crc_v02.csv'

### Read in input data to be used in the query

#################################
### Query data

#q1 = w_query(allo_use1, grp_by=['dates'], allo_col=['ann_allo_m3'], export=False, export_path=path + 'results.csv', debug=True)

years=[2015]

yrs_index = allo_use1.dates.astype('str').str[0:4].astype('int')
df1 = allo_use1[yrs_index.isin(years)]

df2 = df1[['crc', 'usage_m3', 'ann_allo_m3']].groupby('crc').sum()

df3 = df1.groupby('crc').first()

df4 = concat([df2, df3.drop(['usage_m3', 'ann_allo_m3', 'ann_allo_wqn_m3', 'wap', 'dates'], axis=1)], axis=1)

df5 = df4.sort_values('ann_allo_m3', ascending=False)

df6 = df5[0:100]

df6.to_csv(path)


