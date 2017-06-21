# -*- coding: utf-8 -*-
"""
Created on Fri Jan 06 14:46:52 2017

@author: michaelek
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat
from import_fun import rd_ts, rd_sql, rd_site_geo
from spatial_fun import catch_net, pts_poly_join, flow_sites_to_shp, agg_catch
from geopandas import read_file
from ts_stats_fun import malf7d, flow_stats, flow_reg
from numpy import nan, log, argmax, append, in1d


############################################
#### Parameters

allo_csv = 'C:/ecan/shared/base_data/usage/allo_results.csv'
post_year = '2017'

rate_grps = [5, 10, 20]

## Export
export_stats = r'C:\ecan\local\Projects\requests\crc_grps\stats_no_hydroelectric.csv'
export_results = r'C:\ecan\local\Projects\requests\crc_grps\crc_allo_active.csv'


##################################
#### Read in data
allo = read_csv(allo_csv)

##############################
#### Process the allo data

to_date = to_datetime(allo.to_date)

allo1 = allo[to_date > post_year]
allo1.loc[allo1.max_rate <= 0, 'max_rate'] = (allo1.loc[allo1.max_rate == 0, 'daily_vol']/24/60/60*1000).round(1)
allo1.loc[:, 'daily_vol'] = allo1.loc[:, 'daily_vol'].round(2)
allo1 = allo1[allo1.use_type != 'hydroelectric']

##############################
#### Break into groups

grp1 = allo1[(allo1.max_rate > 5) & (allo1.max_rate <= 10)]
grp2 = allo1[(allo1.max_rate > 10) & (allo1.max_rate <= 20)]
grp3 = allo1[(allo1.max_rate > 20)]

#############################
#### Summarize data

stats1 = grp1[['max_rate', 'daily_vol', 'ann_vol']].sum()
stats2 = grp2[['max_rate', 'daily_vol', 'ann_vol']].sum()
stats3 = grp3[['max_rate', 'daily_vol', 'ann_vol']].sum()

all_stats = concat([stats1, stats2, stats3], axis=1).round()
all_stats.columns = ['5-10', '10-20', '> 20']

all_stats_sum = all_stats.sum(axis=1)
all_stats_ratios = all_stats.divide(all_stats_sum, axis=0).round(4)

#############################
#### Reorganize for export

stats_set = concat([all_stats, all_stats_ratios], axis=1)

stats_set.to_csv(export_stats)
allo1.to_csv(export_results, index=False)






############################
#### Testing




































