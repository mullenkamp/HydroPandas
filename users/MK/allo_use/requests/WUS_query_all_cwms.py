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
#cwms_zone = ['Christchurch - West Melton']

path = 'C:/ecan/Projects/requests/cwms/2015-2016/'
base_dir = 'C:/ecan/Projects/requests/water_use_press/2015-2016/set1'

## Filtering parameters

use_type = ['irrigation', 'industry', 'public_supply']
years = [2015]
grp1 = ['take_type', 'use_type']
allo_col = 'ann_allo_m3'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

## Change stockwater to irrigation

allo_use1.loc[allo_use1.use_type == 'stockwater', 'use_type'] = 'irrigation'

### List of cwms zones

cwms_zones = allo.cwms_zone.unique().tolist()

#################################
### Query data

#for i in cwms_zones:
#    q1 = w_query(allo_use1, grp_by=['dates', 'take_type'], allo_col=['ann_allo_m3', 'ann_allo_wqn_m3'], cwms_zone=[i], use_type=use_type, years=years, export_path=path + i + '.csv')
#
#q1 = w_query(allo_use1, grp_by=['dates', 'take_type'], allo_col=['ann_allo_m3', 'ann_allo_wqn_m3'], cwms_zone=cwms_zones, use_type=use_type, years=years, export_path=path + 'Canterbury' + '.csv')

## For the two group by types

for j in grp1:
    for i in cwms_zones:
        q1 = w_query(allo_use1, grp_by=j, allo_col=[allo_col], cwms_zone=[i], years=years, use_type=use_type, export_path=path + i + j + '.csv')[allo_col]
        if j == 'take_type':
            if 'zone_take' in locals():
                zone_take = concat([zone_take, q1], axis=1)
            else:
                zone_take = q1
        if j == 'use_type':
            if 'zone_use' in locals():
                zone_use = concat([zone_use, q1], axis=1)
            else:
                zone_use = q1

zone_take.columns = cwms_zones
zone_use.columns = cwms_zones

zone_take.fillna(0, inplace=True)
zone_use.fillna(0, inplace=True)

zone_take_sum = zone_take.sum()
zone_use_sum = zone_use.sum()

zone_take_perc = zone_take/zone_take_sum * 100
zone_use_perc = zone_use/zone_use_sum * 100

cant_take = zone_take.sum(axis=1)
cant_use = zone_use.sum(axis=1)

cant_take_sum = cant_take.sum()
cant_use_sum = cant_use.sum()

cant_take_perc = cant_take/cant_take_sum * 100
cant_use_perc = cant_use/cant_use_sum * 100

zone_take_perc = concat([zone_take_perc, cant_take_perc], axis=1)
zone_use_perc = concat([zone_use_perc, cant_use_perc], axis=1)

cwms_zones.extend(['Canterbury'])

zone_take_perc.columns = cwms_zones
zone_use_perc.columns = cwms_zones


take_zone = zone_take.transpose()
use_zone = zone_use.transpose()

take_zone_sum = take_zone.sum()
use_zone_sum = use_zone.sum()

take_zone_perc = take_zone/take_zone_sum * 100
use_zone_perc = use_zone/use_zone_sum * 100

for i in cwms_zones:
    q1 = w_query(allo_use1, grp_by=['dates'], allo_col=[allo_col], cwms_zone=[i], years=years, use_type=use_type, export=False)[['usage_m3', allo_col]]
    q1.loc[:, 'unused'] = q1[allo_col] - q1.usage_m3
    q2 = q1[['usage_m3', 'unused']].transpose()
    if 'zone_usage' in locals():
        zone_usage = concat([zone_usage, q2], axis=1)
    else:
        zone_usage = q2

zone_usage.columns = cwms_zones[0:-1]

zone_usage.fillna(0, inplace=True)

zone_usage_sum = zone_usage.sum()
zone_usage_perc = zone_usage/zone_usage_sum * 100

cant_usage = zone_usage.sum(axis=1)
cant_usage_sum = cant_usage.sum()
cant_usage_perc = cant_usage/cant_usage_sum * 100

zone_usage_perc = concat([zone_usage_perc, cant_usage_perc], axis=1)
zone_usage_perc.columns = cwms_zones


usage_zone = zone_usage.transpose()
usage_zone_sum = usage_zone.sum()
usage_zone_perc = usage_zone/usage_zone_sum * 100

### Save data

zone_take_perc.to_csv(base_dir + 'zone_take.csv')
zone_use_perc.to_csv(base_dir + 'zone_use.csv')
use_zone_perc.to_csv(base_dir + 'use_zone.csv')
take_zone_perc.to_csv(base_dir + 'take_zone.csv')

zone_usage_perc.to_csv(base_dir + 'zone_usage.csv')
usage_zone_perc.to_csv(base_dir + 'usage_zone.csv')

del zone_take
del zone_use
del zone_usage
