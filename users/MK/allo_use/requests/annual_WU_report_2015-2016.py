# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv, DataFrame, concat, read_hdf, to_datetime, to_numeric, Timestamp, Grouper
from core.allo_use import status_codes, allo_ts_proc
from numpy import in1d

#################################
### Parameters

allo_csv = 'S:/Surface Water/shared/base_data/usage/allo_gis.csv'
usage_file = 'S:/Surface Water/shared/base_data/usage/usage_daily.h5'

base_dir = r'C:\ecan\local\Projects\requests\annual_WU_report\2015-2016'

start = '2000-07-01'
end = '2016-06-30'

export_path = r'C:\ecan\local\Projects\requests\annual_WU_report\2015-2016\zeb_stats.csv'


#################################
### Read in allocation and filter to active consents

allo = read_csv(allo_csv)
allo.loc[:, 'from_date'] = to_datetime(allo.loc[:, 'from_date'], errors='coerce')
allo.loc[:, 'to_date'] = to_datetime(allo.loc[:, 'to_date'], errors='coerce')

use = read_hdf(usage_file)

allo1 = allo[(allo.status_details != 'Issued -Inactive') & (allo.daily_vol.notnull())]
allo2 = allo1[(allo1.from_date < end) & (allo1.to_date > start)]
allo3 = allo2[((allo2.in_gw_allo == 'YES') & (allo2.take_type == 'Take Groundwater') | (allo2.in_sw_allo == 'YES') & (allo2.take_type == 'Take Surface Water'))]

use1 = use[(use.date <= end) & (use.date >= start)]
use1a = use1.groupby([Grouper(key='wap'), Grouper(key='date', freq='M')]).sum().reset_index()

allo4 = allo3[in1d(allo3.wap, use1.wap.unique())]
use2 = use1[in1d(use1.wap, allo4.wap.unique())]
use2.columns = ['wap', 'time', 'usage']

################################
### Aggregate the data

## allo_block

grp1 = allo4.groupby(['crc', 'take_type', 'wap'])
vol1 = grp1[['daily_vol', 'cav', 'cav_crc']].sum()
wap_count = vol1.groupby(level='wap')['daily_vol'].count()[3:].reset_index()
wap_count.columns = ['wap', 'wap_count']

allo5 = allo4.copy()
enough_start = (allo5.to_date - Timestamp(start)).dt.days
enough_days = (allo5.to_date - allo5.from_date).dt.days

allo5 = allo5.loc[(enough_start > 31) & (enough_days > 31)]
allo5 = allo5[(allo5.use_type != 'hydroelectric')]

allo6 = allo5.set_index(['crc', 'take_type', 'wap'])

##################################
### Create allo ts and merge with usage data

## Daily
allo_ts_daily = allo6.apply(lambda x: allo_ts_proc(x, start_date=start, end_date=end, freq='D'), axis=1)
allo_ts_daily.columns.name = 'time'

allo_ts_daily1 = allo_ts_daily.stack()
allo_ts_daily1.name = 'allo_daily'
allo_ts_daily2 = allo_ts_daily1.reset_index()

allo_use_daily = merge(allo_ts_daily2, use2, on=['wap', 'time'], how='left')

grp3 = allo_use_daily.groupby(['crc', 'take_type', 'wap', 'time'])
day_count = grp3['usage'].transform('count')

allo_use_daily.loc[:, 'usage'] = allo_use_daily.loc[:, 'usage'] / day_count

## Monthly
#allo_ts_mon = allo6.apply(lambda x: allo_ts_proc(x, freq='M'), axis=1)
#allo_ts_mon.columns.name = 'time'
#
#allo_ts_mon1 = allo_ts_mon.stack()
#allo_ts_mon1.name = 'allo_mon'
#allo_ts_mon2 = allo_ts_mon1.reset_index()
#
#use_mon = allo_use_daily.groupby([Grouper(key='crc'), Grouper(key='take_type'), Grouper(key='wap'), Grouper(key='time', freq='M')])['usage'].sum().reset_index()
#
#allo_use_mon = merge(allo_ts_mon2, use_mon, on=['crc', 'take_type', 'wap', 'time'], how='left')


## Yearly
allo_ts_yr = allo6.apply(lambda x: allo_ts_proc(x, start_date=start, end_date=end, freq='A'), axis=1)
allo_ts_yr.columns.name = 'time'

allo_ts_yr1 = allo_ts_yr.stack()
allo_ts_yr1.name = 'allo_yr'
allo_ts_yr2 = allo_ts_yr1.reset_index()

use_yr = allo_use_daily.groupby([Grouper(key='crc'), Grouper(key='take_type'), Grouper(key='wap'), Grouper(key='time', freq='A-JUN')])['usage'].sum().reset_index()

allo_use_yr = merge(allo_ts_yr2, use_yr, on=['crc', 'take_type', 'wap', 'time'], how='left')






####################################
### frequency stats by consent

## Daily
daily_sum = allo_use_daily.groupby(['crc', 'take_type', 'time']).sum().reset_index()

daily_sum1 = daily_sum[in1d(daily_sum.time.dt.month, [10, 11, 12, 1, 2, 3, 4])]

daily_sum1['freq'] = daily_sum1['allo_daily'] * 0.9 <= daily_sum1['usage']
daily_sum1['ratio'] = (daily_sum1['usage'] / daily_sum1['allo_daily'])

daily_freq = (daily_sum1.groupby(['crc', 'take_type'])['freq'].sum()/212.0).round(2)
daily_freq.name = 'daily_freq_ratio'
daily_ratio = (daily_sum1.groupby(['crc', 'take_type'])['ratio'].mean()).round(2)
daily_ratio.name = 'daily_vol_ratio'

## Yearly
yr_sum1 = allo_use_yr.groupby(['crc', 'take_type', 'time']).sum().reset_index()

#yr_sum1['freq'] = yr_sum1['allo_yr'] * 0.9 <= yr_sum1['usage']
yr_sum1['ratio'] = (yr_sum1['usage'] / yr_sum1['allo_yr'])

#yr_freq = (yr_sum1.groupby(['crc', 'take_type'])['freq'].sum()/212.0).round(2)
yr_ratio = (yr_sum1.groupby(['crc', 'take_type'])['ratio'].mean()).round(2)
yr_ratio.name = 'yr_vol_ratio'

## Prepare output

allo7 = allo5.groupby(['crc', 'take_type']).first()[['min_flow', 'cav', 'cav_crc', 'max_rate_crc', 'max_vol_crc', 'catch_name', 'catch_grp', 'catch_grp_name', 'cwms', 'gwaz', 'swaz_grp', 'swaz']]

all_allo = concat([allo7, daily_freq, daily_ratio, yr_ratio], axis=1)

#all_allo.to_csv(export_path)


#################################
### Prepare data for plotting in R

grp9 = allo3.groupby(['crc', 'take_type', 'use_type', 'cwms'])
sum1 = grp9[['cav']].sum()
qual1 = grp9[['from_month', 'from_date', 'to_date']].first()
input1 = concat([sum1, qual1], axis=1)

allo_yr = input1.apply(lambda x: allo_ts_proc(x, start_date=start, end_date=end, freq='A'), axis=1)
allo_yr.columns.name = 'time'
allo_yr1 = allo_yr.stack()
allo_yr1.name = 'allo'
allo_yr2 = allo_yr1.reset_index().drop('time', axis=1)

## Process the use types
allo_yr3 = allo_yr2[(allo_yr2.use_type != 'hydroelectric')]
allo_yr3.loc[allo_yr3.use_type == 'stockwater', 'use_type'] = 'irrigation'

## Aggregate by various types
take_cwms = allo_yr3.groupby(['take_type', 'cwms'])
take_cwms_sum = take_cwms.sum()
take_cwms_sum['tot'] = take_cwms_sum.groupby(level='cwms').transform('sum')
cant_take = take_cwms_sum.sum(level='take_type')
cant_take['cwms'] = 'Canterbury'
cant_take.set_index('cwms', append=True, inplace=True)
take_cwms_sum1 = concat([take_cwms_sum, cant_take])
take_cwms_sum1['ratio'] = take_cwms_sum1['allo'] / take_cwms_sum1['tot']

use_cwms = allo_yr3[(allo_yr3.use_type != 'other')].groupby(['use_type', 'cwms'])
use_cwms_sum = use_cwms.sum()
cant_use = use_cwms_sum.sum(level='use_type')
cant_use['cwms'] = 'Canterbury'
cant_use.set_index('cwms', append=True, inplace=True)
use_cwms_sum1 = concat([use_cwms_sum, cant_use])
use_cwms_sum1['tot'] = use_cwms_sum1.groupby(level='cwms').transform('sum')
use_cwms_sum1['ratio'] = use_cwms_sum1['allo'] / use_cwms_sum1['tot']

# Usage
usage = yr_sum1[yr_sum1.ratio < 2].drop('ratio', axis=1)
usage1 = usage.groupby('crc').sum()
cwms1 = allo5.groupby('crc')['cwms', 'use_type'].first()
usage_cwms = concat([cwms1, usage1], axis=1).reset_index()
usage_cwms = usage_cwms[(usage_cwms.use_type != 'hydroelectric') & (usage_cwms.use_type != 'other')]
usage_cwms.loc[usage_cwms.use_type == 'stockwater', 'use_type'] = 'irrigation'
usage_cwms.loc[usage_cwms.usage.isnull(), 'usage'] = 0
usage_cwms = usage_cwms.loc[usage_cwms.allo_yr.notnull()]
usage_cwms1 = usage_cwms.groupby(['cwms']).sum()
cant_usage = usage_cwms1.sum()
cant_usage['cwms'] = 'Canterbury'
cant_usage = cant_usage.set_index('cwms', append=True).reorder_levels(['cwms', 'use_type'])
usage_cwms2 = concat([usage_cwms1, cant_usage])
usage_cwms2['ratio'] = usage_cwms2['usage'] / usage_cwms2['allo_yr']

use_zone1 = usage_cwms2.groupby(level='use_type').transform('sum')
use_zone2 = use_zone1['usage'] / use_zone1['allo_yr']
use_zone = use_zone2.unstack('use_type')

zone_use1 = usage_cwms2.groupby(level='cwms').transform('sum')







#################################
### Save data

use_zone = usage_cwms2.unstack()
zone_use = usage_cwms2.unstack('cwms')




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







wap = allo6.loc[('CRC000047.3', 'Take Groundwater', 'N34/0143')].reset_index().loc[0]


wap = allo3[allo3.from_month == 'OCT'].loc[232]




a1 = allo_use_yr[(allo_use_yr.usage/allo_use_yr.allo_yr) < 2]
a2 = a1[a1.usage > 0]
a2.sum()
a3 = a1[a1.take_type == 'Take Groundwater']
a3.sum()






















































