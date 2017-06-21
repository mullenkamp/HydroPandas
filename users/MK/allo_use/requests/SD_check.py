# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:55:47 2016

@author: MichaelEK
"""

from numpy import nan, logical_and, where, round, array, in1d, concatenate, isreal, floor, logical_or, sum
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from MSSQL_import_fun import rd_sql
from WA_analysis_fun import w_use_proc, allo_proc, est_wap_use, ros_proc, wap_sd, sd_et_reg
from import_hydstra_fun import rd_hydstra_csv
from ts_stats_fun import w_resample

########################################
##### Parameters

#### Fields and names for databases

### Allocation and usage

## Query fields - Be sure to use single quotes for the names!!!

wap_fields = ['PARENT_B1_ALT_ID', 'WellSwapNo', 'Allocation (l/s)']
sd_fields = ['Well_No', 'SD1_7', 'SD1_30', 'SD1_150']
sd2_fields = ['PARENT_B1_ALT_ID', 'Connection', 'Depletion Calc', 'Depletion Rate (l/s)', 'In Allocation', 'Min Flow', 'WellNo']
zone_fields = ['WELL_NO', 'CatchmentNumber']
wap_gis_fields = ['wap', 'take_type', 'SUB_REGION', 'CATCHMEN_1', 'CATCHMEN_2', 'Zone_Name']
takes_fields = ['PARENT_B1_ALT_ID', 'Max Rate (l/s)']
permit_fields = ['B1_ALT_ID', 'B1_APPL_STATUS', 'StatusType']

## Equivelant short names for analyses - Use these names!!!

wap_names = ['crc', 'wap', 'max_rate_wap']
sd_names = ['wap', 'sd_7', 'sd_30', 'sd_150']
sd2_names = ['crc', 'connection', 'dep_calc', 'dep_rate', 'in_allo', 'min_flow', 'wap']
zone_names = ['wap', 'catchment_num']
wap_gis_names = ['wap', 'take_type', 'subregion', 'catch_num', 'catch', 'gw_zone']
takes_names = ['crc', 'max_rate']
permit_names = ['crc', 'app_status', 'status']

#### Databases

### Allocation

# crc and WAPs with associated allocated rates

server3 = 'SQL2012PROD03'
database3 = 'DataWarehouse'

wap_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterWellsSwaps'
#wap_where = 'B1_PER_CATEGORY'
#wap_where_val = ['Take Groundwater']

# GW/SW takes with catchment info

server5 = 'SQL2012PROD03'
database5 = 'DataWarehouse'

takes_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterCombined'

# Greater zone with WAPs...and other stuff

server6 = 'SQL2012PROD05'
database6 = 'Wells'

zone_table = 'Wells.dbo.WELL_DETAILS'

### Stream depletion

sd_server = "SQL2012PROD05"
sd_database = "Wells"

sd_table = 'Wells.dbo.Well_StreamDepletion_Locations'

sd2_server = "SQL2012PROD03"
sd2_database = 'DataWarehouse'

sd2_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeGwWellStreamDepletion'

# Permits table

permit_table = 'DataWarehouse.dbo.F_ACC_Permit'

#### other parameters

wap_gis_csv = 'C:/ecan/local/Projects/Waimakariri/GIS/tables/waimak_waps.csv'

export_data = 'C:/ecan/local/Projects/requests/waimak_inventory/waimak_sd_data_v03.csv'

#export_mon_path = 'C:/ecan/shared/base_data/usage/sd_est_recent_mon_vol.csv'
#export_sd_est_path = 'C:/ecan/shared/base_data/usage/sd_est_all_mon_vol.csv'
#export_reg_path = 'C:/ecan/shared/base_data/usage/sd_est_reg.csv'

########################################
### Read in data

## Allo and usage

#wap = rd_sql(server3, database3, wap_table, wap_fields, wap_where, wap_where_val)
wap = rd_sql(server3, database3, wap_table, wap_fields)
wap.columns = wap_names

takes = rd_sql(server5, database5, takes_table, takes_fields)
takes.columns = takes_names

zone = rd_sql(server6, database6, zone_table, zone_fields)
zone.columns = zone_names

permit = rd_sql(server5, database5, permit_table, permit_fields)
permit.columns = permit_names

wap_gis = read_csv(wap_gis_csv)[wap_gis_fields]
wap_gis.columns = wap_gis_names

## Stream depletion

sd = rd_sql(sd_server, sd_database, sd_table, sd_fields)
sd.columns = sd_names

sd_acc = rd_sql(sd2_server, sd2_database, sd2_table, sd2_fields)
sd_acc.columns = sd2_names

#######################################
### Merge tables and prepare data

wap_gis2 = wap_gis.dropna(how='all', subset=['subregion', 'catch', 'gw_zone'])

sd2 = sd[sd.sd_7.notnull()]
sd3 = sd2[sd2.sd_150 != 0]

#wap2 = wap[wap.max_rate_wap != 0]
#wap0 = wap[wap.max_rate_wap == 0]

t1 = permit[permit.status == 'OPEN']['crc']

crc_wap = merge(wap, wap_gis2, on=['wap'])
crc_wap = crc_wap[in1d(crc_wap.crc, t1)]

## Correct 0 max rates

crc_wap['count1'] = crc_wap.groupby(['crc'])['wap'].transform('count')
crc_wap2 = merge(crc_wap, takes, on=['crc'])
crc_wap2['max_rate_up'] = (crc_wap2.max_rate/crc_wap2.count1).round(1)

zero_index = crc_wap2.max_rate_wap == 0

crc_wap2.loc[zero_index, 'max_rate_wap'] = crc_wap2.loc[zero_index, 'max_rate_up']
crc_wap2 = crc_wap2[crc_wap2.max_rate_wap != 0]

##  merge SD data

crc_wap_sd = merge(crc_wap2, sd3, on=['wap'], how='left')
crc_wap_sd2 = merge(crc_wap_sd, sd_acc, on=['crc', 'wap'], how='left')

crc_wap_sd2.loc[crc_wap_sd2.sd_150.isnull(), 'sd_150'] = 0
crc_wap_sd2.loc[crc_wap_sd2.take_type == 'Take Surface Water', 'sd_150'] = 100

#dep_calc = DataFrame(nan, index=crc_wap_sd2.index, columns=['dep_calc'])
#
#d_30 = (crc_wap_sd2.dep_calc == '30') | (crc_wap_sd2.dep_calc == '30 days')
#d_0 = (crc_wap_sd2.dep_calc == '0') | (crc_wap_sd2.dep_calc == None)
#d_7 = (crc_wap_sd2.dep_calc == '7') | (crc_wap_sd2.dep_calc == '7 days')
#d_150 = (crc_wap_sd2.dep_calc == '150') | (crc_wap_sd2.dep_calc == '150 days')
#
#dep_calc[d_30] = 30
#dep_calc[d_0] = nan
#dep_calc[d_7] = 7
#dep_calc[d_150] = 150
#
### Create series of SD %
#dep_val = DataFrame(nan, index=crc_wap_sd2.index, columns=['dep_calc'])
#
#for i in dep_calc.index:
#    if dep_calc.ix[i,0] == 30:
#        val = crc_wap_sd2.ix[i, 'sd_30']
#    elif dep_calc.ix[i,0] == 7:
#        val = crc_wap_sd2.ix[i, 'sd_7']
#    elif dep_calc.ix[i,0] == 150:
#        val = crc_wap_sd2.ix[i, 'sd_150']
#    else:
#        val = nan
#    dep_val.ix[i,0] = val * 0.01

## Calc the sw and gw proportions

crc_wap_sd2['sw_contr'] = (crc_wap_sd2.sd_150 * 0.01 * crc_wap_sd2.max_rate_wap).round(2)
crc_wap_sd2['gw_contr'] = ((1 - crc_wap_sd2.sd_150*0.01) * crc_wap_sd2.max_rate_wap).round(2)

sw_index = crc_wap_sd2.take_type == 'Take Surface Water'

crc_wap_sd2.loc[sw_index, 'sw_contr'] = crc_wap_sd2.loc[sw_index, 'max_rate_wap']
crc_wap_sd2.loc[sw_index, 'gw_contr'] = 0

#t1 = crc_wap_sd2.sw_contr + crc_wap_sd2.gw_contr

crc_wap_sd3 = crc_wap_sd2.drop(['count1', 'max_rate', 'max_rate_up'], axis=1)

## Remove duplicates

crc_wap_sd4 = crc_wap_sd3.drop_duplicates()

## Add in qualitative connection value

qual1 = crc_wap_sd4.sd_150

indexdirect = qual1 >= 90
indexhigh = (qual1 >= 60) & (qual1 < 90)
indexmod = (qual1 >= 40) & (qual1 < 60)
indexlow = (qual1 >= 1) & (qual1 < 40)

crc_wap_sd4['new_connection'] = nan

crc_wap_sd4.loc[indexdirect, 'new_connection'] = 'direct'
crc_wap_sd4.loc[indexhigh, 'new_connection'] = 'high'
crc_wap_sd4.loc[indexmod, 'new_connection'] = 'moderate'
crc_wap_sd4.loc[indexlow, 'new_connection'] = 'low'

#################################
### Summarize data

#waimak1 = crc_wap_sd3[crc_wap_sd3.subregion.notnull()]
#waimak2 = waimak1.groupby(['catch', 'take_type'])[['sw_contr', 'gw_contr']].sum()


################################
### Export data

crc_wap_sd4.to_csv(export_data, index=False)










crc_wap_sd2[crc_wap_sd2.crc == 'CRC156498']









#######################################
### Allocation processing

allo1 = allo_proc(takes, wap, dates, zone, zone_add, takes_names, wap_names, dates_names, zone_names, zone_add_names, irr_names, stock_names, ind_names, pub_names, crc_mod, max_rate_mod, max_vol_mod, use_type_mod, irr_par, irr_par_names, use_cav=use_cav, export_path=allo_export_path)

### Water use and allocation time series processing

use_yr, use_mon = w_use_proc(allo1, w_use, w_use_names, end_date=max_date, export_path=usage_export_path, export_mon_path=usage_mon_export_path)

######################################
### Reliability of supply

lowflow, use_ros = ros_proc(restr, sites, crc, use_mon, allo1, restr_names, sites_names, crc_names, min_days=150, export=True, export_ros_path=restr_export_path, export_use_ros_path=use_ros_export_path, export_ann_use_ros_path=ann_use_ros_export_path)

######################################
### Estimate the water usage for all WAPs from existing usage data

usage_new = est_wap_use(use_ros, allo1, export=True, export_path=usage_mon_est_export_path)

######################################
### Stream depletion for recent years (2012 - current)

sd_use = wap_sd(sd, usage_new, allo1, export_path=sd_export_path)

######################################
### Stream depletion for even older years (1972 - 2012)

sd_est_all_mon_vol, sd_est_mon_vol, sd_reg = sd_et_reg(sd_usage_path, tot_usage_path, allo_path, vcn_data_path, vcn_sites_path, wap_catch_path, export=export, export_mon_path=export_mon_path, export_sd_est_path=export_sd_est_path, export_reg_path=export_reg_path)

##########################################################
### Extract all WAPs with X and Y locations for GIS import

wap1 = allo1.groupby('wap').first().reset_index()
wap2 = wap1[['wap', 'take_type', 'catchment', 'gw_zone', 'sw_zone', 'catchment_num', 'cwms_zone', 'NZTMX', 'NZTMY']]

wap2.to_csv('C:/ecan/GIS_base/tables/WAP_details_v03.csv', index=False)


##########################################################
### Extract all WAPs with X and Y locations for GIS import for 2014-2015 water year

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone', 'NZTMX', 'NZTMY']
out_cols = ['take_type', 'use_type', 'catchment', 'catchment_num', 'cwms_zone', 'sw_zone', 'gw_zone', 'ann_allo_m3', 'usage_m3', 'NZTMX', 'NZTMY']

allo1 = read_csv('C:/ecan/base_data/usage/takes_results_with_cav.csv')[allo_cols]
series = read_csv('C:/ecan/base_data/usage/usage_takes_series_with_cav.csv')

allo_use1 = merge(series, allo1, on=['crc', 'wap'])
allo_use2 = allo_use1[allo_use1.dates == '2015-06-30']

wap_grp = allo_use2.groupby('wap')
wap_allo = wap_grp['ann_allo_m3'].sum()
wap_oth = wap_grp.first().drop('ann_allo_m3', axis=1)

wap_all = concat([wap_allo, wap_oth], axis=1)[out_cols]

wap_all.to_csv('C:/ecan/GIS_base/tables/WAP_details_2015_v01.csv')

#####################################
### Testing section

cols = ['crc', 'wap', 'max_rate_wap', 'take_type', 'cav', 'max_rate', 'max_vol', 'return_period', 'ind_ann_allo']

allo1.loc[allo1.crc == 'CRC950649.1', cols]
allo1.loc[allo1.crc == 'CRC132861', cols]

takes[takes.crc == 'CRC132861']
wap[wap.crc == 'CRC132861']

lowflow[lowflow.crc == 'CRC132861']

lowflow[lowflow.crc == 'CRC132861'].groupby(['dates']).sum()

allo1.loc[allo1.wap == 'J37/0251', cols]
wap[wap.wap == 'J37/0251']

takes[takes.crc == 'CRC144500']


sum(data3.ind_ann_allo == 0)
sum(data5.ann_allo_m3 == 0)
sum(data5.usage_m3.isnull())
sum(data4.usage.isnull())
data5[data5.usage_m3.isnull()]

printf(data4[data4.crc == 'CRC000744.1'])

use_ros[use_ros.up_allo_m3.isnull()]

usage.columns = ['wap', 'dates', 'mon_allo', 'mon_allo_wqn', 'crc', 'usage']

lowflow[lowflow.gauge_num == '164601']
lowflow[lowflow.crc == 'CRC141818']

usage[usage.crc == 'CRC000061.1']

res1[res1 == 0]

allo_use2[allo_use2.usage.isnull()]

x = rd_hydstra_csv('C:/ecan/Projects/otop/temp/OTOPDAILYRAINFALL.CSV')

w_resample(x, period='month')

use_ros[use_ros.band_restr.notnull()]

col2 = ['crc', 'wap', 'dates', 'band_restr', 'up_allo_m3', 'ann_up_allo']

df = allo_use2.loc[allo_use2.crc == 'CRC141818', col2]

sum(allo_use2.ann_up_allo.isnull())

col3 = ['crc', 'wap', 'dates', 'ann_up_allo', 'usage_ratio']
allo_use2.loc[allo_use2.usage_ratio > 2, col3]

use_mon[use_mon.crc == 'CRC101875']

use_yr[use_yr.crc == 'CRC101875']

use_ros = read_csv('C:/ecan/base_data/usage/usage_takes_mon_series_sw_up_with_cav.csv')
allo1 = read_csv('C:/ecan/base_data/usage/takes_results_with_cav.csv')

est_usage = usage_new
allo = allo1

use_mon = read_csv('C:/ecan/base_data/usage/usage_takes_mon_series_with_cav.csv')
allo1 = read_csv('C:/ecan/base_data/usage/takes_results_with_cav.csv')

takes[takes.crc == 'CRC970065']
allo1[allo1.crc == 'CRC970065']

usage_path = 'C:/ecan/base_data/usage/usage_takes_mon_series_with_cav.csv'
allo_path = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'

vcn_sites_sel.catch_num.unique()
index1 = vcn_sites_sel[vcn_sites_sel.catch_num == 629]['Data VCN_s'].values

vcn_data.loc[:, index1].mean(axis=1)

wap_catch[wap_catch.catch_num == 646]

usage3['sd_vol.day']/usage3['allo_vol.day']

wap1 = 'J41/0262'

usage2.loc[usage2.wap == wap1, ['crc', 'wap', 'dates', 'use_type', 'sd_usage_est']]

tot_use4.loc[tot_use4.wap == wap1, :]

