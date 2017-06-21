# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:55:47 2016

@author: MichaelEK
"""

from numpy import nan, logical_and, where, round, array, in1d, concatenate, isreal, floor, logical_or, sum
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from import_fun import rd_sql, rd_hydstra_csv
from allo_use_fun import w_use_proc, allo_proc, est_wap_use, ros_proc, wap_sd, sd_et_reg, allo_errors
from ts_stats_fun import w_resample

########################################
##### Parameters

#### Fields and names for databases

### Allocation and usage

## Query fields - Be sure to use single quotes for the names!!!

w_use_fields = ['UsageSite', 'Year', 'Month', 'Usage']
takes_fields = ['PARENT_B1_ALT_ID', 'Activity', 'Catchment No', 'Consented Annual Volume (m3/year)', 'Irrigation Area', 'GW Allocation Zone', 'SW Allocation Zone', 'Max Rate (l/s)', 'Max Volume (m3)', 'Return Period (days)', 'Water Use (Detail)']
wap_fields = ['PARENT_B1_ALT_ID', 'WellSwapNo', 'Allocation (l/s)', 'B1_PER_CATEGORY']
zone_fields = ['WELL_NO', 'WMCRZone', 'CatchmentNumber', 'NZTMX', 'NZTMY']
zone_add_fields = ['Zone_Code', 'Zone_Name']
irr_par_fields = ['PARENT_B1_ALT_ID', 'B1_PER_CATEGORY', 'Area (ha)', 'SIWL or EISR (mm)', 'Soil PAW (mm)']
dates_fields = ['B1_ALT_ID', 'fmDate', 'toDate']
sd_fields = ['Well_No', 'SD1_30']

## Equivelant short names for analyses - Use these names!!!

w_use_names = ['wap', 'year', 'month', 'usage']
takes_names = ['crc', 'take_type', 'catchment', 'cav', 'irr_area', 'gw_zone', 'sw_zone', 'max_rate', 'max_vol', 'return_period', 'use_type']
wap_names = ['crc', 'wap', 'max_rate_wap', 'take_type']
zone_names = ['wap', 'cwms_zone_num', 'catchment_num', 'NZTMX', 'NZTMY']
zone_add_names = ['cwms_zone_num', 'cwms_zone']
irr_par_names = ['crc', 'take_type', 'irr_area', 'siwl', 'paw']
dates_names = ['crc', 'from_date', 'to_date']
sd_names = ['wap', 'sd_30']

### Low flow restrictions

## Query fields - Be sure to use single quotes for the names!!!

restr_fields = ['SiteID', 'RestrictionDate', 'BandNo', 'BandAllocation']
sites_fields = ['SiteID', 'RefDBaseKey', 'RecordNo', 'WellNo']
crc_fields = ['SiteID', 'BandNo', 'RecordNo']

## Equivelant short names for analyses - Use these names!!!

restr_names = ['SiteID', 'dates', 'band_num', 'band_restr']
sites_names = ['SiteID', 'gauge_num', 'crc', 'wap']
crc_names = ['SiteID', 'band_num', 'crc']

#### Databases

### Allocation and usage

# water use

server1 = "SQL2012PROD03"
database1 = "WUS"

w_use_table = 'WUS.dbo.vw_WUS_Fact_MonthlyUsageByUsageSite_MsAccess'

# GW/SW takes with catchment info

server5 = 'SQL2012PROD03'
database5 = 'DataWarehouse'

takes_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterCombined'
takes_where = 'Include in Allocation?'
takes_where_val = ['Yes']

# crc and WAPs with associated allocated rates

server3 = 'SQL2012PROD03'
database3 = 'DataWarehouse'

wap_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterWellsSwaps'

# crc with start and end dates

server4 = 'SQL2012PROD03'
database4 = 'DataWarehouse'

dates_table = 'DataWarehouse.dbo.F_ACC_Permit'

# Greater zone with WAPs...and other stuff

server6 = 'SQL2012PROD05'
database6 = 'Wells'

zone_table = 'Wells.dbo.WELL_DETAILS'
zone_add_table = 'Wells.dbo.WMCR_Zones'

# FullEstAllocation calculation parameters for irrigation consents

server7 = 'SQL2012PROD03'
database7 = 'DataWarehouse'

irr_par_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterIrrigation'

### Low flow restrictions

# daily restrictions

server1 = 'SQL2012PROD03'
database1 = 'LowFlows'

restr_table = 'LowFlows.dbo.LowFlowSiteRestrictionDaily'
restr_where = 'SnapshotType'
restr_where_val = ['Live']

# Sites info

server2 = 'SQL2012PROD05'
database2 = 'Wells'

sites_table = 'Wells.dbo."vMinimumFlowSites+Consent+Well_classes"'

# crc, sites, and bands

server3 = 'SQL2012PROD03'
database3 = 'LowFlows'

crc_table = 'LowFlows.dbo.vLowFlowConsents2'

### Stream depletion

sd_server = "SQL2012PROD05"
sd_database = "Wells"

sd_table = 'Wells.dbo.Well_StreamDepletion_Locations'

#### other parameters

### Allocation and usage

## changes made to the allocation data due to problems

crc_mod = 'CRC970065'
max_rate_mod = 250
max_vol_mod = 21600
use_type_mod = 'Irrigation'
TDC_old_crc = 'CRC011399'
TDC_old_fmDate = '1969-02-12'

## Other parameters

min_usage = 100

max_date = '2016-06-30'

irr_names = ['Irrigation', 'Intensive Farming', 'Arable Farming']
stock_names = 'Stockwater'
ind_names = ['Industrial Use', 'Cooling Water']
pub_names = ['Public Water Supply (Municipal/Community)']

use_cav = True

base_export_path = 'C:/ecan/shared/base_data/usage/'

allo_export_path = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'

usage_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_series_with_cav.csv'
usage_mon_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_with_cav.csv'

### Low flow restrictions

restr_export_path = 'C:/ecan/shared/base_data/usage/low_flow_restr_with_cav.csv'
use_ros_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_sw_up_with_cav.csv'
ann_use_ros_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_series_sw_up_with_cav.csv'

### All WAP water usage est

usage_mon_est_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_all_est_with_cav.csv'

### Stream depletion for recent years

sd_export_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_all_est_SD_with_cav.csv'

### Stream depletion for older years

sd_usage_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_all_est_SD_with_cav.csv'
tot_usage_path = 'C:/ecan/shared/base_data/usage/usage_takes_mon_series_sw_up_with_cav.csv'
allo_path = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'
flow_path = 'C:/ecan/shared/Projects/otop/flow/otop_flow_recorders.CSV'
vcn_data_path = 'C:/ecan/shared/base_data/precip/VCN_data'
wap_catch_path = 'C:/ecan/shared/GIS_base/tables/WAP_details_v02_catch.csv'
vcn_sites_path = 'C:/ecan/shared/GIS_base/tables/NIWA_rain_grid_catch.csv'

export = True

export_mon_path = 'C:/ecan/shared/base_data/usage/sd_est_recent_mon_vol.csv'
export_sd_est_path = 'C:/ecan/shared/base_data/usage/sd_est_all_mon_vol.csv'
export_reg_path = 'C:/ecan/shared/base_data/usage/sd_est_reg.csv'

########################################
### Read in data

## Allo and usage

w_use = rd_sql(server1, database1, w_use_table, w_use_fields)
#takes = rd_sql(server5, database5, takes_table, takes_fields)
takes = rd_sql(server5, database5, takes_table, takes_fields, takes_where, takes_where_val)
wap = rd_sql(server3, database3, wap_table, wap_fields)
dates = rd_sql(server4, database4, dates_table, dates_fields)
zone = rd_sql(server6, database6, zone_table, zone_fields)
zone_add = rd_sql(server6, database6, zone_add_table, zone_add_fields)
irr_par = rd_sql(server7, database7, irr_par_table, irr_par_fields)

dates.loc[dates.B1_ALT_ID == TDC_old_crc, 'fmDate'] = to_datetime(TDC_old_fmDate)

## low flow restrictions

restr = rd_sql(server1, database1, restr_table, restr_fields, restr_where, restr_where_val).drop_duplicates(keep='last')
sites = rd_sql(server2, database2, sites_table, sites_fields)
crc = rd_sql(server3, database3, crc_table, crc_fields)

## Stream depletion

sd = rd_sql(sd_server, sd_database, sd_table, sd_fields)
sd.columns = sd_names

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

###########################################################
#### Find the errors (that can automatically be found)

allo_errors(takes, wap, dates, zone, zone_add, takes_names, wap_names, dates_names, zone_names, zone_add_names, irr_names, stock_names, ind_names, pub_names, irr_par, irr_par_names, export_path=base_export_path)

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


allo_no_max_path = 'C:/ecan/shared/base_data/usage/takes_no_max_rate.csv'





