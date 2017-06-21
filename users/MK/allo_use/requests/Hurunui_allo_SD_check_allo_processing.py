# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:55:47 2016

@author: MichaelEK
"""
from os import path, makedirs
from numpy import nan, logical_and, where, round, array, in1d, concatenate, isreal, floor, logical_or, sum
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric, read_hdf
from misc_fun import printf
from import_fun import rd_sql, rd_hydstra_csv
from allo_use_fun import allo_proc, allo_ts_proc, allo_errors, allo_loc_proc, allo_use_proc, hist_sd_use, est_use, ros_proc
from ts_stats_fun import w_resample
from spatial_fun import pts_sql_join
from geopandas import read_file

########################################
#### Parameters

min_usage = 100

max_date = '2016-06-30'
sql_join_codes = ['swaz_gis', 'catch_gis', 'cwms_gis']
vcn_grid_shp = r'C:\ecan\shared\GIS_base\vector\NIWA_rain_grid_Canterbury.shp'
vcn_data_path = 'C:/ecan/shared/base_data/precip/VCN_data'

## Export
base_export_path = r'C:\ecan\local\Projects\requests\hurunui_inventory'

allo_export_path = 'allo_results.csv'
allo_loc_export_path = 'allo_loc.shp'
crc_waps_export_path = 'crc_waps.csv'

allo_ts_mon_export = 'allo_ts_mon_results.csv'
allo_ts_ann_export = 'allo_ts_ann_results.csv'
allo_use_export = 'allo_use_ts_mon_results.csv'
use_ros_export_path = 'allo_use_ros_mon.csv'
ann_use_ros_export_path = 'allo_use_ros_ann.csv'
usage_mon_est_export_path = 'allo_est_use_mon.h5'
export_mon_path = 'sd_est_recent_mon_vol.csv'
export_sd_est_path = 'sd_est_all_mon_vol.csv'
export_reg_path = 'sd_est_reg.csv'
export_use_daily = 'usage_daily.h5'
#export_use_mon = 'C:/ecan/shared/base_data/usage/usage_mon.csv'

export_allo_gis = 'allo_gis.shp'

#######################################
#### Load in data when needed

allo = read_csv(allo_export_path)
allo_ts_mon = read_csv(allo_ts_mon_export)
crc_wap = read_csv(crc_waps_export_path)
usage = read_hdf(export_use_daily)
allo_use1 = read_csv(allo_use_export)
allo_xy = read_file(allo_loc_export_path)
vcn_grid = read_file(vcn_grid_shp)[['Data VCN_s', 'Network', 'geometry']]
vcn_grid.columns = ['ecan_id', 'niwa_id', 'geometry']
allo_use_ros = read_csv(use_ros_export_path)
usage_est = read_hdf(usage_mon_est_export_path)

########################################
#### Run functions

### allocation processing
allo = allo_proc(in_allo=False, export=True, export_path=path.join(base_export_path, allo_export_path))

### Determine locations based on a signle WAP per consent...and stuff...
allo_xy, crc_wap = allo_loc_proc(allo, export_shp=path.join(base_export_path, allo_loc_export_path), export_waps=path.join(base_export_path, crc_waps_export_path))

### Create a monthly/annual time series of allocation volumes
allo_ts_ann, allo_ts_mon = allo_ts_proc(allo, export_ann_path=allo_ts_ann_export, export_mon_path=allo_ts_mon_export)

### Usage
usage = w_use_proc(export=True, export_use_daily)
allo_use1 = allo_use_proc(allo_ts_mon, usage, crc_wap, export=True, export_path=allo_use_export)

### Spatial joins of all relevant spatial data to the allo_xy
allo_xy2 = pts_sql_join(allo_xy, sql_join_codes).drop_duplicates(subset=['crc', 'take_type', 'use_type'])
allo_xy2.to_file(path.join(base_export_path, export_allo_gis))
vcn_grid2 = pts_sql_join(vcn_grid, sql_join_codes).dropna(subset=['cwms'])

### ROS
allo_use_ros, allo_use_ros_ann = ros_proc(allo_use1, export=True, export_use_ros_path=use_ros_export_path, export_ann_use_ros_path=ann_use_ros_export_path)

### Estimate usage for missing data from the years 2012 - 2016
usage_est = est_use(allo_use_ros, allo_xy2, allo_ts_mon, export=True, export_path=usage_mon_est_export_path)

### Estimate Stream depletiing usage for earlier years (1972-2012)
sd_est_all_mon_vol, sd_est_mon_vol, sd_reg = hist_sd_use(usage_est, allo_xy2, vcn_grid2, vcn_data_path, export=True, export_mon_path=export_mon_path, export_sd_est_path=export_sd_est_path, export_reg_path=export_reg_path)


###########################################################
#### Find the errors (that can automatically be found)

#allo_errors(takes, wap, dates, zone, zone_add, takes_names, wap_names, dates_names, zone_names, zone_add_names, irr_names, stock_names, ind_names, pub_names, irr_par, irr_par_names, export_path=base_export_path)

#####################################
### Testing section


allo.sort_values('ann_vol', ascending=False)[:30]

allo.sort_values('ann_vol', ascending=False)[30:60]

allo[allo.crc == 'CRC990088.6']
allo[allo.crc == 'CRC950649']

allo[allo.crc == 'CRC021998.1']


takes[takes.crc == 'CRC052033.1']
takes2[takes2.crc == 'CRC052033.1']



allo_ts[allo_ts.crc == 'CRC981180']
CRC981180


CRC905324

allo_ts[allo_ts.crc == 'CRC001135.1']
CRC001135.1



crc_wap_dates[crc_wap_dates.duplicated(subset=['Dates', 'take_type', 'wap'])]
crc_wap_dates[crc_wap_dates.duplicated(subset=['Dates', 'take_type', 'wap'], keep=False)].sort_values(['wap', 'Dates'])
allo_wap1[allo_wap1.duplicated(subset=['Dates', 'take_type', 'wap', 'use_type'], keep=False)].sort_values(['wap', 'Dates'])

crc_wap[crc_wap.duplicated(subset=['take_type', 'wap'], keep=False)].sort_values(['wap'])


allo_wap1[allo_wap1.crc == 'CRC147355']

allo_use1[allo_use1.duplicated(subset=['Dates', 'take_type', 'wap', 'use_type'], keep=False)].sort_values(['wap', 'Dates'])

allo_use2[allo_use2.duplicated(['crc', 'Dates', 'take_type'])]





date1 = '2013-11-30'
crc1 = 'CRC000047.3'
waps1 = ['N34/0142', 'N34/0143', 'N34/0134']


t1 = use1[in1d(use1.wap, waps1)].sort_values('Dates')
tot1 = round(2726.549370 + 9755.096954, 2)

allo_use2[(allo_use2.crc == crc1) & (allo_use2.Dates == date1)]



poly = rd_sql(code='swaz_gis')
poly2 = rd_sql(code='cwms_gis')
join1 = sjoin(allo_xy, poly, how='left', op='within').drop('index_right', axis=1)
join2 = sjoin(join1, poly2, how='left', op='within').drop('index_right', axis=1)


export_sd_hdf = 'C:/ecan/shared/base_data/usage/sd_est_all_mon_vol.h5'
export_sd_nc = 'C:/ecan/shared/base_data/usage/sd_est_all_mon_vol.nc'

from pandas import read_hdf
from xarray import Dataset


t1['swaz'] = t1['swaz'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))

