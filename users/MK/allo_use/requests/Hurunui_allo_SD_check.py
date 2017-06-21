# -*- coding: utf-8 -*-
"""
Created on Thu May 19 08:55:47 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import nan, logical_and, where, round, array, in1d, concatenate, isreal, floor, logical_or, sum
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from core.misc import printf
from core.allo_use import allo_query
from core.ecan_io import rd_hydstra_csv, rd_sql
from core.ts import w_resample
from geopandas import read_file
from core.spatial import sel_sites_poly, xy_to_gpd, pts_sql_join

#########################################
#### Parameters

## import parameters
use_type_csv='S:/Surface Water/shared/base_data/usage/use_type_def.csv'
crc_waps_csv = r'C:\ecan\local\Projects\requests\hurunui_inventory\crc_waps.csv'

## Database parameters
server03 = 'SQL2012PROD03'
#server05 = 'SQL2012PROD05'
database1 = 'DataWarehouse'
#database2 = 'Wells'

crc_wap_code = 'crc_wap_act_acc'
crc_ann_vol_code = 'crc_gen_acc'
sd_code = 'sd'
permit_code = 'crc_details_acc'
waps_details_code = 'waps_details'
cwms_code = 'cwms_gis'
sql_join_codes = ['swaz_gis', 'catch_gis']

## query parameters
#grp_by = ['dates', 'use_type']
cwms_zone = 'Hurunui - Waiau'
#swaz = 'all'
#allo_col = ['mon_vol']

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'

#take_type = ['Take Surface Water']
#years = [2016]

allo_csv = r'C:\ecan\local\Projects\requests\hurunui_inventory\allo_results.csv'
allo_shp = r'C:\ecan\local\Projects\requests\hurunui_inventory\allo_gis.shp'

## output parameters
base_path = 'E:/ecan/local/Projects/requests'
name = 'hurunui_inventory'
date = '2017-05-04'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)



#########################################
#### Read in data

sd = rd_sql(code=sd_code)
permit = rd_sql(code=permit_code)
#crc_wap = read_csv(crc_waps_csv)
crc_wap = rd_sql(code=crc_wap_code)
crc_ann_vol = rd_sql(code=crc_ann_vol_code)
waps_details = rd_sql(code=waps_details_code)[['wap', 'NZTMX', 'NZTMY']]
cwms_loc = rd_sql(code=cwms_code)
sd_conn = rd_sql('SQL2012PROD03', 'DataWarehouse', 'D_ACC_ActivityAttribute_TakeGwWellStreamDepletion', ['PARENT_B1_ALT_ID', 'B1_PER_CATEGORY', 'WellNo', 'Connection'])
sd_conn.columns = ['crc', 'take_type', 'wap', 'orig_connection']

#######################################
#### Filter and merge data

cwms_loc1 = cwms_loc[cwms_loc.cwms == cwms_zone]
open_crc = permit.loc[permit.status == 'OPEN', ['crc', 'from_date', 'to_date']]
wap_loc = xy_to_gpd(waps_details, 'wap', 'NZTMX', 'NZTMY')

cwms_waps = sel_sites_poly(wap_loc, cwms_loc1)
cwms_waps2 = pts_sql_join(cwms_waps, sql_join_codes).drop('geometry', axis=1)
crc_wap2 = merge(crc_wap, crc_ann_vol, on=['crc', 'take_type'])
crc_wap_cwms = merge(crc_wap2, cwms_waps2, on='wap')
open_crc_wap_cwms = merge(crc_wap_cwms, open_crc, on='crc')
open_crc_wap_cwms = merge(open_crc_wap_cwms, sd_conn, on=['crc', 'take_type', 'wap'], how='left')

#####################################
#### Stream depletion

## Merge data
crc_wap_sd2 = merge(open_crc_wap_cwms, sd[['wap', 'sd1_7', 'sd1_30', 'sd1_150']], on=['wap'], how='left')

crc_wap_sd2.loc[crc_wap_sd2.sd1_150.isnull(), 'sd1_150'] = 0
crc_wap_sd2.loc[crc_wap_sd2.take_type == 'Take Surface Water', 'sd1_150'] = 100

## Run calcs

crc_wap_sd2['sw_contr'] = (crc_wap_sd2.sd1_150 * 0.01 * crc_wap_sd2.max_rate_wap).round(2)
crc_wap_sd2['gw_contr'] = ((1 - crc_wap_sd2.sd1_150*0.01) * crc_wap_sd2.max_rate_wap).round(2)

sw_index = crc_wap_sd2.take_type == 'Take Surface Water'

crc_wap_sd2.loc[sw_index, 'sw_contr'] = crc_wap_sd2.loc[sw_index, 'max_rate_wap']
crc_wap_sd2.loc[sw_index, 'gw_contr'] = 0

## Remove duplicates

crc_wap_sd3 = crc_wap_sd2.drop_duplicates()

## Add in qualitative connection value

qual1 = crc_wap_sd3.sd1_150

indexdirect = qual1 >= 90
indexhigh = (qual1 >= 60) & (qual1 < 90)
indexmod = (qual1 >= 40) & (qual1 < 60)
indexlow = (qual1 >= 1) & (qual1 < 40)

crc_wap_sd3['new_connection'] = nan

crc_wap_sd3.loc[indexdirect, 'new_connection'] = 'direct'
crc_wap_sd3.loc[indexhigh, 'new_connection'] = 'high'
crc_wap_sd3.loc[indexmod, 'new_connection'] = 'moderate'
crc_wap_sd3.loc[indexlow, 'new_connection'] = 'low'

#############################################
#### Rename some columns and export

output1 = crc_wap_sd3.rename(columns={'sd': 'sd_acc'})

output1.to_csv(export_path, index=False)


