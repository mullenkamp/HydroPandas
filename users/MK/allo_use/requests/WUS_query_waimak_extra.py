# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, date_range
from allo_use_fun import w_query
from allo_use_plot_fun import allo_plt, allo_multi_plot
from misc_fun import printf
from import_fun import rd_sql

#################################
### Parameters

## import parameters
series_csv = 'C:/ecan/shared/base_data/usage/sd_est_all_mon_vol.csv'
allo_csv = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'
#zones_csv = 'C:/ecan/shared/base_data/usage/WAPs_catch_zones_cwms.csv'

allo_cols = ['crc', 'wap', 'take_type', 'use_type', 'max_rate_wap', 'cwms_zone']
#zone_cols= ['wap', 'NZTMX', 'NZTMY', 'cwms_zone']
#zone_fields= ['wap', 'NZTMX', 'NZTMY', 'NIWA_catch', 'catchment', 'g_catch_num', 'g_catch', 'catchment_num', 'swaz_grp', 'swaz', 'swaz_num', 'cwms_zone']

### Stream depletion

sd_server = "SQL2012PROD05"
sd_database = "Wells"

sd_table = 'Wells.dbo.Well_StreamDepletion_Locations'

## Column names

sd_fields = ['Well_no', 'NZTMXwell', 'NZTMYwell', 'NZTMX', 'NZTMY', 'SD1_30', 'SD1_150']

sd_names = ['wap', 'x', 'y', 'stream_x', 'stream_y', 'sd_30', 'sd_150']


## query parameters
cwms_zone = ['Waimakariri']

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'

#take_type = ['Take Surface Water']
years = date_range(start='2005', end='2016', freq='A').astype(str).str[0:4].astype(int).tolist()

## output parameters
base_path = 'C:/ecan/local/Projects/requests'
name = 'waimak_waps_sd'
date = '2016-10-10'

export_fig_path = path.join(base_path, name, date,'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'


if not path.exists(export_fig_path):
    makedirs(export_fig_path)

#################################
### Read in allocation and usage data and merge data

sd = rd_sql(sd_server, sd_database, sd_table, sd_fields)
sd.columns = sd_names

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]
#sw_zones = read_csv(zones_csv).drop(['FID','take_type'], axis=1)
#sw_zones.columns = zone_fields
#sw_zones = sw_zones[zone_cols]
#sw_zones['swaz'] = sw_zones.swaz.str.replace('/', '-')
#sw_zones['swaz_grp'] = sw_zones.swaz_grp.str.replace('/', '-')

allo_use1 = merge(series, allo, on=['crc', 'wap'])
allo_use2 = merge(allo_use1, sd, on=['wap'], how='left')

### Read in input data to be used in the query

#crcs = read_csv(waps_csv).RecordNo.unique().tolist()

#################################
### Query data

sel1 = allo_use2[allo_use2.cwms_zone == cwms_zone[0]].drop('cwms_zone', axis=1)

sel1.to_csv(export_path, index=False)

#lw = w_query(allo_use2, grp_by=['dates', 'take_type', 'use_type'], use_col=['usage'], cwms_zone=cwms_zone, years=years, export_path=export_path)

#index1 = otop1.index.levels[1][~in1d(otop1.index.levels[1], out1)].values
#otop2 = otop1.loc[(slice(None), index1, slice(None)), :]

#################################
### Make directories if necessary

#otop1.index.levels[1]
#
#for i in otop1.index.levels[1]:
#    if not path.exists(path.join(export_path, i)):
#        makedirs(path.join(export_path, i))
#    if not path.exists(path.join(export_path, i, swaz_path)):
#        makedirs(path.join(export_path, i, swaz_path))


################################
### plot the summaries

## Singles
allo_plt(lw, start='2004', export_path=export_fig_path, export_name=use_name)
allo_plt(lw, start='1970', cat=['tot_allo'], export_path=export_fig_path, export_name=allo_name)

#allo_multi_plot(otop2, agg_level=1, export_path=export_path, export_name='_' + name4 + ex_name)


#############################
### Check oddities

#otop10 = w_query(allo_use2, grp_by=['dates'], allo_col=allo_col, years=[2015], export=False)
#
#otop11 = otop2[otop2.usage_m3.notnull()]
#
#PARENT_B1_ALT_ID='CRC951874.1'










