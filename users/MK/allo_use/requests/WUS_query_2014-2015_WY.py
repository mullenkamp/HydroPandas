# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame
from query_use_allo_fun import w_query
from allo_use_plot_fun import allo_plt, allo_multi_plot
from misc_fun import printf

#################################
### Parameters

## import parameters
series_csv = 'C:/ecan/shared/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'
zones_csv = 'C:/ecan/shared/base_data/usage/WAPs_catch_zones_cwms.csv'

allo_cols = ['crc', 'wap', 'take_type', 'irr_area', 'gw_zone', 'use_type']
zone_cols= ['wap', 'NZTMX', 'NZTMY', 'NIWA_catch', 'catchment', 'g_catch_num', 'g_catch', 'catchment_num', 'swaz_grp', 'swaz', 'swaz_num', 'cwms_zone']

## query parameters
#cwms_zone = ['Lower Waitaki - South Coastal Canterbury']
allo_col = ['ann_allo_m3']
year = [2015]

#take_type = ['Take Surface Water']

## output parameters
export_path = 'C:/ecan/local/Projects/requests/2014-2015_water_year'
#export_fig_path = 'figures'
export_csv = '2014-2015_allo_use.csv'

#allo_name = 'lower_waitaki_past_allo.png'
#use_name = 'lower_waitaki_allo_use.png'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]
sw_zones = read_csv(zones_csv).drop(['FID','take_type'], axis=1)
sw_zones.columns = zone_cols
sw_zones['swaz'] = sw_zones.swaz.str.replace('/', '-')
sw_zones['swaz_grp'] = sw_zones.swaz_grp.str.replace('/', '-')

allo_use1 = merge(series, allo, on=['crc', 'wap'])
allo_use2 = merge(allo_use1, sw_zones, on=['wap'])

### Read in input data to be used in the query

#################################
### Query data

q1 = w_query(allo_use2, years=year, allo_col=allo_col, export_path=path.join(export_path, export_csv), debug=True)

q2 = q1.drop(['ann_allo_wqn_m3', 'band_num', 'band_restr', 'ann_up_allo_m3', 'ann_up_allo_wqn_m3', 'gauge_num'], axis=1)

q2.to_csv(path.join(export_path, export_csv), index=False)

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
#allo_plt(lw, start='2004', export_path=export_fig_path, export_name=use_name)
#allo_plt(lw, start='1970', cat=['tot_allo'], export_path=export_fig_path, export_name=allo_name)

#allo_multi_plot(otop2, agg_level=1, export_path=export_path, export_name='_' + name4 + ex_name)


#############################
### Check oddities

#otop10 = w_query(allo_use2, grp_by=['dates'], allo_col=allo_col, years=[2015], export=False)
#
#otop11 = otop2[otop2.usage_m3.notnull()]
#
#PARENT_B1_ALT_ID='CRC951874.1'










