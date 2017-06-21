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
from MSSQL_import_fun import rd_sql

#################################
### Parameters

## Database info

# water use

w_use_fields = ['UsageSite', 'Day', 'Usage']

server1 = "SQL2012PROD03"
database1 = "WUS"

w_use_table = 'WUS.dbo.vw_WUS_Fact_DailyUsageByUsageSite'
use_where = 'UsageSite'
use_where_val = ['K37/0122', 'K37/0668', 'K37/0656', 'K37/0871', 'K37/0773', 'J37/0057', 'K37/0684', 'K37/1300', 'K37/1247', 'K37/1248', 'K37/1332', 'K37/1547', 'K37/1558', 'K37/1623', 'J37/0207', 'J37/0280', 'BY19/0024']


## import parameters
series_csv = 'C:/ecan/shared/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'
zones_csv = 'C:/ecan/shared/base_data/usage/WAPs_catch_zones_cwms.csv'

allo_cols = ['crc', 'wap', 'take_type', 'irr_area', 'gw_zone', 'use_type']
zone_cols= ['wap', 'NZTMX', 'NZTMY', 'NIWA_catch', 'catchment', 'g_catch_num', 'g_catch', 'catchment_num', 'swaz_grp', 'swaz', 'swaz_num', 'cwms_zone']

## query parameters
#cwms_zone = ['Lower Waitaki - South Coastal Canterbury']
allo_col = ['ann_allo_m3']

waps = ['K37/0122', 'K37/0668', 'K37/0656', 'K37/0871', 'K37/0773', 'J37/0057', 'K37/0684', 'K37/1300', 'K37/1247', 'K37/1248', 'K37/1332', 'K37/1547', 'K37/1558', 'K37/1623', 'J37/0207', 'J37/0280', 'BY19/0024']

#take_type = ['Take Surface Water']

## output parameters
export_fig_path = 'C:/ecan/local/Projects/requests/louisa/figures'
export_path = 'C:/ecan/local/Projects/requests/louisa/louisa_use.csv'

allo_name = 'louisa_past_allo.png'
use_name = 'louisa_allo_use.png'

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

usage = rd_sql(server1, database1, w_use_table, w_use_fields, use_where, use_where_val)

### Read in input data to be used in the query

#################################
### Query data

#lw = w_query(allo_use2, grp_by=['dates', 'wap'], allo_col=allo_col, wap=waps, export_path=export_path)

#index1 = otop1.index.levels[1][~in1d(otop1.index.levels[1], out1)].values
#otop2 = otop1.loc[(slice(None), index1, slice(None)), :]

usage.to_csv(export_path, index=False)

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










