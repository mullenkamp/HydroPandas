# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame
from core.allo_use import allo_query
from core.allo_use.plot import allo_plt, allo_multi_plot
from core.misc import printf
from datetime import date
from core.ecan_io import rd_sql
from geopandas import read_file

#################################
### Parameters

## import parameters
allo_gis_file=r'S:\Surface Water\shared\GIS_base\vector\allocations\allo_gis.shp'

## query parameters
grp_by = ['dates', 'take_type', 'use_type']
cwms_zone = ['Selwyn - Waihora']
swaz = 'all'
allo_col = ['mon_vol']
crc2 = 'all'
take_type = 'all'
use_type = 'all'
catch_num = 'all'

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'

#take_type = ['Take Surface Water']
years = 'all'

## output parameters
base_path = 'C:/ecan/local/Projects/requests'
name = 'selwyn'
date = '2017-03-06'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)

#export_shp = path.join(base_path, name, date, name + '_allo_use.shp')

#################################
### Read in allocation and usage data and merge data


### Read in input data to be used in the query

#crcs = read_csv(waps_csv).RecordNo.unique().tolist()

#################################
### Query data

lw = allo_query(grp_by=grp_by, swaz=swaz, crc=crc2, cwms_zone=cwms_zone, take_type=take_type, use_type=use_type, catch_num=catch_num, allo_col=allo_col, years=years, export_path=export_path, export=True, debug=False)

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

to_date = '2017-03-01'
w_yrs = '2015-06-30'


lw1 = lw[lw.dates == w_yrs]
lw2 = lw1[lw1.to_date >= to_date]
lw2.sort_values('mon_vol', ascending=False)[['crc', 'take_type', 'mon_vol', 'usage']]

lw_irr = lw1[lw1.use_type == 'irrigation']

lw_irr_sd = lw_irr[lw_irr.sd1_150.notnull()]

cc1 = rd_sql(code='crc_gen_acc')

lw_irr_sd1 = merge(lw_irr, cc1, on=['crc', 'take_type'])

lw_irr_sd2 = lw_irr_sd1[lw_irr_sd1.min_flow == 'Yes']

allo_gis = read_file(allo_gis_file)

selwyn1 = allo_gis[allo_gis.crc.isin(lw_irr.crc)]
selwyn1.to_file(export_shp)

lw_irr.to_csv(export_path, index=False)


from core.ecan_io.mssql import rd_sql
from pandas import read_hdf, read_csv

allo_use_file=r'S:\Surface Water\shared\base_data\usage\allo_est_use_mon.h5'

allo1 = rd_sql(code='crc_wap_act_acc')
use1 = read_hdf(allo_use_file)[['crc', 'dates', 'take_type', 'use_type', 'mon_vol', 'usage']]
use_ht = read_csv(r'C:\ecan\hilltop\xml_test\use_daily_all_waps.csv')

crc = 'CRC142170'

waps = allo1[allo1.crc == crc].wap.values

use_ht[use_ht.site.isin(waps)]
use1[use1.crc == crc]
crc_wap[crc_wap.crc == crc]
usage[usage.wap.isin(waps)]
allo_use1[allo_use1.crc == crc]
allo_use_ros_ann[allo_use_ros_ann.crc == crc]












