# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, read_hdf
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
use_daily_h5 = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
crc_waps_csv = 'S:/Surface Water/shared/base_data/usage/crc_waps.csv'


## query parameters
grp_by = ['dates', 'use_type', 'take_type']
cwms_zone = 'all'
swaz = 'all'
allo_col = ['mon_vol']
crc2 = 'all'
take_type = ['Take Groundwater']
use_type = 'all'
catch_num = 'all'

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'
crc_csv = r'C:\ecan\local\Projects\requests\maureen\2017-03-20\selwyn_crc1.csv'

#take_type = ['Take Surface Water']
years = [2016]

## output parameters
base_path = 'C:/ecan/local/Projects/requests'
name = 'maureen'
date = '2017-03-20'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')
export_use_path = path.join(base_path, name, date, name + '_daily_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)

export_shp = path.join(base_path, name, date, name + '_allo_use.shp')

#################################
### Read in allocation and usage data and merge data


### Read in input data to be used in the query

crcs = read_csv(crc_csv).crc.unique()

#################################
### Query data

lw = allo_query(grp_by=grp_by, swaz=swaz, crc=crcs.tolist(), cwms_zone=cwms_zone, take_type=take_type, use_type=use_type, catch_num=catch_num, allo_col=allo_col, years=years, export_path=export_path, export=False, debug=True)

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


#################################
#### Output daily usage by crc

use1 = read_hdf(use_daily_h5)
crc_wap = read_csv(crc_waps_csv)

crc_wap2 = crc_wap[crc_wap.crc.isin(crcs)]

use2 = use1[use1.wap.isin(crc_wap2.wap.unique())]
use3 = merge(use2, crc_wap2[['wap', 'crc']], on='wap')

use4 = use3.groupby(['crc', 'date'])['usage'].sum()

use5 = use4.unstack(0)

use5.to_csv(export_use_path, header=True)

#############################
### Check oddities

#otop10 = w_query(allo_use2, grp_by=['dates'], allo_col=allo_col, years=[2015], export=False)
#
#otop11 = otop2[otop2.usage_m3.notnull()]
#

to_date = '2017-03-01'

lw1 = lw[lw.to_date >= to_date]

lw_irr = lw1[lw1.use_type == 'irrigation']

lw_irr_sd = lw_irr[lw_irr.sd1_150.notnull()]

cc1 = rd_sql(code='crc_gen_acc')

lw_irr_sd1 = merge(lw_irr, cc1, on=['crc', 'take_type'])

lw_irr_sd2 = lw_irr_sd1[lw_irr_sd1.min_flow == 'Yes']

allo_gis = read_file(allo_gis_file)

selwyn1 = allo_gis[allo_gis.crc.isin(lw_irr.crc)]
selwyn1.to_file(export_shp)

lw_irr.to_csv(export_path, index=False)

wap1 = 'M36/0047'

crcs[~in1d(crcs, lw.crc)]





