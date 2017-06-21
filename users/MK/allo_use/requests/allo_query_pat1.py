# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, read_hdf, TimeGrouper
from allo_use_fun import allo_query
from allo_use_plot_fun import allo_plt, allo_multi_plot
from misc_fun import printf

#################################
### Parameters

## import parameters

## query parameters
grp_by = ['dates', 'use_type']
cwms_zone = 'all'
swaz = 'all'
allo_col = ['mon_vol']
crc2 = 'all'
shp = r'C:\ecan\local\Projects\requests\patrick\2017-02-09\new model boundary.shp'
use_daily_h5 = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
crc_waps_csv = 'S:/Surface Water/shared/base_data/usage/crc_waps.csv'

#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'

#take_type = ['Take Surface Water']
years = 'all'

## output parameters
base_path = 'C:/ecan/local/Projects/requests'
name = 'patrick'
date = '2017-02-09'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)

#################################
### Read in allocation and usage data and merge data

crc_wap = read_csv(crc_waps_csv)
usage = read_hdf(use_daily_h5)

### Read in input data to be used in the query

#crcs = read_csv(waps_csv).RecordNo.unique().tolist()

#################################
### Query data

lw = allo_query(shp=shp, grp_by=grp_by, swaz=swaz, crc=crc2, cwms_zone=cwms_zone, allo_col=allo_col, years=years, export_path=export_path, export=True, debug=False)

lw2 = allo_query(shp=shp, grp_by=grp_by, swaz=swaz, crc=crc2, cwms_zone=cwms_zone, allo_col=allo_col, years=years, export_path=export_path, export=True, debug=True)

crc_set = lw2.crc.unique()
waps = crc_wap[crc_wap.crc.isin(crc_set)]

usage1 = usage[usage.wap.isin(waps.wap.values)]
usage1.index = usage1.date
usage2 = usage1[['wap', 'usage']].groupby(['wap', TimeGrouper('D')]).sum()
usage2.to_csv(path.join(base_path, name, date, name + '_use_ts_wap.csv'))

usage3 = usage1.usage.resample('d').sum()
usage3.to_csv(path.join(base_path, name, date, name + '_use_ts_agg.csv'))


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













