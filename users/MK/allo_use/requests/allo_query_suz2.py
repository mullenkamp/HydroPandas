# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame
from allo_use_fun import allo_query
from allo_use_plot_fun import allo_plt, allo_multi_plot
from misc_fun import printf

#################################
### Parameters

## import parameters

## query parameters
grp_by = ['dates']
cwms_zone = 'all'
swaz = 'all'
allo_col = ['mon_vol']
crc = crc1.values.tolist()
wap = 'all'
#waps_csv = 'C:/ecan/local/Projects/requests/helen/set1/set1.csv'
debug = True
take_type = 'all'
years = 'all'

## output parameters
base_path = 'C:/ecan/local/Projects/requests'
name = 'jenn'
date = '2017-02-03'

export_fig_path = path.join(base_path, name, date, 'figures')
export_path = path.join(base_path, name, date, name + '_allo_use.csv')

allo_name = name + '_past_allo.png'
use_name = name + '_allo_use.png'

if not path.exists(export_fig_path):
    makedirs(export_fig_path)

#################################
### Read in allocation and usage data and merge data


### Read in input data to be used in the query

#crcs = read_csv(waps_csv).RecordNo.unique().tolist()

#################################
### Query data

lw = allo_query(grp_by=grp_by, swaz=swaz, cwms_zone=cwms_zone, crc=crc, allo_col=allo_col, years=years, export_path=export_path, export=True, debug=debug)

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

lw1 = lw[(lw.to_date > '2017-01-01') & (lw.dates == '2016-06-30')]
lw1.to_csv(export_path, index=False)









