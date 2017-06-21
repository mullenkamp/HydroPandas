# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 15:55:29 2016

@author: MichaelEK
"""

from pandas import read_csv
from import_fun import rd_henry, rd_ts, rd_hydstra_csv
from numpy import in1d, where
from misc_fun import printf
from ts_stats_fun import flow_stats
from spatial_fun import catch_net, str_paths, nx_shp

#############################
#### Input parameters

flow_csv = 'C:/ecan/shared/base_data/flow/all_flow_data.csv'
sites_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/sites.csv'
catch_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/catch.csv'
catch_sites_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/catch_sites.csv'
str_seg_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/site_stream_seg.csv'

shp_dir = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/test1'
shp_pts = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/test1/site_vert.shp'
shp_lines = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/test1/MFE_rivers_split.shp'

search_dis = 20000

sites_names = ['x', 'y', 'rec', 'gauge', 'site']
catch_sites_col = ['GRIDCODE', 'SITE']
catch_sites_names = ['catch', 'site']
catch_col = ['ID', 'GRIDCODE', 'AREA_M2']
catch_names = ['id', 'site', 'area_m2']

############################
### import data

flow = rd_ts(flow_csv)

sites = read_csv(sites_csv)
sites.columns = sites_names

catch = read_csv(catch_csv)[catch_col]
catch.columns = catch_names

nx1 = nx_shp(shp_pts, shp_lines)

############################
### Processing

## Stream network
nodes, edges = str_paths(nx1)

## process tables
rec_sites = sites.loc[sites.rec !=0, 'site'].values
gauge_sites = sites.loc[sites.rec ==0, 'site'].values

# Remove tiny polygons from catch


# Gauging tables
gauge_flow = rd_henry(gauge_sites.astype(str).tolist(), sites_by_col=True)

gf1 = gauge_flow.loc[:, gauge_flow.count() >= 10]

gf_mon_count = gf1.resample('M').count()
index1 = gf_mon_count.index.month
gf_mon_count2 = gf_mon_count.groupby(index1).sum()

catch1, singles = catch_net(catch_sites_csv)







