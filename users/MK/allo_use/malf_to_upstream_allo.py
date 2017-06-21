# -*- coding: utf-8 -*-
"""
Created on Fri Jan 06 14:46:52 2017

@author: michaelek
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat
from import_fun import rd_ts, rd_sql, rd_site_geo
from spatial_fun import catch_net, pts_poly_join, flow_sites_to_shp, agg_catch
from geopandas import read_file
from ts_stats_fun import malf7d, flow_stats, flow_reg
from numpy import nan, log, argmax, append, in1d


############################################
#### Parameters

flow_csv = 'C:\\ecan\\shared\\base_data\\flow\\otop\\otop_flow_rec_data.csv'
gauge_csv = 'C:\\ecan\\shared\\base_data\\flow\\otop\\otop_min_flow_gauge_data.csv'
gauge_shp = 'C:/ecan/local/Projects/otop/GIS/vector/otop_min_flow_gauge_loc.shp'

current_year = '2016'
min_years = 10

allo_loc_shp = r'C:\ecan\shared\GIS_base\vector\allocations\allo_loc.shp'
catch_del_shp = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\catch_sites_join.shp'
catch_sites_csv = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\results\catch_sites.csv'
crc_col = 'crc'

allo_csv = 'C:/ecan/shared/base_data/usage/allo_results.csv'
post_year = '2017'

allo_loc_shp = r'S:\Surface Water\shared\GIS_base\vector\allocations\allo_loc.shp'

## Export
export_stats = 'C:/ecan/shared/projects/otop/min_flow/min_flow_gauge_reg.csv'
export_sites_shp = 'C:/ecan/local/Projects/otop/GIS/vector/malf_check/flow_sites_v02.shp'
export_results = 'C:/ecan/shared/projects/otop/min_flow/malf_to_up_allo_v03.csv'


##################################
#### Read in data

r_flow = rd_ts(flow_csv)
r_flow.columns = r_flow.columns.astype('int32')

gauge = read_csv(gauge_csv)
gauge['site'] = gauge['site'].astype('int32')
gauge['date'] = to_datetime(gauge['date'])

gauge_info = read_file(gauge_shp)
gauge_info1 = gauge_info[['river', 'site_name']]
gauge_info1.index = gauge_info.site

allo_loc = read_file(allo_loc_shp)

allo = read_csv(allo_csv)

##################################
#### Filter and reorganize data

gauge1 = gauge[gauge.flow > 0]
r_flow = r_flow[r_flow[current_year:].dropna(axis=1, how='all').columns]
r_malf, r1, r2 = malf7d(r_flow, malf_min=0.8)
r_flow2 = r_flow.loc[:, r_malf.index[r_malf['MALF7D 10 yrs'].notnull()]]

g_flow = gauge1.pivot('date', 'site', 'flow')
n_gauge = gauge1.groupby('site')['date'].count()

#################################
#### Run simple linear regressions

reg2, g_flow_ts = flow_reg(r_flow2, g_flow, min_obs=9, make_ts=True)

flow = concat([r_flow2, g_flow_ts], axis=1)

##################################
#### Naturalize flows

flow1, g_flow3, nat_flow1, nat_g_flow = stream_nat(catch_del_shp, catch_sites_csv, export=False, flow_csv=flow)

################################
#### Run stats

rec_malf, r1, r2 = malf7d(flow1, malf_min=0.8)
rec_stats = flow_stats(flow1)

nat_stats = flow_stats(nat_flow1)
nat_malf, g1, g2 = malf7d(nat_flow1, malf_min=0.8)

#sites_all = append(r_flow2.columns, g_flow_ts.columns)
sites_all = flow1.columns.values

#################################
#### Create shapefile for watershed delineation

sites_geo = flow_sites_to_shp(sites_all, export=True, export_path=export_sites_shp)


###############################
#### Once the watershed delineation has been completed...
#### Determine the total catchments by site

catch_del = agg_catch(catch_del_shp, catch_sites_csv)

##############################
#### Process the allo data and combine with crc_catch

to_date = to_datetime(allo_loc.to_date)

allo1 = allo_loc[to_date > post_year]
allo2 = allo1[(allo1.sd1_150.notnull()) | (allo1.take_type == 'Take Surface Water')]
allo2['daily_rate'] = (allo2.daily_vol / 24/60/60*1000).round(2)
gw_index = (allo1.take_type == 'Take Groundwater')
allo2.loc[gw_index, 'daily_rate'] = (allo2.loc[gw_index, 'daily_rate'] * allo2.loc[gw_index, 'sd1_150'] * 0.01).round(2)
allo2['inst_rate'] = allo2.max_rate
allo2.loc[gw_index, 'inst_rate'] = (allo2.loc[gw_index, 'inst_rate'] * allo2.loc[gw_index, 'sd1_150'] * 0.01).round(2)
allo2.loc[allo2.inst_rate == 0, 'inst_rate'] = allo2.loc[allo2.inst_rate == 0, 'daily_rate']

crc_catch2, catch4 = pts_poly_join(allo2, catch_del, 'site')

##############################
#### Estimate the daily rate per site and compare to MALFs

site_rate = (crc_catch2.groupby('site')[['inst_rate', 'daily_rate']].sum() * 0.001).round(3)

miss_index = rec_malf['MALF7D 10 yrs'].isnull().values

rec_malf.loc[miss_index, 'MALF7D 10 yrs'] = rec_malf.loc[miss_index, 'MALF7D all yrs']
nat_malf.loc[miss_index, 'MALF7D 10 yrs'] = nat_malf.loc[miss_index, 'MALF7D all yrs']

malfs = concat([rec_malf[['MALF7D 10 yrs', 'MALF7D all yrs']], nat_malf[['MALF7D 10 yrs', 'MALF7D all yrs']]], axis=1).sort_index()

comp1 = concat([site_rate, malfs], axis=1).reset_index()
comp1.columns = ['site', 'Upstream allocation rate (m3/s)', 'Upstream allocation rate (m3/s)', 'MALF7D 10 yrs', 'MALF7D all yrs', 'MALF7D 10 yrs', 'MALF7D all yrs']


#############################
#### Reorganize for export

site_info1 = rd_site_geo()
site_info2 = site_info1[in1d(site_info1.site, comp1['site'])][['site', 'river', 'site_name']]
site_info2.columns = ['Site number', 'River', 'Site name']

export1 = merge(site_info2, comp1, left_on='Site number', right_on='site').drop('site', axis=1)

export1.to_csv(export_results, index=False)






############################
#### Testing

t1 = comp1['inst_rate']/comp1['MALF7D 10 yrs'].iloc[:,1]
t2 = comp1['daily_rate']/comp1['MALF7D 10 yrs'].iloc[:,1]

concat([comp1['index'], t1, t2], axis=1)
































