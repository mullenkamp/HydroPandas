# -*- coding: utf-8 -*-
"""
Created on Mon Oct 03 16:36:30 2016

@author: MichaelEK
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat
from stats_fun import lin_reg
from import_fun import rd_hydstra_csv, rd_vcn, rd_ts, rd_henry
from spatial_fun import grid_interp_iter, catch_net, pts_poly_join, catch_agg, grid_catch_agg
from geopandas import read_file
from ts_stats_fun import malf7d, flow_stats, est_low_flows
import matplotlib as plt
%matplotlib inline
import seaborn as sns
from numpy import nan, log, in1d
from scipy import stats, optimize
import statsmodels.api as sm
from os import path, makedirs

############################################
#### Parameters

catch_sites_csv = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/results/catch_sites.csv'
catch_shp = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/catch1.shp'
flow_csv = 'C:\\ecan\\shared\\base_data\\flow\\all_flow_data.csv'

norm_area = True

## Other data

buffer_dis = 10000
grid_res = 0.005
epsg = 4326
period = 'water year'

poly_id_col = 'GRIDCODE'

gauging_sites_dict = {279: [66204, True], 343: [66204, True], 361: [66204, True], 370: [66417, True], 387: [66417, True], 66432: [66213, True]}
nonltr_sites_dict = {166405: [66204, True], 66201: [66213, True], 66409: [66204, True], 66472:[66204, True]}

## Export paths

export_base = 'C:\\ecan\\local\\Projects\\Waimakariri\\analysis\\flow'

export_flow = 'flow.csv'
export_flow_est = 'flow_est.csv'
export_gauge = 'gauge.csv'
export_gauge_est = 'gauge_est.csv'

export_flow_nat = 'flow_nat.csv'
export_flow_est_nat = 'flow_est_nat.csv'
export_gauge_nat = 'gauge_nat.csv'
export_gauge_est_nat = 'gauge_est_nat.csv'


####################################
#### Query the VCSN data

#df1, gpd = rd_vcn(catch_shp, buffer_dis=buffer_dis)
#
#df2, gpd2 = grid_interp_iter(df1, gpd, grid_res, period, epsg)
#
#poly = read_file(catch_shp)
#
##agg_precip, catch_areas = grid_catch_agg(df2, poly, gpd2, poly_id_col, catch_sites_csv)
#
#pts_poly, poly2 = pts_poly_join(gpd2, poly, poly_id_col)
#pts_poly2 = pts_poly['id']
#
#df3 = df2[pts_poly2.index]
#site_precip = df3.groupby(pts_poly2.values, axis=1).mean()
#sites, singles = catch_net(catch_sites_csv)
#
#agg_precip, catch_areas = catch_agg(sites, site_precip, poly2, agg_fun='mean')


##################################
####  Naturalize flows

#flow_norm, gauge_norm, nat_flow_norm, nat_gauge_norm = stream_nat(catch_shp, catch_sites_csv, norm_area=norm_area)

flow1, gaugings1, nat_flow, nat_gauge = stream_nat(catch_shp, catch_sites_csv)

##################################
#### Read in data

sites1 = read_file(catch_shp)['GRIDCODE'].unique()

flow = rd_ts(flow_csv)
flow.columns = flow.columns.astype(int)
flow = flow.loc[:, in1d(flow.columns, sites1)]

gaugings = rd_henry(gaugings1.columns.tolist(), sites_by_col=True)
gaugings.columns = gaugings.columns.astype(int)

#################################
#### Make correlations
i = 66417

t1 = concat([flow1[i], nat_flow[i]], axis=1).dropna()
t1.plot()

#malf, alf, t2 = malf7d(flow)

#lin_reg(agg_precip[site], alf[site])


#t1 = concat([agg_precip[site], alf[site]], axis=1)
#t1
#x = nat_flow_norm[70105]
#
#x1 = x.resample('3M').sum()
#x1
#x1.rolling(3, center=True).mean()
#

#
#gauge_count = nat_gauge_norm.resample('A-JUN').count()
#nat_gauge_norm1 = nat_gauge_norm.loc[:, gauge_count.sum() > 10]
#
#
#gaugings[gaugings == 0] = nan
#flow[flow == 0] = nan
##t1 = lin_reg(gauge_norm, flow_norm)
#t1 = lin_reg(gaugings, flow[ltr])
#
#t2 = [i[i['n-obs'] > 10] for i in t1]
#t3 = [i for i in t2 if not i.empty]
#
##t4 = DataFrame(i[i.NRMSE == min(i.NRMSE)].values.flatten() for i in t3)
##t4.columns = t3[0].columns
#
#x1 = t3[4]

stats1 = flow_stats(flow)
ltr = stats1[stats1['Tot data yrs'] >= 20].index
nonltr = stats1[stats1['Tot data yrs'] < 20].index

gaugings[gaugings == 0] = nan
flow[flow == 0] = nan

t1 = lin_reg(flow[ltr], gaugings, log_x=True, log_y=True)
t2 = [i[i['n-obs'] > 10] for i in t1]
#t2 = [i[(i['n-obs'] <= 10) & (i['n-obs'] > 6)] for i in t1]
t3 = [i for i in t2 if not i.empty]
t3

gauge_site = 66432
site_reg = DataFrame((i[i.Y_loc.values == gauge_site].values[0] for i in t3 if sum(i.Y_loc.values == gauge_site)), columns=t3[0].columns)
site_reg

flow_site = 66213
data = concat([gaugings[gauge_site], flow[flow_site]], axis=1).dropna()
data[data == 0] = nan

sns.regplot(x=data[flow_site], y=data[gauge_site])
sns.regplot(x=log(data[flow_site]), y=log(data[gauge_site]))


t1 = lin_reg(flow[ltr], flow[nonltr], log_x=True, log_y=True)
t2 = [i[i['n-obs'] > 10] for i in t1]
t3 = [i for i in t2 if not i.empty]

nonltr_site = 66472
site_reg = DataFrame((i[i.Y_loc.values == nonltr_site].values[0] for i in t3 if sum(i.Y_loc.values == nonltr_site)), columns=t3[0].columns)
site_reg

flow_site = 66213
data = concat([flow[nonltr_site], flow[flow_site]], axis=1).dropna()
data[data == 0] = nan

sns.regplot(x=data[flow_site], y=data[nonltr_site])
sns.regplot(x=log(data[flow_site]), y=log(data[nonltr_site]))


#
#huber_t = sm.RLM(log(data[flow_site]), log(data[gauge_site]),  M=sm.robust.norms.HuberT())
#hub_results = huber_t.fit()
#print(hub_results.params)
#print(hub_results.bse)
#print(hub_results.summary(yname='y', xname=['var_%d' % i for i in range(len(hub_results.params))]))
#
#def reject_outliers(data, m = 2.):
#    from numpy import abs, median
#    d = abs(data - median(data))
#    mdev = median(d)
#    s = d/mdev if mdev else 0.
#    return data[s<m]


#######################################
#### Estimate new flow series

end_date = '2015-06-03'

gauge_est = est_low_flows(gaugings, flow, gauging_sites_dict)
flow_est = est_low_flows(flow, flow, nonltr_sites_dict)

flow_stats(flow_est)
flow_stats(flow[ltr])

malf_est, alf, mis = malf7d(flow_est[:end_date])
malf, alf, mis = malf7d(flow[ltr][:end_date])

nat_gauge_est = est_low_flows(nat_gauge, nat_flow, gauging_sites_dict)
nat_flow_est = est_low_flows(nat_flow, nat_flow, nonltr_sites_dict)


######################################
#### Export data

flow.to_csv(path.join(export_base, export_flow))
flow_est.to_csv(path.join(export_base, export_flow_est))
gaugings.to_csv(path.join(export_base, export_gauge))
gauge_est.to_csv(path.join(export_base, export_gauge_est))

nat_flow.to_csv(path.join(export_base, export_flow_nat))
nat_flow_est.to_csv(path.join(export_base, export_flow_est_nat))
nat_gauge.to_csv(path.join(export_base, export_gauge_nat))
nat_gauge_est.to_csv(path.join(export_base, export_gauge_est_nat))


















