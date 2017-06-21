# -*- coding: utf-8 -*-
"""
Created on Mon Oct 03 16:36:30 2016

@author: MichaelEK
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat
from stats_fun import lin_reg
from import_fun import rd_hydstra_csv, rd_vcn, rd_ts, rd_henry, rd_hydstra_db
from spatial_fun import grid_interp_iter, catch_net, pts_poly_join, grid_catch_agg, flow_sites_to_shp, agg_catch
from geopandas import read_file
from ts_stats_fun import malf7d, flow_stats, est_low_flows, est_low_flows_reg, flow_reg
#import matplotlib as plt
#%matplotlib inline
#import seaborn as sns
from numpy import nan, log, argmax, append
#from scipy import stats, optimize
#import statsmodels.api as sm


############################################
#### Parameters

flow_csv = 'C:\\ecan\\shared\\base_data\\flow\\otop\\otop_flow_rec_data.csv'
gauge_csv = 'C:\\ecan\\shared\\base_data\\flow\\otop\\otop_min_flow_gauge_data.csv'
gauge_shp = 'C:/ecan/local/Projects/otop/GIS/vector/otop_min_flow_gauge_loc.shp'

min_years = 10

## Export
export_stats = 'C:/ecan/shared/projects/otop/min_flow/min_flow_gauge_reg.csv'

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

##################################
#### Filter and reorganize data

gauge1 = gauge[gauge.flow > 0]
r_stats = flow_stats(r_flow)
r_flow = r_flow.loc[:, (r_stats['Tot data yrs'] >= min_years).values]

g_flow = gauge1.pivot('date', 'site', 'flow')
n_gauge = gauge1.groupby('site')['date'].count()

#################################
#### Run simple linear regressions

reg2, new_ts = flow_reg(r_flow, g_flow, min_obs=9, make_ts=True)

################################
#### Run stats

r_malf, r1, r2 = malf7d(r_flow)

new_g_stats = flow_stats(new_ts)
new_g_malf, g1, g2 = malf7d(new_ts)























#################################
#### Filter recorder sites

new_index = r_flow['2016'].notnull().sum() > 0
r_flow = r_flow.loc[:, new_index]


#### Make correlations


t1 = lin_reg(r_flow, g_flow, log_x=True, log_y=True)
#t2 = [i[i['n-obs'] > 10] for i in t1]
t2 = [i[(i['n-obs'] > 7)] for i in t1]
t3 = [i for i in t2 if not i.empty]

site_reg = []
for g in g_flow.columns:
    site_reg1 = DataFrame((i[i.Y_loc.values == g].values[0] for i in t3 if sum(i.Y_loc.values == g)), columns=t3[0].columns)
    site_reg.append(site_reg1)


top1 = []
for f in site_reg:
    p_index = f['p-value'] < 0.05
    if sum(p_index) > 0:
        qual = argmax(((f.R2 - f.NRMSE))[p_index])
        best1 = f.iloc[qual, :]
        top1.append(best1)
top = concat(top1, axis=1).T
top.index = top.Y_loc


bad_top1 = []
for f in site_reg:
    if len(f) > 0:
        qual = argmax((f.R2 - f.NRMSE))
        best1 = f.iloc[qual, :]
        bad_top1.append(best1)
bad_top = concat(bad_top1, axis=1).T



### Combine stats and export
g_stats = concat([gauge_info1, flow_stats(gauge.pivot('date', 'site', 'flow')), n_gauge, top], axis=1)
g_stats.to_csv(export_stats)


#### Plotting
g_site = 46
r_site = 69644

data = concat([g_flow[g_site], r_flow[r_site]], axis=1).dropna()
data[data == 0] = nan
sns.regplot(x=log(data[r_site]), y=log(data[g_site]))


#######################################
#### Estimate new flow series

end_date = '2015-06-03'

gauge_est = est_low_flows(gaugings, flow, gauging_sites_dict)

flow_est = est_low_flows(flow, flow, nonltr_sites_dict)

flow_stats(flow_est)
flow_stats(flow[ltr])

malf_est, alf, mis = malf7d(flow_est[:end_date])
malf, alf, mis = malf7d(flow[ltr][:end_date])


######################################
#### Get dataframes of correlations

gauge_est_reg = est_low_flows_reg(gaugings, flow, gauging_sites_dict)

flow_est_reg = est_low_flows_reg(flow, flow, nonltr_sites_dict)


































