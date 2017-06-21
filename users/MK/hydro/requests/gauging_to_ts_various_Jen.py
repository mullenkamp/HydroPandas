# -*- coding: utf-8 -*-
"""
Example script to estimate a full time series at gauging sites from regresions to recorder sites.
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from core.ecan_io import rd_hydstra_csv, rd_vcn, rd_ts, rd_henry, rd_hydstra_db, flow_import, rd_sql
from core.ts.sw import malf7d, flow_stats, flow_reg, malf_reg
from geopandas import GeoDataFrame, read_file
from seaborn import regplot
from os import path
import matplotlib.pyplot as plt
from core.ts.plot import reg_plot
from numpy import nan
from core.spatial import sel_sites_poly

############################################
#### Parameters

base_path = r'C:\ecan\local\Projects\requests\jen\2017-02-24'
sites_csv = 'sites.csv'
sites_shp = r'C:\ecan\shared\GIS_base\vector\hydro_sites\all_rec_loc.shp'

flow_csv = 'C:/ecan/shared/base_data/flow/all_flow_rec_data.csv'

#cwms_code = 'cwms_gis'
#cwms_zone = ['Hurunui - Waiau', 'Kaikoura']
poly_shp = 'otop_SWAZ_diss.shp'

min_years = 10

## Export
export_reg = 'flow_gauge_reg.csv'
export_malf = 'flow_gauge_reg_ts_malf.csv'

export_fig1 = str(corr_sites[0]) + '_reg.png'
export_fig2 = str(corr_sites[1]) + '_reg.png'

##################################
#### Read in data

#cwms_loc = rd_sql(code=cwms_code)
#cwms_loc1 = cwms_loc[cwms_loc.cwms.isin(cwms_zone)]

poly = read_file(path.join(base_path, poly_shp))
r_sites_pts = read_file(sites_shp)

sites = read_csv(path.join(base_path, sites_csv))
r_sites = sites[sites.gauge.isnull()].site_number.values


r_flow = rd_ts(flow_csv)
r_flow.columns = r_flow.columns.astype('int32')
site_r_flow = r_flow[r_sites].dropna(how='all')

g_sites = sites[~sites.gauge.isnull()].site_number.values
g_flow = flow_import(gauge_sites=g_sites)
g_flow['site'] = g_flow['site'].astype('int32')
g_flow['date'] = to_datetime(g_flow['date'])


##################################
#### Filter and reorganize data

poly_sites = sel_sites_poly(poly, r_sites_pts).site.values
poly_r_flow = r_flow.loc[:, poly_sites]

g_flow = g_flow[g_flow.flow > 0]
site_r_flow[site_r_flow <= 0] = nan

m1, a, b = malf7d(site_r_flow, intervals=[min_years])
sites_need_corr = m1.loc[m1.iloc[:, 0].isnull()].index

m2, a, b = malf7d(poly_r_flow, intervals=[min_years])
r_flow = poly_r_flow[m2.loc[m2.iloc[:, 0].notnull()].index]

g_flow1 = g_flow.pivot('date', 'site', 'flow')
#n_gauge = g_flow.groupby('site')['date'].count()
#g_flow2 = concat([g_flow1, site_r_flow[sites_need_corr]], axis=1)


#################################
#### Run simple linear regressions

reg1, g_ts1 = flow_reg(r_flow, g_flow1, min_obs=9, make_ts=True, logs=False)
reg_ts, new_r_malf = malf_reg(r_flow, site_r_flow[sites_need_corr])


################################
#### Run stats

r_malf, r1, r2 = malf7d(r_flow)

new_g_stats = flow_stats(g_ts1)
new_g_malf2, g1, g2 = malf7d(g_ts1)

###############################
#### Export data

reg2.to_csv(path.join(base_path, export_reg), index=False)
new_g_malf2.to_csv(path.join(base_path, export_malf))



##############################
#### Plot

#reg_site = corr_sites[0]
#
#set1 = concat([r_flow[corr_sites[0]], site_r_flow], axis=1).dropna()
#ymax = set1.max()[sites].values[0]
#xmax = set1.max()[reg_site]
#
#plt1 = regplot(set1[reg_site], set1[sites])
#plt.ylim(0, ymax)
#plt.xlim(0, xmax)
#
#reg_site = corr_sites[1]
#
#set2 = concat([r_flow[corr_sites[1]], site_r_flow], axis=1).dropna()
#ymax = set2.max()[sites].values[0]
#xmax = set2.max()[reg_site]
#
#plt.figure()
#plt2 = regplot(set2[reg_site], set2[sites])
#plt.ylim(0, ymax)
#plt.xlim(0, xmax)


x1 = r_flow[corr_sites[0]]
x1.name = corr_sites[0]
x2 = r_flow[corr_sites[1]]
x2.name = corr_sites[1]
y = site_r_flow
y.name = sites[0]

plt1 = reg_plot(x1, y, freq='day', export=True, export_path=path.join(base_path, export_fig1))
plt2 = reg_plot(x2, y, freq='day', export=True, export_path=path.join(base_path, export_fig2))

x1 = r_flow[70105]
y1 = site_r_flow[70103]
x2 = r_flow[69602]

plt2 = reg_plot(x1, y1, freq='day', export=False)
plt1 = reg_plot(x2, y1, freq='day', export=False)


x3 = r_flow[[70105, 69602]]
y3 = site_r_flow[70103]

lin_reg(x3, y3)
lin_reg(x3, y3, log_x=True, log_y=True)

plt1 = reg_plot(x_roll[70105], y_roll[70103], freq='day', export=False)



malf1, a, b = malf7d(site_r_flow[69514])







