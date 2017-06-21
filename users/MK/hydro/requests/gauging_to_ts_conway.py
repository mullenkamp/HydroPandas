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
from core.spatial import sel_sites_poly

############################################
#### Parameters

flow_csv = 'C:/ecan/shared/base_data/flow/all_flow_rec_data.csv'
base_path = r'C:\ecan\local\Projects\requests\suz\2017-03-09'
rec_shp = r'S:\Surface Water\shared\GIS_base\vector\hydro_sites\all_rec_loc.shp'

sites = [64304]
corr_sites = [64610]
cwms_code = 'cwms_gis'
cwms_zone = ['Hurunui - Waiau', 'Kaikoura']
#poly_shp = '62107_poly_corr.shp'

min_years = 10

## Export
export_reg = 'flow_gauge_reg.csv'
export_malf = 'flow_gauge_reg_ts_malf.csv'

export_fig1 = str(corr_sites[0]) + '_reg.png'
#export_fig2 = str(corr_sites[1]) + '_reg.png'

##################################
#### Read in data

cwms_loc = rd_sql(code=cwms_code)
cwms_loc1 = cwms_loc[cwms_loc.cwms.isin(cwms_zone)]

rec_pts = read_file(rec_shp)

rec_sites = sel_sites_poly(cwms_loc1, rec_pts)['site']

r_flow = rd_ts(flow_csv)
r_flow.columns = r_flow.columns.astype('int32')
r_flow = r_flow[rec_sites.values]


g_flow = flow_import(rec_sites=sites)
#g_flow['site'] = g_flow['site'].astype('int32')
#g_flow['date'] = to_datetime(g_flow['date'])

site_r_flow = r_flow[sites].dropna()

##################################
#### Filter and reorganize data

g_flow = g_flow[g_flow.flow > 0]
site_r_flow = site_r_flow[site_r_flow > 0]

r_stats = flow_stats(r_flow)
r_flow = r_flow.loc[:, (r_stats['Tot data yrs'] >= min_years).values]

g_flow1 = g_flow.pivot('date', 'site', 'flow')
#n_gauge = g_flow.groupby('site')['date'].count()

#################################
#### Run simple linear regressions

reg1, malf1 = malf_reg(r_flow, g_flow)

malf2, a, b = malf7d(g_flow)
malf3, a, b = malf7d(r_flow[64610])

reg1, new_ts1 = flow_reg(r_flow, g_flow1, min_obs=9, make_ts=True)
reg2, new_ts2 = flow_reg(r_flow[corr_sites[0]], site_r_flow, min_obs=9, make_ts=True)
reg3, new_ts3 = flow_reg(r_flow[corr_sites[1]], site_r_flow, min_obs=9, make_ts=True)

################################
#### Run stats

r_malf, r1, r2 = malf7d(r_flow)

new_g_stats = flow_stats(new_ts2)
new_g_malf2, g1, g2 = malf7d(new_ts2)
new_g_malf3, g1, g2 = malf7d(new_ts3)

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



