# -*- coding: utf-8 -*-
"""
Script to determine the optimal regression to use for pre-1998 69607.
"""

from core.ts.sw import malf7d, flow_stats, flow_reg, stream_nat
from core.ecan_io import flow_import, rd_henry, rd_hydstra_db, rd_ts
from pandas import concat
from numpy import nan, log, in1d
import seaborn as sns
from core.ts.plot import hydrograph_plot, reg_plot

#################################
#### Parameters

g_sites = [69607, 69650]
r_sites = [69618]
rec_path = r'C:\ecan\shared\base_data\flow\all_flow_rec_data.csv'
catch_sites_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/catch_sites.csv'
catch_shp = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'

end_corr_date = '1996-12-31'

regplt_export = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\reg1.png'
stats_export = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\69607_predam_stats.csv'
ts_export = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\69607_predam_ts.csv'

regplt_export2 = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\69650_reg1.png'
stats_export2 = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\69650_predam_stats.csv'
ts_export2 = r'C:\ecan\local\Projects\requests\opihi_SH1_predam\69650_predam_ts.csv'

#################################
#### Read in data

g_flow = rd_henry(g_sites, end=end_corr_date, sites_by_col=True)
g_flow.columns = g_flow.columns.astype('int32')

r_flow_all = rd_ts(rec_path)
r_flow_all.columns = r_flow_all.columns.astype('int32')
r_flow = r_flow_all[r_sites][:end_corr_date]


### Alt with naturalisation

flow, gaugings, nat_flow, nat_gauge = stream_nat(catch_shp, catch_sites_csv, flow_csv=r_flow, export=False)

g_flow = nat_gauge[g_sites]
t1 = nat_flow[r_sites].dropna()

x1 = r_flow[~in1d(r_flow.index, t1.index)].dropna()

r_flow = concat([x1, t1], axis=0)

#### Clean up data for regression
g_flow[g_flow <= 0 ] = nan
g_flow[g_flow > 2000 ] = nan
r_flow[r_flow <= 0 ] = nan

#### Regression
reg1, new_ts = flow_reg(r_flow, g_flow, make_ts=True, logs=False)


###############################
#### Plot
data = concat([g_flow[69650], r_flow], axis=1).dropna()
data.columns = data.columns.astype('str')
sns.regplot(x=log(data[r_sites]), y=log(data[g_sites]))
lm = sns.regplot(x='69618', y='69650', data=data, truncate=True)
lm.axes.set_ylim(0, 20)
lm.axes.set_xlim(0, 20)


###############################
#### Export results

predam1 = new_ts.loc[new_ts[69650].first_valid_index():, 69650]
predam2 = predam1.copy()
predam2[predam2 > (reg1.loc[69650].max_y * 1.5)] = nan

stats1 = flow_stats(predam1)
stats2 = flow_stats(predam2)

malf1 = malf7d(predam1, intervals=[10, 20, 30])

stats_both = concat([concat([stats1, malf1], axis=1), concat([stats2, malf1], axis=1)])
stats_both.index = ['all_flows', 'constrained_flows']

stats_both.to_csv(stats_export2, header=True)
predam1.to_csv(ts_export2, header=True)

#plt1 = hydrograph_plot(predam1)
plt2 = reg_plot(r_flow[69618], g_flow[69650], freq='day', export=True, export_path=regplt_export2)

##############################
#### Testing
t1 = nat_flow.dropna()

t2 = (t1 - r_flow).dropna()
t2.plot()

x1 = r_flow[~in1d(r_flow.index, t1.index)].dropna()

r_flow = concat([x1, t1], axis=0)


g1 = rd_henry([69650])




















