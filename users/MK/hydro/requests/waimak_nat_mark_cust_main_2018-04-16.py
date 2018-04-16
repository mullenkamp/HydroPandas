# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 15:17:37 2016

@author: MichaelEK
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from pdsql.mssql import rd_sql, rd_sql_ts
from hydropandas.tools.river.ts.naturalisation import stream_nat
from hydropandas.tools.river.ts.stats import malf7d, flow_stats, flow_reg
from geopandas import read_file
from core.classes.hydro import hydro
from os.path import join
from numpy import exp

############################################
#### Parameters

base_dir = r'E:\ecan\shared\projects\waimak\data\2018-04-16'

sites = [66417]
dataset_types = [5]

server = 'sql2012dev01'
database = 'Hydro'
table = 'HydstraTSDataDaily'

site_col = 'Site'
date_col = 'Time'
values_col = 'Value'




### Output

nat_flow_csv = 'nat_flow.csv'
allo_nat_flow_csv = 'allo_nat_flow.csv'


#### Regressions

#lin_reg_csv = 'lin_reg.csv'
#log_reg_csv = 'log_reg.csv'
#lin_reg_flow_csv = 'lin_reg_flow.csv'
#log_reg_flow_csv = 'log_reg_flow.csv'
#
#combo_reg_csv = 'combo_reg.csv'
#combo_reg_flow_csv = 'combo_reg_flow.csv'
#
#base_data_csv = 'recorded_flows.csv'
#base_reg_data_csv = 'recorded_est_flows.csv'
#nat_data_csv = 'nat_flows.csv'

#alf_min_base_csv = 'recorded_alf_min.csv'
#alf_min_nat_csv = 'nat_alf_min.csv'

#### stats

#stats_base_csv  = 'stats_base.csv'
#malf_base_csv = 'malf_base.csv'
stats_nat_csv  = 'stats_nat.csv'
malf_nat_csv = 'malf_nat.csv'


###########################################
#### Load in data

flow1 = rd_sql_ts(server, database, table, site_col, date_col, values_col, where_col={site_col: sites, 'FeatureMtypeSourceID': dataset_types})

flow2 = flow1.Value.copy()
flow2.name = 'flow'
flow2.index.names = ['site', 'time']
flow2.index = flow2.index.set_levels(flow2.index.levels[0].astype(int), level=0)

###########################################
#### Run stream naturalization

nat_flow, sd1 = stream_nat(sites, flow_csv=flow2, return_data=True)
nat_flow.to_csv(join(base_dir, nat_flow_csv))
sd1.to_csv(join(base_dir, allo_nat_flow_csv), index=False)

nat_flow1 = nat_flow.nat_flow.unstack(0).copy()


###########################################
#### MALFs and stats

#stats_base = flow_stats(nat_flow1)
#malf_base, alf_base, alf_mis_base, alf_days_base, alf_min_base = malf7d(nat_flow1, intervals=[10], return_alfs=True)
stats_nat = flow_stats(nat_flow1)
malf_nat, alf_nat, alf_mis_nat, alf_days_nat, alf_min_nat = malf7d(nat_flow1, intervals=[10], return_alfs=True)

#stats_base.to_csv(join(reg_base, stats_base_csv))
#malf_base.to_csv(join(reg_base, malf_base_csv))
stats_nat.to_csv(join(base_dir, stats_nat_csv))
malf_nat.to_csv(join(base_dir, malf_nat_csv))

#alf_min_base.to_csv(join(reg_base, alf_min_base_csv))
#alf_min_nat.to_csv(join(base_dir, alf_min_nat_csv))


##########################################
#### Plot

new3, reg3 = h2.flow_reg(y_sites, x_sites, below_median=False, logs=True, min_obs=12)

stats1 = h2.stats('flow')
reg2.sort_values('Y_loc')
reg3.sort_values('Y_loc')

x_site = 66401
y_site = 66432
x_max = 1
y_max = 0.2

h2.plot_reg(x_mtype='flow', x_site=x_site, y_mtype='flow_m', y_site=y_site, logs=False)
h2.plot_reg(x_mtype='flow', x_site=x_site, y_mtype='flow_m', y_site=y_site, logs=True)

#h2.plot_reg(x_mtype='flow', x_site=x_site, y_mtype='flow_m', y_site=y_site, x_max=x_max, y_max=y_max)


new0, reg0 = h2.flow_reg([y_site], [x_site], below_median=False, logs=True)
reg2[reg2.Y_loc == y_site]
reg0



#########################################
#### Testing














