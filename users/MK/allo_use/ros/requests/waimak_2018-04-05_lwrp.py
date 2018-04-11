# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 08:42:52 2018

@author: MichaelEK
"""
import os
import pandas as pd
import numpy as np
from core.allo_use.ros import min_max_trig
from hydropandas.io.tools.general_ts import rd_ts
from pdsql.mssql import rd_sql_ts, rd_sql
from hydropandas.tools.river.ts.stats import flow_stats

##############################################
### Parameters

base_dir = r'E:\ecan\shared\projects\waimak\data\2018-04-05'

flow_csv = 'recorded_est_flows.csv'
min_flow_trigs_csv = 'min_flow_trigs.csv'
flow_red_csv = 'flow_reductions.csv'
site_allo_csv = 'site_allo_lwrp.csv'

end_date = '2016-06-30'

server = 'sql2012dev01'
database = 'Hydro'
table = 'HydstraTSDataDaily'

#sites = {66204: 53, 253: 54, 66215: 57, 389: 56, 387: 58}

server1 = 'SQL2012PROD03'
database1 = 'LowFlows'
sites_table = 'LowFlowSite'
sites_fields = ['Siteid', 'RefDBaseKey', 'Waterway', 'Location']
sites_names = ['SiteID', 'site', 'waterway', 'location']

## export
set_name = 'lwrp'

export_flow_csv = 'mod_flow_base_' + set_name + '.csv'
export_big_flow_csv = 'mod_flow_combo_' + set_name + '.csv'
export_trigs_csv = 'trigs_' + set_name + '.csv'
export_mon_restr_csv = 'mon_restr_' + set_name + '.csv'
export_ann_restr_csv = 'ann_restr_' + set_name + '.csv'


##############################################
### Read in lowflow db triggers and allocations

site_allo = pd.read_csv(os.path.join(base_dir, site_allo_csv))

sites1 = rd_sql(server1, database1, sites_table, sites_fields, rename_cols=sites_names, where_col={'RefDBaseKey': site_allo.Site.tolist()})
sites1.site = sites1.site.astype(int)
sites = sites1.set_index('site').drop(['waterway', 'location'], axis=1).SiteID.to_dict()

lfdb_sites = list(sites.values())

l1, lfdb_trigs = min_max_trig(lfdb_sites)
lfdb_trigs.rename(columns={'band_num': 'allo_block'}, inplace=True)
lfdb_trigs['plan'] = 'lfdb'
lfdb_trigs.rename(columns={'SiteID': 'Site'}, inplace=True)
lfdb_trigs.replace({'Site': {sites[i]: i for i in sites}}, inplace=True)

lfdb_trigs['min_trig'] = lfdb_trigs['min_trig'].round(3)
lfdb_trigs['max_trig'] = lfdb_trigs['max_trig'].round(3)

## Convert wierd allocation values
lfdb_trigs.loc[lfdb_trigs.min_allo_perc.isin([104, 100]), 'min_allo_perc'] = 0
lfdb_trigs.loc[lfdb_trigs.max_allo_perc.isin([104, 108]), 'max_allo_perc'] = 100

##############################################
### Load in data and the other parameters

flow1 = rd_ts(os.path.join(base_dir, flow_csv))
flow1.columns.name = 'Site'
flow1.index.name = 'Time'

flow2 = flow1.stack()
flow2.index = flow2.index
flow2.index = flow2.index.reorder_levels(['Site', 'Time'])
flow2.name = 'Value'
flow3 = flow2.sort_index().copy()

flow1.columns = flow1.columns.astype(int)
#flow = flow1[list(sites.keys())].copy()

miss_sites = sites1.loc[~sites1.site.isin(flow1.columns), 'site']

extra1 = rd_sql_ts(server, database, table, 'Site', 'Time', 'Value', where_col={'FeatureMtypeSourceID': [5], 'Site': miss_sites.tolist()}).Value

flow4 = pd.concat([flow3, extra1])
flow4 = flow4[flow4.index.get_level_values('Time') <= end_date]
flow4.index.set_levels(flow4.index.levels[0].astype(int), 'Site', inplace=True)
flow5 = flow4.loc[list(sites.keys())]
flow5.name = 'flow'
flow6 = flow5.reset_index().copy()

min_trigs = pd.read_csv(os.path.join(base_dir, min_flow_trigs_csv))
flow_red = pd.read_csv(os.path.join(base_dir, flow_red_csv))

min_trigs.min_trig = min_trigs.min_trig*0.001

### Create flow reduction time series
#siteids = {sites[i]: i for i in sites}
#flow_red.replace({'Site': siteids}, inplace=True)
flow7 = pd.merge(flow6, flow_red, on='Site')
flow7['pc5'] = flow7['flow'] - flow7['pc5'] * flow7['flow']
flow7['full_abs'] = flow7['flow'] - flow7['full_abs'] * flow7['flow']
flow7.set_index(['Site', 'Time'], inplace=True)
flow7.columns.name = 'flow_type'
flow7a = flow7.stack()
flow7a.name = 'flow'

### Calc max triggers for plans
trigs1 = pd.merge(min_trigs, site_allo, on=['Site', 'allo_block'])
trigs1['max_trig'] = trigs1.min_trig + trigs1.allocation*0.001
trigs1['min_allo_perc'] = 0
trigs1['max_allo_perc'] = 100

### Combine all restriction data
trigs = pd.concat([trigs1.drop('allocation', axis=1), lfdb_trigs]).reset_index(drop=True)

## (max_trig-min_trig)/(max_allo - min_allo)
slope = (trigs.max_allo_perc - trigs.min_allo_perc)/(trigs.max_trig - trigs.min_trig)
b = trigs.min_allo_perc - trigs.min_trig * slope

trigs['slope'] = slope
trigs['intercept'] = b

### Replace lowflow ids with site ids
#trigs.replace({'Site': siteids}, inplace=True)

### Flow stats
stats1 = flow_stats(flow5.unstack(0))
n_days = flow5.reset_index('Site').groupby(['Site', pd.Grouper(freq='A-JUN')]).count()
n_days1 = n_days[n_days.flow >= 300]

##############################################
### Runs

## No change in flow
flow8 = flow7a.reset_index().copy()
flow8['mon'] = flow8.Time.dt.month
flow9 = pd.merge(flow8, trigs, on=['Site', 'mon'])


def calc_restr(df, flow_col):
    if df[flow_col] <= df['min_trig']:
        val = df['min_allo_perc']
    elif df[flow_col] > df['max_trig']:
        val = df['max_allo_perc']
    else:
        val = df[flow_col] * df['slope'] + df['intercept']
    return val

restr1 = flow9.apply(lambda x: calc_restr(x, 'flow'), axis=1)

flow10 = flow9.copy()
flow10['flow_restr'] = restr1.round(2)
flow10.set_index('Time', inplace=True)

################################################
### Summarize results

## Annual
grp1 = flow10.groupby(['flow_type', 'plan', 'Site', 'allo_block', pd.Grouper(freq='A-JUN')])

full_restr = grp1.apply(lambda x: (x.flow_restr == 0).sum())
full_restr.name = 'full_restr_days'
partial_restr = grp1.apply(lambda x: ((x.flow_restr > 0) & (x.flow_restr < 100)).sum())
partial_restr.name = 'partial_restr_days'
no_restr = grp1.apply(lambda x: ((x.flow_restr == 100)).sum())
no_restr.name = 'no_restr_days'

restr_count1 = pd.concat([no_restr, partial_restr, full_restr], axis=1)

restr_count1['sum_days'] = restr_count1['no_restr_days'] + restr_count1['partial_restr_days'] + restr_count1['full_restr_days']

mean_restr = grp1['flow_restr'].mean().round(2)
mean_restr.name = 'restr_perc'

ann_restr = pd.concat([restr_count1, mean_restr], axis=1)

## Monthly
grp1 = flow10.groupby(['flow_type', 'plan', 'Site', 'allo_block', pd.Grouper(freq='M')])

full_restr = grp1.apply(lambda x: (x.flow_restr == 0).sum())
full_restr.name = 'full_restr_days'
partial_restr = grp1.apply(lambda x: ((x.flow_restr > 0) & (x.flow_restr < 100)).sum())
partial_restr.name = 'partial_restr_days'
no_restr = grp1.apply(lambda x: ((x.flow_restr == 100)).sum())
no_restr.name = 'no_restr_days'

restr_count1 = pd.concat([no_restr, partial_restr, full_restr], axis=1)

restr_count1['sum_days'] = restr_count1['no_restr_days'] + restr_count1['partial_restr_days'] + restr_count1['full_restr_days']

mean_restr = grp1['flow_restr'].mean().round(2)
mean_restr.name = 'restr_perc'

mon_restr = pd.concat([restr_count1, mean_restr], axis=1)

#################################################
### Export

flow10.to_csv(os.path.join(base_dir, export_big_flow_csv))
flow7.to_csv(os.path.join(base_dir, export_flow_csv))
trigs.to_csv(os.path.join(base_dir, export_trigs_csv), index=False)
mon_restr.to_csv(os.path.join(base_dir, export_mon_restr_csv))
ann_restr.to_csv(os.path.join(base_dir, export_ann_restr_csv))


#################################################
### testing
#
#flow_type = 'flow'
#plan = 'lfdb'
#site = 253
#allo_block = 1
#year = '1998-06-30'
#
#ann_restr.loc[flow_type, plan, site, allo_block]
#trigs[(trigs.Site == site) & (trigs.plan == plan) & (trigs.allo_block == allo_block)]
#flow7.loc[site]









