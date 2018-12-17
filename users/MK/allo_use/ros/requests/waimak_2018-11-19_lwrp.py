# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 08:42:52 2018

@author: MichaelEK
"""
import os
import pandas as pd
import numpy as np
from low_flows import min_max_trig
from hydropandas.io.tools.general_ts import rd_ts
from pdsql.mssql import rd_sql_ts, rd_sql
from hydropandas.tools.river.ts.stats import flow_stats
from hydropandas.tools.river.ts.naturalisation import stream_nat
from hydrolm import LM
from gistools import vector
import geopandas as gpd

pd.options.display.max_columns = 10

##############################################
### Parameters

base_dir = r'E:\ecan\shared\projects\waimak\data\2018-11-19'

flow_csv1 = 'nat_flows_set1.csv'
flow_csv2 = 'nat_flow_66417.csv'
flow_csv3 = 'nat_flows_set2.csv'
min_flow_trigs_csv = 'min_flow_trigs.csv'
#flow_red_csv = 'flow_reductions.csv'
site_allo_csv = 'site_allo_lwrp.csv'

min_date = '2005-07-01'
end_date = '2015-06-30'

server = 'sql2012test01'
database = 'Hydro'
ts_table = 'TSDataNumericDaily'
ts_summ_table = 'TSDataNumericDailySumm'
hydro_sites_table = 'ExternalSite'

hydro_sites_fields = ['ExtSiteID', 'NZTMX', 'NZTMY']

qual_codes = [200, 400, 500, 520, 600]

#sites = {66204: 53, 253: 54, 66215: 57, 389: 56, 387: 58}

server1 = 'SQL2012PROD03'
database1 = 'LowFlows'
sites_table = 'LowFlowSite'
sites_fields = ['Siteid', 'RefDBaseKey', 'Waterway', 'Location']
sites_names = ['SiteID', 'site', 'waterway', 'location']

## export
set_name = 'all_plans'

export_flow_csv = 'mod_flow_base_' + set_name + '.csv'
export_big_flow_csv = 'mod_flow_combo_' + set_name + '.csv'
export_trigs_csv = 'trigs_' + set_name + '.csv'
export_mon_restr_csv = 'mon_restr_' + set_name + '.csv'
export_ann_restr_csv = 'ann_restr_' + set_name + '.csv'


##############################################
### Read in lowflow db triggers and allocations

#site_allo = pd.read_csv(os.path.join(base_dir, site_allo_csv))
min_trigs = pd.read_csv(os.path.join(base_dir, min_flow_trigs_csv))
#min_trigs = min_trigs[min_trigs.plan == set_name].copy()
min_trigs.allo_block = min_trigs.allo_block.str.strip().str.lower()
min_trigs[['min_trig', 'allo', 'max_trig']] = min_trigs[['min_trig', 'allo', 'max_trig']] * 0.001

sites1 = rd_sql(server1, database1, sites_table, sites_fields, rename_cols=sites_names, where_col={'RefDBaseKey': min_trigs.Site.unique().tolist()})
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

#############################################
### Read in flow data

flow1 = pd.read_csv(os.path.join(base_dir, flow_csv1), index_col=0, parse_dates=True)
flow2 = pd.read_csv(os.path.join(base_dir, flow_csv2), index_col=1, parse_dates=True)['nat_flow']
flow2.name = '66417'
flow1a = pd.read_csv(os.path.join(base_dir, flow_csv3), index_col=0, parse_dates=True)

r_data = rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': [5], 'ExtSiteID': ['66204'], 'QualityCode': qual_codes}, from_date=min_date, to_date=end_date, date_col='DateTime')
r_data.DateTime = pd.to_datetime(r_data.DateTime)
r_data1 = r_data.pivot_table('Value', 'DateTime', 'ExtSiteID')

flow3 = pd.concat([flow1, flow2.to_frame(), flow1a, r_data1], axis=1)

flow3.columns = flow3.columns.astype(int)

flow_all = flow3[flow3.index <= end_date].interpolate().copy()


#######################################
### Calc max triggers for plans
trigs1 = min_trigs.copy()
trigs1['min_allo_perc'] = 0
trigs1['max_allo_perc'] = 100

### Combine all restriction data
trigs = pd.concat([trigs1.drop('allo', axis=1), lfdb_trigs], sort=True).reset_index(drop=True)

## (max_trig-min_trig)/(max_allo - min_allo)
slope = (trigs.max_allo_perc - trigs.min_allo_perc)/(trigs.max_trig - trigs.min_trig)
b = trigs.min_allo_perc - trigs.min_trig * slope

trigs['slope'] = slope
trigs['intercept'] = b

### Replace lowflow ids with site ids
#trigs.replace({'Site': siteids}, inplace=True)

### Flow stats
#stats1 = flow_stats(flow5.unstack(0))
#n_days = flow5.reset_index('Site').groupby(['Site', pd.Grouper(freq='A-JUN')]).count()
#n_days1 = n_days[n_days.flow >= 300]

##############################################
### Runs

## No change in flow
flow_all.index.name = 'Time'
flow_all.columns.name = 'Site'
flow7 = flow_all.stack()
flow7.name = 'flow'

flow8 = flow7.reset_index().copy()
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
flow10['allo_perc'] = restr1.round(2)
flow10.set_index('Time', inplace=True)

################################################
### Summarize results

## Annual
grp1 = flow10.groupby(['plan', 'Site', 'allo_block', pd.Grouper(freq='A-JUN')])

full_restr = grp1.apply(lambda x: (x.allo_perc == 0).sum())
full_restr.name = 'full_restr_days'
partial_restr = grp1.apply(lambda x: ((x.allo_perc > 0) & (x.allo_perc < 100)).sum())
partial_restr.name = 'partial_restr_days'
no_restr = grp1.apply(lambda x: ((x.allo_perc == 100)).sum())
no_restr.name = 'no_restr_days'

restr_count1 = pd.concat([no_restr, partial_restr, full_restr], axis=1)

restr_count1['sum_days'] = restr_count1['no_restr_days'] + restr_count1['partial_restr_days'] + restr_count1['full_restr_days']

mean_restr = grp1['allo_perc'].mean().round(2)
mean_restr.name = 'allo_perc'

ann_restr = pd.concat([restr_count1, mean_restr], axis=1)

## Monthly
grp1 = flow10.groupby(['plan', 'Site', 'allo_block', pd.Grouper(freq='M')])

full_restr = grp1.apply(lambda x: (x.allo_perc == 0).sum())
full_restr.name = 'full_restr_days'
partial_restr = grp1.apply(lambda x: ((x.allo_perc > 0) & (x.allo_perc < 100)).sum())
partial_restr.name = 'partial_restr_days'
no_restr = grp1.apply(lambda x: ((x.allo_perc == 100)).sum())
no_restr.name = 'no_restr_days'

restr_count1 = pd.concat([no_restr, partial_restr, full_restr], axis=1)

restr_count1['sum_days'] = restr_count1['no_restr_days'] + restr_count1['partial_restr_days'] + restr_count1['full_restr_days']

mean_restr = grp1['allo_perc'].mean().round(2)
mean_restr.name = 'allo_perc'

mon_restr = pd.concat([restr_count1, mean_restr], axis=1)

#################################################
### Export

flow10.to_csv(os.path.join(base_dir, export_big_flow_csv))
flow7.to_csv(os.path.join(base_dir, export_flow_csv), header=True)
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









