# -*- coding: utf-8 -*-
"""
Created on Mon May 21 08:41:08 2018

@author: MichaelEK
"""
import pandas as pd
from pdsql import mssql
import os

############################################
### Parameters

server = 'sql2012prod03'
database = 'Hydstra'


server = 'sql2012dev01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

nat_csv = r'E:\ecan\shared\projects\otop\naturalisation\nat_flows_opihi.csv'

sites = ['69607', '69650', '69635', '69618', '69616', '69615', '69602']
dataset = [5]

export_dir = r'E:\ecan\local\Projects\requests\dan\2018-05-21'
export_summ1 = 'opihi_mon_stats.csv'
export_summ2 = 'opihi_mon_stats_nat.csv'
export_flow_data = 'rec_flows_opihi.csv'

############################################
### Extract data

## Pull out recorder data

nat_data = pd.read_csv(nat_csv)
nat_data.date = pd.to_datetime(nat_data.date, dayfirst=True)
nat_data.set_index('date', inplace=True)
nat_data = nat_data[sites].stack(0)
tsdata1 = nat_data.reset_index()
tsdata1.columns = ['DateTime', 'ExtSiteID', 'Value']


tsdata = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_col={'ExtSiteID': sites, 'DatasetTypeID': dataset})
tsdata.to_csv(os.path.join(export_dir, export_flow_data))
#
#tsdata1 = tsdata.reset_index()

###############################################
### 7 day mean monthly low flow

grp0 = tsdata1.set_index('DateTime').groupby(['ExtSiteID'])['Value']

mean7 = grp0.rolling(7, center=True).mean().reset_index()

mean7['month'] = mean7.DateTime.dt.month
mean7['year'] = mean7.DateTime.dt.year

grp1 = mean7.groupby(['ExtSiteID', 'year', 'month'])['Value']

mlf = grp1.min()

mmlf = mlf.groupby(level=['ExtSiteID', 'month']).mean()
mmlf.name = 'mmlf7d'

#############################################
### Summarize

tsdata2 = tsdata1.copy()

tsdata2['month'] = tsdata2.DateTime.dt.month
tsdata2['year'] = tsdata2.DateTime.dt.year

grp1 = tsdata2.groupby(['ExtSiteID', 'year', 'month'])['Value']

stats1 = grp1.describe().drop('count', axis=1)

stats2 = stats1.groupby(level=['ExtSiteID', 'month']).mean()

##############################################
### Combine and save

res1 = pd.concat([mmlf, stats2], axis=1).round(3)

res1.to_csv(os.path.join(export_dir, export_summ2))



