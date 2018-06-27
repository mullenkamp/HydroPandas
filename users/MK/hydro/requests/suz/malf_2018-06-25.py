# -*- coding: utf-8 -*-
"""
Created on Mon May 21 08:41:08 2018

@author: MichaelEK
"""
import pandas as pd
from pdsql import mssql
import os
from hydropandas.tools.river.ts.stats import malf7d, flow_stats

############################################
### Parameters

server = 'sql2012dev01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

sites = ['63001', '63101', '63201']
dataset = [5]

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-06-25'
export_stats = 'stats.csv'
export_malf = 'malf7d.csv'
export_alf = 'alf7d.csv'

############################################
### Extract data

## Pull out recorder data

tsdata = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_col={'ExtSiteID': sites, 'DatasetTypeID': dataset})
#tsdata.to_csv(os.path.join(export_dir, export_flow_data))
#
tsdata1 = tsdata.unstack(0)
tsdata1.columns = tsdata1.columns.droplevel(0)

###############################################
### malf

stats = flow_stats(tsdata1)

malf, alf, mis_alf, f, a = malf7d(tsdata1, intervals=[10, 20], return_alfs=True)

##############################################
### Combine and save

stats.to_csv(os.path.join(export_dir, export_stats))
malf.to_csv(os.path.join(export_dir, export_malf))
alf.to_csv(os.path.join(export_dir, export_alf))



