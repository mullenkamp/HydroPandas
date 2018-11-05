# -*- coding: utf-8 -*-
"""
Created on Mon May 21 08:41:08 2018

@author: MichaelEK
"""
import pandas as pd
from pdsql import mssql
import os
from hydropandas.tools.river.ts.stats import malf7d, flow_stats

pd.options.display.max_columns = 10

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

sites = ['68001', '68002']
dataset = [5, 1521]

export_dir = r'E:\ecan\local\Projects\requests\dan\2018-11-06'
export_stats = 'stats.csv'
export_malf = 'malf7d.csv'
export_alf = 'alf7d.csv'

############################################
### Extract data

## Pull out recorder data

#hyd1 = hyd(ini_path, dll_path)
#
#tsdata1 = hyd1.get_ts_data(sites)
#tsdata2 = hyd1.get_ts_data(sites, varfrom=140)
#
#tsdata = pd.concat([tsdata1, tsdata2]).drop('qual_code', axis=1).unstack(0)
tsdata = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_col={'ExtSiteID': sites, 'DatasetTypeID': dataset})
#tsdata.to_csv(os.path.join(export_dir, export_flow_data))

tsdata1 = tsdata.unstack(0)
tsdata1.columns = tsdata1.columns.droplevel(0)

###############################################
### malf

stats = flow_stats(tsdata1)

malf, alf, mis_alf, f, a = malf7d(tsdata1, intervals=[10, 20, 30], return_alfs=True)

##############################################
### Combine and save

stats.to_csv(os.path.join(export_dir, export_stats))
malf.to_csv(os.path.join(export_dir, export_malf))
alf.to_csv(os.path.join(export_dir, export_alf))

