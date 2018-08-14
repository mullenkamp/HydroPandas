# -*- coding: utf-8 -*-
"""
Created on Mon May 21 08:41:08 2018

@author: MichaelEK
"""
import pandas as pd
from pdsql import mssql
import os
from hydropandas.tools.river.ts.stats import malf7d, flow_stats
from pyhydllp import hyd

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

ini_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
dll_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'

sites = ['71162', '71122', '1071105', '71116']
dataset = [5]

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-08-08'
export_stats = 'stats.csv'
export_malf = 'malf7d.csv'
export_alf = 'alf7d.csv'

############################################
### Extract data

## Pull out recorder data

hyd1 = hyd(ini_path, dll_path)

tsdata1 = hyd1.get_ts_data(sites)
tsdata2 = hyd1.get_ts_data(sites, varfrom=140)

tsdata = pd.concat([tsdata1, tsdata2]).drop('qual_code', axis=1).unstack(0)
#tsdata = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_col={'ExtSiteID': sites, 'DatasetTypeID': dataset})
#tsdata.to_csv(os.path.join(export_dir, export_flow_data))
#
#tsdata1 = tsdata.unstack(0)
tsdata.columns = tsdata.columns.droplevel(0)

###############################################
### malf

stats = flow_stats(tsdata)

malf, alf, mis_alf, f, a = malf7d(tsdata, intervals=[10, 20], return_alfs=True)

##############################################
### Combine and save

stats.to_csv(os.path.join(export_dir, export_stats))
malf.to_csv(os.path.join(export_dir, export_malf))
alf.to_csv(os.path.join(export_dir, export_alf))



