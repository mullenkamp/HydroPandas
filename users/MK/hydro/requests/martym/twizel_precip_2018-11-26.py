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

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
ts_summ_table = 'TSDataNumericDaily'

sites = ['402001']
dataset = [15]

from_date = '2017-10-01'
to_date = '2017-12-31'

export_dir = r'E:\ecan\local\Projects\requests\martym\2018-11-26'
export_data = 'precip_402001_2017-10-01_2017-12-31.csv'
export_stats = 'stats.csv'
export_malf = 'malf7d.csv'
export_alf = 'alf7d.csv'

############################################
### Extract data


tsdata = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_col={'ExtSiteID': sites, 'DatasetTypeID': dataset}, from_date=from_date, to_date=to_date)
tsdata.to_csv(os.path.join(export_dir, export_data), header=True)


###############################################
### malf
#
#stats = flow_stats(tsdata)
#
#malf, alf, mis_alf, f, a = malf7d(tsdata, intervals=[10, 20], return_alfs=True)
#
###############################################
#### Combine and save
#
#stats.to_csv(os.path.join(export_dir, export_stats))
#malf.to_csv(os.path.join(export_dir, export_malf))
#alf.to_csv(os.path.join(export_dir, export_alf))
#
#
#r_site = ['219510']
