# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from hilltoppy import web_service as ws

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

sites = ['J36/0016', 'K37/3262', '69302']
datasets = [5, 9, 12]

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-08-23'
export1 = 'rdr_rwl_usage_data_2018-08-23.csv'


############################################
### Extract data

## Pull out recorder data

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': sites, 'DatasetTypeID': datasets})
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = tsdata1[tsdata1.DateTime >= '2005-01-01']

grp1 = tsdata2.groupby(['ExtSiteID', 'DatasetTypeID'])

grp1.min()
grp1.max()

flow1 = tsdata2[tsdata2.DatasetTypeID == 5].copy()
vol1 = tsdata2[tsdata2.DatasetTypeID == 9].copy()
vol1.Value = vol1.Value/24/60/60

ml1 = ws.measurement_list(base_url, hts, 'J36/0016-M1')
rdr_data = ws.get_data(base_url, hts, 'J36/0016-M1', 'Flow [Flow]', agg_method='Average', agg_interval='1 day')

rdr_data2 = (rdr_data*0.001).reset_index().drop('Measurement', axis=1).copy()
rdr_data2.DateTime = pd.to_datetime(rdr_data2.DateTime.dt.date)
rdr_data2.Site = 'J36/0016'
rdr_data2.rename(columns={'Site': 'ExtSiteID'}, inplace=True)

combo1 = pd.concat([flow1.drop('DatasetTypeID', axis=1), vol1.drop('DatasetTypeID', axis=1), rdr_data2]).set_index(['ExtSiteID', 'DateTime']).Value

combo2 = combo1.unstack(0)

combo2.to_csv(os.path.join(export_dir, export1))






