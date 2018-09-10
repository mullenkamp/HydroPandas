# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from hilltoppy import web_service as ws
from hilltoppy import hilltop

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

sites = ['J36/0016', 'K37/3262', '69302']
datasets = [5, 9, 12]

rdr_site = 'J36/0016-M1'
rwl_site = 'K37/3262-M1'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-09-10'
export1 = 'rdr_rwl_usage_data_2018-09-10.csv'


############################################
### Extract data

## Pull out recorder data

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': sites, 'DatasetTypeID': datasets})
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = tsdata1[tsdata1.DateTime >= '2005-01-01']

grp1 = tsdata2.groupby(['ExtSiteID', 'DatasetTypeID'])

flow1 = tsdata2[tsdata2.DatasetTypeID == 5].copy()
vol1 = tsdata2[tsdata2.DatasetTypeID == 9].copy()
vol1.Value = vol1.Value/24/60/60

rdr_data1 = ws.get_data(base_url, hts, rdr_site, 'Flow [Flow]', agg_method='Average', agg_interval='1 day')
rdr_data1.Value = rdr_data1.Value*0.001

rdr_data2 = rdr_data1.reset_index().drop('Measurement', axis=1).copy()
rdr_data2.DateTime = pd.to_datetime(rdr_data2.DateTime.dt.date)
rdr_data2.Site = 'J36/0016'
rdr_data2.rename(columns={'Site': 'ExtSiteID'}, inplace=True)

#rwl_mtype = ws.measurement_list(base_url, hts, rwl_site)
rwl_data1 = ws.get_data(base_url, hts, rwl_site, 'Flow [Flow]', agg_method='Average', agg_interval='1 day')
rwl_data1.Value = rwl_data1.Value*0.001

rwl_data2 = rwl_data1.reset_index().drop('Measurement', axis=1).copy()
rwl_data2.DateTime = pd.to_datetime(rwl_data2.DateTime.dt.date)
rwl_data2.Site = 'K37/3262'
rwl_data2.rename(columns={'Site': 'ExtSiteID'}, inplace=True)

combo1 = pd.concat([flow1.drop('DatasetTypeID', axis=1), vol1.drop('DatasetTypeID', axis=1), rdr_data2, rwl_data2])

combo2 = combo1.groupby(['ExtSiteID', 'DateTime']).Value.last()

combo3 = combo2.unstack(0)

combo3.to_csv(os.path.join(export_dir, export1))



#ml4 = ws.measurement_list(base_url, hts, rdr_site)


