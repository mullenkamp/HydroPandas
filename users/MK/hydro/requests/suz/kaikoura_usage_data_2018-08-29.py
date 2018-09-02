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
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'
crc_table = 'CrcAllo'

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = [9, 12]
cwms = ['kaikoura']
#rdr_site = 'J36/0016-M1'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-08-29'
export1 = 'kaikoura_usage_data_2018-08-29.csv'
export2 = 'kaikoura_crc_usage_summary_2018-08-29.csv'

############################################
### Extract data

## Pull out recorder data
sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CwmsName': cwms}).ExtSiteID.tolist()

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': sites1, 'DatasetTypeID': datasets}, from_date='2017-07-01', to_date='2018-06-30', date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap'], where_col={'wap': sites1})
crc = mssql.rd_sql(server, database, crc_table, where_col={'crc': crc_wap.crc.unique().tolist(), 'crc_status': ['Issued - Active']}).drop('mod_date', axis=1)

crc_wap1 = crc_wap[crc_wap.crc.isin(crc.crc.unique())]

tsdata2 = tsdata1.groupby(['ExtSiteID']).Value.sum().round().reset_index()
tsdata2.rename(columns={'ExtSiteID': 'wap', 'Value': 'usage'}, inplace=True)

crc_wap2 = pd.merge(crc_wap1, crc, on=['crc', 'take_type', 'allo_block'], how='left')

crc_wap3 = pd.merge(crc_wap2, tsdata2, on='wap', how='left')


combo2 = tsdata1.groupby(['ExtSiteID', 'DateTime']).Value.last().round(2)

combo3 = combo2.unstack(0)

combo3.to_csv(os.path.join(export_dir, export1))
crc_wap3.to_csv(os.path.join(export_dir, export2), index=False)


#ml4 = ws.measurement_list(base_url, hts, rdr_site)


