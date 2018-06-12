# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 14:34:06 2018

@author: michaelek
"""

from pdsql import mssql


###########################################
### Parameters

sites = [69635]
bands = [1]

server = 'sql2012dev01'
database = 'hydro'
restr_table = 'LowFlowRestrSiteBand'
flow_table = 'TSDataNumericDaily'

restr_cols = ['site', 'band_num', 'date', 'waterway', 'location', 'flow', 'min_trig', 'max_trig', 'band_allo']

from_date = '2013-01-01'
to_date = '2018-03-31'

base_dir = r'E:\ecan\local\Projects\requests\dan\2018-04-26'
export_flow = '69635_flow.csv'
export_restr = '69635_restr.csv'

###########################################
### Extract the data

## Restrictions

restr1 = mssql.rd_sql(server, database, restr_table, restr_cols, where_col={'site': sites, 'band_num': bands}, from_date=from_date, to_date=to_date, date_col='date')

## Flow

flow1 = mssql.rd_sql(server, database, flow_table, where_col={'ExtSiteID': sites, 'DatasetTypeID': [5]}, from_date=from_date, to_date=to_date, date_col='DateTime')

flow2 = flow1.drop(['DatasetTypeID', 'ModDate'], axis=1)

