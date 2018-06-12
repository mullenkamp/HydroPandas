# -*- coding: utf-8 -*-
"""
Created on Mon Apr 30 09:17:59 2018

@author: michaelek
"""
import os
from pdsql import mssql


####################################################
### Parameters

server = 'sql2012dev01'
database = 'hydro'

ts_table = 'TSDataNumericDaily'
sites_table = 'ExternalSite'

sites = ['68001', '68002', '68006']

base_dir = r'E:\ecan\local\Projects\requests\justin_rogers\2018-04-30'

export_ts = 'ts_data.csv'
export_sites = 'sites_loc.csv'

####################################################
### Extract data

ts_data = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value', 'QualityCode'], where_col={'ExtSiteID': sites, 'DatasetTypeID': [5]})

site_data = mssql.rd_sql(server, database, sites_table, where_col={'ExtSiteID': sites})

####################################################
### Export data

ts_data.to_csv(os.path.join(base_dir, export_ts), index=False)
site_data.to_csv(os.path.join(base_dir, export_sites), index=False)



























