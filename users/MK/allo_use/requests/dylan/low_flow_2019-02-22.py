# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 11:35:58 2019

@author: michaelek
"""
import os
import pandas as pd
from pdsql import mssql
from allotools import AlloUsage
from datetime import datetime

pd.options.display.max_columns = 10


############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
sites_table = 'ExternalSite'
lf_band_table = 'LowFlowRestrSiteBand'
lf_crc_table = 'LowFlowRestrSiteBandCrc'

sel_dict = {'SwazGroupName': ['Hurunui', 'Waiau']}

date = '2019-02-22'

export_path = r'E:\ecan\local\Projects\requests\dylan\2019-02-22'
export1 = 'lf_crc_bands_2019-02-22.csv'

###########################################
### Get data

sites = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'SwazName', 'SwazGroupName'], sel_dict)
sites.rename(columns={'ExtSiteID': 'site'}, inplace=True)

lf_band1 = mssql.rd_sql(server, database, lf_band_table, ['site', 'band_num', 'min_trig', 'max_trig'], {'site': sites.site.tolist()}, from_date=date, to_date=date, date_col='date')

lf_crc1 = mssql.rd_sql(server, database, lf_crc_table, ['site', 'band_num', 'crc'], {'site': lf_band1.site.unique().tolist(), 'band_num': lf_band1.band_num.unique().tolist()}, from_date=date, to_date=date, date_col='date')

both1 = pd.merge(lf_crc1, lf_band1, on=['site', 'band_num'])

both2 = pd.merge(both1, sites, on=['site'])

both2.to_csv(os.path.join(export_path, export1), index=False)







