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
crc_table = 'CrcAllo'
crc_wap_table = 'CrcWapAllo'

sel_dict = {'CatchmentGroupName': ['Hurunui River', 'Waiau River']}

date = '2019-02-27'

export_path = r'E:\ecan\local\Projects\requests\dylan\2019-02-27'
export1 = 'lf_crc_bands_2019-02-28.csv'

###########################################
### Get data

sites = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'SwazName', 'SwazGroupName', 'CatchmentGroupName'], sel_dict)
sites.rename(columns={'ExtSiteID': 'wap'}, inplace=True)

lf_band1 = mssql.rd_sql(server, database, lf_band_table, ['site', 'band_num', 'min_trig', 'max_trig'], {'site': sites.wap.tolist()}, from_date=date, to_date=date, date_col='date')

lf_crc1 = mssql.rd_sql(server, database, lf_crc_table, ['site', 'band_num', 'crc'], {'site': lf_band1.site.unique().tolist(), 'band_num': lf_band1.band_num.unique().tolist()}, from_date=date, to_date=date, date_col='date')
#
#both1 = pd.merge(lf_crc1, lf_band1, on=['site', 'band_num'])
#
#both2 = pd.merge(both1, sites, on=['site'])

crc_wap1 = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'wap'], {'wap': sites.wap.tolist(), 'take_type': ['Take Surface Water']})
crc_wap1 = crc_wap1.drop_duplicates()

crc_wap1['minimum_flow'] = False
crc_wap1.loc[crc_wap1.crc.isin(lf_crc1.crc.unique()), 'minimum_flow'] = True

crc1 = mssql.rd_sql(server, database, crc_table, ['crc', 'take_type', 'from_date', 'to_date', 'crc_status', 'max_rate_crc', 'feav', 'use_type'], {'crc': crc_wap1.crc.unique().tolist(), 'take_type': ['Take Surface Water']})
crc1 = crc1.drop_duplicates(['crc', 'take_type'])

both3 = pd.merge(crc_wap1, crc1, on=['crc', 'take_type'])

both4 = pd.merge(both3, sites, on='wap')

both4.to_csv(os.path.join(export_path, export1), index=False)







