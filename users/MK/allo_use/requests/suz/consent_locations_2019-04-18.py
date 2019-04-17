# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 11:08:39 2019

@author: michaelek
"""

import os
import pandas as pd
from pdsql import mssql

#######################################
### Parameters

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'

base_path = r'E:\ecan\local\Projects\requests\suz\2019-04-18'

request_csv = 'request.csv'

output_csv = 'consent_locations_2019-04-18.csv'


######################################
### Read data

crc1 = pd.read_csv(os.path.join(base_path, request_csv))
crc1.rename(columns={'Consent Number ': 'crc'}, inplace=True)

crc2 = crc1['crc'].copy()

crc_wap1 = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'wap'], where_in={'crc': crc2.tolist()}).drop_duplicates()

wap1 = crc_wap1.wap.unique()

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'], where_in={'ExtSiteID': wap1.tolist()})
sites1.rename(columns={'ExtSiteID': 'wap'}, inplace=True)

crc_wap2 = pd.merge(crc_wap1, sites1, on='wap')

crc_wap2.to_csv(os.path.join(base_path, output_csv), index=False)






































