# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 15:18:25 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql

pd.options.display.max_columns = 10

####################################
### Parameters

server = 'sql2012prod04'
db = 'bgauging'
table = 'BGauging'

hydro_s = 'sql2012test01'
hydro_db = 'hydro'
sites_table = 'ExternalSite'

from_date = '2018-01-01'
to_date = '2018-11-27'

names_entered = ['RileyB', 'PeterMa']

cols = ['RiverSiteIndex', 'Date', 'Party', 'LogonIdEntered', 'EnterDate']

output_path = r'E:\ecan\local\Projects\requests\helen\2018-11-14'
output1 = 'gaugers_2018-11-27.csv'
output2 = 'gaugers_count_2018-11-27.csv'

#####################################
### Run query

data1 = mssql.rd_sql(server, db, table, cols, from_date=from_date, to_date=to_date, date_col='Date')

data1.rename(columns={'RiverSiteIndex': 'ExtSiteID', 'Date': 'GaugedDate', 'EnterDate': 'EnteredDate'}, inplace=True)

data1.sort_values('GaugedDate', inplace=True)

data1.GaugedDate = pd.to_datetime(data1.GaugedDate).dt.date

count1 = data1.groupby('LogonIdEntered').GaugedDate.count().sort_values()

sites1 = mssql.rd_sql(hydro_s, hydro_db, sites_table, ['ExtSiteID', 'ExtSiteName'], where_col={'ExtSiteID': data1.ExtSiteID.tolist()})

sites1.ExtSiteID = sites1.ExtSiteID.astype(int)

data2 = pd.merge(sites1, data1, on='ExtSiteID')

data2.to_csv(os.path.join(output_path, output1), index=False)

count1.to_csv(os.path.join(output_path, output2), index=True)

























