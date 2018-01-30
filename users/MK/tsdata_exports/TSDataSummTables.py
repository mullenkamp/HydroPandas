# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Script to extract time series data from Hydstra and save them to sql tables.

Must be run in a 32bit python!
"""
import os
import pandas as pd
from datetime import date, datetime
from hydropandas.io.tools.mssql import rd_sql, to_mssql, del_mssql_table_rows

#############################################
### Parameters
## Input

server = 'SQL2012DEV01'
database = 'Hydro'
site_link_table = 'SiteLinkMaster'

sql_dict = {'main': ['vTSDataNumeric', 'TSDataNumericSumm'], 'hourly': ['vTSDataNumericHourly', 'TSDataNumericHourlySumm'], 'daily': ['vTSDataNumericDaily', 'TSDataNumericDailySumm']}

select_stmt = "select Site, FeatureMtypeSourceID, min(Value) as Min, avg(Value) as Mean, max(Value) as Max, count(Value) as Count, min(Time) as FromDate, max(Time) as ToDate from "

groupby_stmt = " group by Site, FeatureMtypeSourceID"

today1 = str(date.today())
today2 = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

## Export

sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

#############################################
### Get the SQL summary from the main tables

site_link = rd_sql(server, database, site_link_table)

for i in sql_dict:
    view = sql_dict[i][0]
    table = sql_dict[i][1]
    summ1 = rd_sql(server, database, stmt=select_stmt + view + groupby_stmt)
    if len(summ1) > 18000:
        del_mssql_table_rows(server, database, table)
        to_mssql(summ1, server, database, table)
        log1 = pd.DataFrame([[today2, table, 'pass', 'all good', today2]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
        to_mssql(log1, **sql_log)


