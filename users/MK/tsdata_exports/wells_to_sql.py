# -*- coding: utf-8 -*-
"""
Created on Tue Jan  9 09:28:21 2018

@author: MichaelEK
"""

import pandas as pd
import os
from datetime import date
from hydropandas.io.tools.mssql import to_mssql, rd_sql_ts, rd_sql, create_mssql_table
from hydropandas.util.misc import save_df

##############################################
### Parameters

gwl_m_dict = {'server': 'SQL2012PROD05', 'database': 'Wells', 'table': 'DTW_READINGS', 'groupby_cols': ['WELL_NO', 'timestamp'], 'date_col': 'DATE_READ', 'values_cols': 'DEPTH_TO_WATER', 'where_col': {'TIDEDA_FLAG': 'N'}}
gwl_m_names_dict = {'WELL_NO': 'Site', 'DATE_READ': 'Time', 'DEPTH_TO_WATER': 'Value', 'timestamp': 'ChangeStamp'}

gwl_sites_dict = {'server': 'SQL2012PROD05', 'database': 'Wells', 'table': 'WELL_DETAILS', 'site_col': 'WELL_NO', 'date_col': 'DATE_READ', 'data_col': 'DEPTH_TO_WATER', 'qual_col': None, 'add_where': "TIDEDA_FLAG='N'"}


dtype_dict = {'Site': 'VARCHAR(19)', 'HydroID': 'VARCHAR(29)', 'Time': 'DATETIME', 'Value': 'float', 'ChangeStamp': 'int'}
export = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'WellsTSData'}
pkey = ['Site', 'HydroID', 'Time']

################################################
### Extract data

wl1 = rd_sql_ts(**gwl_m_dict)
wl2 = wl1.reset_index().dropna().drop_duplicates(['WELL_NO', 'DATE_READ'])
wl2['timestamp'] = wl2.timestamp.apply(lambda x: int.from_bytes(x, 'big'))
wl2['HydroID'] = 'aq / wl / mfield / qc'
wl2.rename(columns=gwl_m_names_dict, inplace=True)
wl2['Value'] = wl2['Value'].round(4)

create_mssql_table(primary_keys=pkey, dtype_dict=dtype_dict, **export)
to_mssql(wl2, **export)









