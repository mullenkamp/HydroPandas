# -*- coding: utf-8 -*-
"""
Created on Tue Dec 05 09:34:22 2017

@author: MichaelEK
"""
import numpy as np
import pandas as pd
from datetime import datetime
from low_flows import low_flow_restr
from pdsql.mssql import to_mssql, create_table, rd_sql, del_table_rows

############################################
### Parameters

sql_site_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowRestrSite'}
sql_site_band_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowRestrSiteBand'}
#site_dtype_dict = {'site': 'varchar(19)', 'date': 'date', 'waterway': 'varchar(59)', 'location': 'varchar(59)', 'site_type': 'varchar(9)', 'flow_method': 'varchar(29)', 'days_since_flow_est': 'int', 'flow': 'numeric(10,3)', 'crc_count': 'int', 'min_trig': 'numeric(10,3)', 'max_trig': 'numeric(10,3)', 'restr_category': 'varchar(9)'}
#site_band_dtype_dict = {'site': 'varchar(19)', 'band_num': 'int', 'date': 'date', 'waterway': 'varchar(59)', 'location': 'varchar(59)', 'site_type': 'varchar(9)', 'flow_method': 'varchar(29)', 'days_since_flow_est': 'int', 'flow': 'numeric(10,3)', 'crc_count': 'int', 'min_trig': 'numeric(10,3)', 'max_trig': 'numeric(10,3)', 'band_allo': 'int'}

#site_pkey = ['site', 'date']
#site_band_pkey = ['site', 'band_num', 'date']

last_date_stmt = "select max(Time) from ExtractionLog where HydroTable='LowFlowRestrSite' and RunResult='pass'"

sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

###########################################
### Run function
today = datetime.today()
today1 = str(today.date())

try:
    print('Get last day of extraction')
    last_date1 = rd_sql(stmt=last_date_stmt, **sql_log).loc[0][0].date()

    print('Querying LowFlows db')

    basic, complete = low_flow_restr(from_date=str(last_date1), to_date=today1, only_restr=False)

    ## Backfill if needed
    #basic, complete = low_flow_restr(from_date='2017-10-01', to_date='2018-01-14', only_restr=False)
    #basic.apply(lambda x: x.astype(str).str.len().max())
    #basic['days_since_flow_est'] = np.nan
    #complete['days_since_flow_est'] = np.nan

    ### mssql stuff
    ## Create table
    #tab1 = create_mssql_table(dtype_dict=site_dtype_dict, primary_keys=site_pkey, **sql_site_dict)
    #tab2 = create_mssql_table(dtype_dict=site_band_dtype_dict, primary_keys=site_band_pkey, **sql_site_band_dict)

    ## Save data
    print('Saving data')

    del_table_rows(from_date=str(last_date1), to_date=today1, date_col='date', **sql_site_dict)
    del_table_rows(from_date=str(last_date1), to_date=today1, date_col='date', **sql_site_band_dict)

    to_mssql(basic, **sql_site_dict)
    to_mssql(complete, **sql_site_band_dict)

    print('Success')

    today2 = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    today3 = str(today.strftime('%Y-%m-%d %H:%M:%S'))

    log1 = pd.DataFrame([[today3, sql_site_dict['table'], 'pass', 'all good', str(last_date1), today2], [today3, sql_site_band_dict['table'], 'pass', 'all good', str(last_date1), today2]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log1, **sql_log)
except Exception as err:
    err1 = err
    print(err1)
    today2 = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    log2 = pd.DataFrame([[today3, sql_site_dict['table'], 'pass', str(err1), str(last_date1), today2], [today1, sql_site_band_dict['table'], 'pass', str(err1), str(last_date1), today2]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log2, **sql_log)
















