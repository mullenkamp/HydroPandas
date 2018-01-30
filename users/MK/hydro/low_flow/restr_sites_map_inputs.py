# -*- coding: utf-8 -*-
"""
Created on Tue Dec 05 09:34:22 2017

@author: MichaelEK
"""
import numpy as np
from datetime import date
from core.allo_use.ros import low_flow_restr
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table, rd_sql, del_mssql_table_rows

############################################
### Parameters

sql_site_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowRestrSite'}
sql_site_band_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowRestrSiteBand'}
#site_dtype_dict = {'site': 'varchar(19)', 'date': 'date', 'waterway': 'varchar(59)', 'location': 'varchar(59)', 'site_type': 'varchar(9)', 'flow_method': 'varchar(29)', 'days_since_flow_est': 'int', 'flow': 'numeric(10,3)', 'crc_count': 'int', 'min_trig': 'numeric(10,3)', 'max_trig': 'numeric(10,3)', 'restr_category': 'varchar(9)'}
#site_band_dtype_dict = {'site': 'varchar(19)', 'band_num': 'int', 'date': 'date', 'waterway': 'varchar(59)', 'location': 'varchar(59)', 'site_type': 'varchar(9)', 'flow_method': 'varchar(29)', 'days_since_flow_est': 'int', 'flow': 'numeric(10,3)', 'crc_count': 'int', 'min_trig': 'numeric(10,3)', 'max_trig': 'numeric(10,3)', 'band_allo': 'int'}

#site_pkey = ['site', 'date']
#site_band_pkey = ['site', 'band_num', 'date']

###########################################
### Run function
print('Querying LowFlows db')

today1 = str(date.today())

basic, complete = low_flow_restr(from_date=today1, to_date=today1, only_restr=False)

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

del_mssql_table_rows(from_date=today1, to_date=today1, date_col='date', **sql_site_dict)
del_mssql_table_rows(from_date=today1, to_date=today1, date_col='date', **sql_site_band_dict)

to_mssql(basic, **sql_site_dict)
to_mssql(complete, **sql_site_band_dict)

print('Success')

















