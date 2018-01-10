# -*- coding: utf-8 -*-
"""
Created on Tue Dec 05 09:34:22 2017

@author: MichaelEK
"""
import pandas as pd
from datetime import date
from core.allo_use.ros import low_flow_restr
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table

############################################
### Parameters

sql_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowSiteRestr'}
dtype_dict = {'site': 'varchar(19)', 'date': 'date', 'Waterway': 'varchar(59)', 'Location': 'varchar(59)', 'flow_method': 'varchar(29)', 'days_since_flow_est': 'int', 'flow': 'numeric(10,3)', 'crc_count': 'int', 'min_trig': 'numeric(10,3)', 'max_trig': 'numeric(10,3)', 'restr_category': 'varchar(9)'}

#server = 'SQL2012DEV01'
#database = 'Hydro'
#table = 'LowFlowSiteRestr'

###########################################
### Run function

today1 = str(date.today())

basic, complete = low_flow_restr(from_date=today1, to_date=today1, only_restr=False)

basic['restr_category'] = 'No'
basic.loc[basic['below_min_trig'], 'restr_category'] = 'Full'
basic.loc[(basic['flow'] < basic['max_trig']) & (basic['flow'] > basic['min_trig']), 'restr_category'] = 'Partial'

num1 = pd.to_numeric(basic.site, 'coerce')
basic.loc[num1.isnull(), 'flow_method'] = 'GW'

#basic['restr_category'] = basic['restr_category'].astype('category')
#basic1.apply(lambda x: x.astype(str).str.len().max())

basic1 = basic.drop('below_min_trig', axis=1).copy()

### mssql stuff
## Create table
#tab1 = create_mssql_table(dtype_dict=dtype_dict, primary_keys=['site', 'date'], **sql_dict)

to_mssql(basic1, **sql_dict)
















