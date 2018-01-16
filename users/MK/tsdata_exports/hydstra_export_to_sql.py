# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Script to extract time series data from Hydstra and save them to sql tables.

Must be run in a 32bit python!
"""
import os
import pandas as pd
from datetime import date
from hydropandas.io.tools.mssql import rd_sql, to_mssql
from hydropandas.io.tools.hydllp import rd_hydstra

#############################################
### Parameters
base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

hydstra_code_sql = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraLink'}

today1 = date.today()
#today1 = date(2017, 12, 22)
#server1 = 'SQL2012DEV01'
#database1 = 'HydstraArchive'
#dtype_dict = {'wtemp': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'flow': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'precip': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'swl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'gwl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'lakel': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}}

max_date_stmt = 'select max(ModDate) from HydstraTSDataDaily'

sql_hourly = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraTSDataHourly'}
sql_daily = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraTSDataDaily'}

grp_dict = {'daily': {'interval': 'day', 'export': sql_daily}, 'hourly': {'interval': 'hour', 'export': sql_hourly}}

#############################################
### Get the SQL-hydstra codes and last date

hydstra_codes = rd_sql(**hydstra_code_sql).set_index('HydstraCode')['FeatureMtypeSourceID'].to_dict()

last_date1 = rd_sql(stmt=max_date_stmt, **sql_daily).loc[0][0]
last_date1 = '2017-12-22'

#############################################
### Iterate through hydstra codes and save as hdf files

for j in grp_dict:
    for i in hydstra_codes:
        print('HydstraCode ' + str(i))
        s1 = rd_hydstra(int(i), from_mod_date=last_date1, to_mod_date=str(today1), code_convert=hydstra_codes, **grp_dict[j])




#t7 = read_hdf(join(base_dir, hydstra_code_dict[i] + '_' + str(today1) + '.h5'))


