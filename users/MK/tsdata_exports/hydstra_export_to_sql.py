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
from hydropandas.io.tools.mssql import rd_sql, to_mssql
from hydropandas.io.tools.hydllp import rd_hydstra

#############################################
### Parameters
#base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

hydstra_code_sql = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraLink'}

today1 = str(date.today())
today2 = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

qual_code_convert = [[10, 11, 18, 20, 21, 30, 31, 40, 50, 60], [600, 600, 520, 500, 500, 400, 400, 300, 200, 100]]
qual_code_dict = dict(zip(qual_code_convert[0], qual_code_convert[1]))
#today1 = date(2017, 12, 22)
#server1 = 'SQL2012DEV01'
#database1 = 'HydstraArchive'
#dtype_dict = {'wtemp': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'flow': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'precip': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'swl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'gwl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'lakel': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}}

max_date_stmt = "select max(Time) from ExtractionLog where HydroTable='HydstraTSDataDaily' and RunResult='pass'"

sql_hourly = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraTSDataHourly'}
sql_daily = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraTSDataDaily'}

grp_dict = {'daily': {'interval': 'day', 'export': sql_daily}, 'hourly': {'interval': 'hour', 'export': sql_hourly}}
sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

#############################################
### Get the SQL-hydstra codes and last date

hydstra_codes = rd_sql(**hydstra_code_sql).set_index('HydstraCode')['FeatureMtypeSourceID'].to_dict()

last_date1 = str(rd_sql(stmt=max_date_stmt, **sql_daily).loc[0][0].date())
#last_date1 = '2017-12-28'
#today2 = '2017-12-28 00:00:00'

print('Last sucessful date is ' + last_date1)

#############################################
### Iterate through hydstra codes and save as hdf files

for j in grp_dict:
    print('Interval: ' + j)
    try:
        for i in hydstra_codes:
            print('HydstraCode ' + str(i))
            s1 = rd_hydstra(int(i), from_mod_date=last_date1, to_mod_date=today1, code_convert=hydstra_codes, qual_code_convert=qual_code_dict, **grp_dict[j])
        log1 = pd.DataFrame([[today2, grp_dict[j]['export']['table'], 'pass', 'all good', last_date1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
        to_mssql(log1, **sql_log)
    except Exception as err:
        err1 = err
        print(err1)
        log2 = pd.DataFrame([[today2, grp_dict[j]['export']['table'], 'fail', str(err1), last_date1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
        to_mssql(log2, **sql_log)



#t7 = read_hdf(join(base_dir, hydstra_code_dict[i] + '_' + str(today1) + '.h5'))


