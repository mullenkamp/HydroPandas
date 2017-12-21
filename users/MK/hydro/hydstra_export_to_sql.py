# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Script to extract time series data from Hydstra and save them to sql tables.

Must be run in a 32bit python!
"""

from core.ecan_io import rd_sql, write_sql
from core.ecan_io.hydllp import rd_hydstra
from pandas import concat, read_hdf, read_csv, merge, Timestamp, to_datetime, DataFrame, to_numeric, DateOffset
from os.path import join
from datetime import date
from core.misc.misc import save_df

#############################################
### Parameters
base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

today1 = date.today()

#server1 = 'SQL2012DEV01'
#database1 = 'HydstraArchive'
#dtype_dict = {'wtemp': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'flow': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'precip': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'swl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'gwl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'lakel': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}}

#sql_table_dict = {130: 'F_HY_Lakel_data', 100: 'F_HY_SWL_data', 140: 'F_HY_Flow_Data', 143: 'F_HY_Flow_Data'}
hydstra_code_dict = {130: 'lakel_data', 100: 'swl_data', 140: 'flow_data', 450: 'wtemp_data', 10: 'precip_data', 110: 'gwl_data'}


#############################################
### Iterate through hydstra codes and save as hdf files

for i in hydstra_code_dict:
    s1 = rd_hydstra(i, interval='hour')
    save_df(s1, join(base_dir, hydstra_code_dict[i] + '_' + str(today1) + '.h5'))







