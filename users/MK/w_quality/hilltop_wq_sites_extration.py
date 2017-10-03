# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 08:51:46 2017

@author: MichaelEK
"""

from core.ecan_io.hilltop import rd_hilltop_sites, rd_ht_quan_data, rd_ht_wq_data
from pandas import Series
from core.ecan_io import write_sql


#############################################
### Parameters

sites_dtype = {'site': 'VARCHAR(64)', 'data_source': 'VARCHAR(64)', 'mtype': 'VARCHAR(49)', 'mtype': 'VARCHAR(49)', 'unit': 'VARCHAR(49)', 'start_date': 'DATETIME', 'end_date': 'DATETIME'}

data_dtype = {'site': 'VARCHAR(64)', 'mtype': 'VARCHAR(49)', 'time': 'DATETIME', 'data': 'VARCHAR(19)'}

server = 'SQL2012DEV01'
database = 'Hydro'
sites_table = 'WQ_sites'
data_table = 'WQ_data'


hts = r'\\hilltop01\Hilltop\Data\Squalarc.hts'
#hts = r'\\hilltop01\Hilltop\Data\Annual\Selwyn.hts'

#sites = ['SQ36287']

#df1 = rd_ht_quan_data(hts)
df5, sites_info = rd_ht_wq_data(hts, output_site_data=True)

sites_info2 = sites_info.drop('divisor', axis=1)

###############################################
### Dump on sql database

#write_sql(server, database, sites_table, sites_info2, sites_dtype, drop_table=True)

df5.to_csv(r'E:\ecan\shared\projects\end_of_squalarc\wq_data.csv', encoding='utf-8', index=False)
sites_info2.to_csv(r'E:\ecan\shared\projects\end_of_squalarc\wq_site_data.csv', encoding='utf-8', index=False)

##############################################
### Testing

sites1 = rd_hilltop_sites(hts)
param2 = Series(sites1.mtype.unique())


param2[param2.str.contains('nitrogen', case=False, na=False)].sort_values()

param2[param2.str.contains('nitrate', case=False, na=False)].sort_values()
param2[param2.str.contains('nitrite', case=False, na=False)].sort_values()

sites_df = sites1.copy()






































































