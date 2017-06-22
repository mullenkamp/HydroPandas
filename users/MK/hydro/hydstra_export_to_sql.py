# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Must be run in a 32bit python!
"""

#### Hydstra export improvement

from core.ecan_io import rd_sql, rd_hydstra_by_var, write_sql
from pandas import concat, to_numeric, read_csv

### SQL Parameters
server = 'SQL2012PROD03'
db = 'Hydstra'
period_tab = 'PERIOD'
var_tab = 'VARIABLE'
site_tab = 'SITE'
qual_tab = 'QUALITY'

period_cols = ['STATION', 'DATASOURCE', 'VARFROM', 'VARIABLE', 'PERSTART', 'PEREND']
period_names = ['site', 'datasource', 'varfrom', 'varto', 'start', 'end']
var_cols = ['VARNUM', 'VARNAM', 'VARUNIT', 'SHORTNAME']
var_names = ['var_num', 'var_name', 'var_unit', 'var_short_name']
site_cols = ['STATION', 'STNAME', 'SHORTNAME']
site_names = ['site', 'site_name', 'site_short_name']
qual_cols = ['QUALITY', 'TEXT']
qual_names = ['qual_code', 'qual_name']

#mtype_dict = {'swl': [100, 'mean', r'E:\ecan\shared\base_data\swl\swl_data.csv'], 'precip': [10, 'tot', r'E:\ecan\shared\base_data\precip\precip_data.csv'], 'gwl': [110, 'mean', r'E:\ecan\shared\base_data\gwl\gwl_data.csv'], 'lakel': [130, 'mean', r'E:\ecan\shared\base_data\lakel\lakel_data.csv'], 'wtemp': [450, 'mean', r'E:\ecan\shared\base_data\wtemp\wtemp_data.csv']}
mtype_dict = {'swl': [100, 'mean', r'C:\ecan\shared\base_data\swl\swl_data.csv'], 'precip': [10, 'tot', r'C:\ecan\shared\base_data\precip\precip_data.csv'], 'gwl': [110, 'mean', r'C:\ecan\shared\base_data\gwl\gwl_data.csv'], 'lakel': [130, 'mean', r'C:\ecan\shared\base_data\lakel\lakel_data.csv'], 'wtemp': [450, 'mean', r'C:\ecan\shared\base_data\wtemp\wtemp_data.csv']}

### Export parameters

end = '2017-05-21'

server1 = 'SQL2012DEV01'
database1 = 'HydstraArchive'
dtype_dict = {'wtemp': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'flow': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'precip': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'swl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'gwl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'lakel': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}}

flow_export = r'C:\ecan\shared\base_data\flow\flow_data.csv'
#precip_export = r'E:\ecan\shared\base_data\precip\precip_data.csv'
#gwl_export = r'E:\ecan\shared\base_data\gwl\gwl_data.csv'
#swl_export = r'E:\ecan\shared\base_data\swl\swl_data.csv'

### Import
period1 = rd_sql(server, db, period_tab, period_cols, where_col='DATASOURCE', where_val=['A'])
period1.columns = period_names
period1.loc[:, 'site'] = period1.site.str.strip()

var1 = rd_sql(server, db, var_tab, var_cols)
var1.columns = var_names

site1 = rd_sql(server, db, site_tab, site_cols)
site1.columns = site_names

qual1 = rd_sql(server, db, qual_tab, qual_cols)
qual1.columns = qual_names

### Determine the variables to extract
period2 = period1[period1.varto.isin(period1.varto.round())].sort_values('site')
period2 = period2[period2.varto != 101]
data_vars1 = period2.varto.sort_values().unique()
var2 = var1[var1.var_num.isin(data_vars1)]

### Extract and save data

## Precip data
i = 'precip'
precip = rd_hydstra_by_var(mtype_dict[i][0], end_time=end, data_type=mtype_dict[i][1], export=True, export_path=mtype_dict[i][2], sites_chunk=30)

write_sql(server1, database1, i + '_data', precip.reset_index(), dtype_dict[i], drop_table=True)

## swl data
i = 'swl'
swl = rd_hydstra_by_var(mtype_dict[i][0], end_time=end, data_type=mtype_dict[i][1], export=True, export_path=mtype_dict[i][2])

write_sql(server1, database1, i + '_data', swl.reset_index(), dtype_dict[i], drop_table=True)

## gwl data
i = 'gwl'
gwl = rd_hydstra_by_var(mtype_dict[i][0], end_time=end, data_type=mtype_dict[i][1], export=True, export_path=mtype_dict[i][2])

gwl2 = gwl.reset_index()
gwl2.loc[:, 'site'] = gwl2.loc[:, 'site'].str.replace('_', '/')
gwl3 = gwl2.set_index(['site', 'time'])
gwl3.to_csv(mtype_dict[i][2])

write_sql(server1, database1, i + '_data', gwl2, dtype_dict[i], drop_table=True)

## lakel data
i = 'lakel'
lakel = rd_hydstra_by_var(mtype_dict[i][0], end_time=end, data_type=mtype_dict[i][1], export=True, export_path=mtype_dict[i][2])

write_sql(server1, database1, i + '_data', lakel.reset_index(), dtype_dict[i], drop_table=True)

## wtemp data
i = 'wtemp'
wtemp = rd_hydstra_by_var(mtype_dict[i][0], end_time=end, data_type=mtype_dict[i][1], export=True, export_path=mtype_dict[i][2])

write_sql(server1, database1, i + '_data', wtemp.reset_index(), dtype_dict[i], drop_table=True)

## Flow data
flow1 = rd_hydstra_by_var(140, end_time=end, data_type='mean', sites_chunk=26)
flow2 = rd_hydstra_by_var(143, end_time=end, data_type='mean')

flow2.loc[:, 'data'] = flow2.loc[:, 'data'] * 0.001

flow = concat([flow1, flow2])
flow.to_csv(flow_export)

write_sql(server1, database1, i + '_data', flow.reset_index(), dtype_dict[i], drop_table=True)



