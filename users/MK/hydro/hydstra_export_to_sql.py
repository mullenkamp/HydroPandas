# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Script to extract time series data from Hydstra and save them to sql tables.

Must be run in a 32bit python!
"""

#### Hydstra export improvement

from core.ecan_io import rd_sql, rd_hydstra_by_var, write_sql, rd_hydstra_db
from core.ecan_io.mssql import site_stat_stmt, sql_del_rows_stmt
from pandas import concat, read_hdf, read_csv, merge, Timestamp, to_datetime, DataFrame, to_numeric, DateOffset
from os.path import join
from datetime import date, timedelta
from time import time
from core.misc import save_df
from core.ts import grp_ts_agg

### SQL Parameters
server = 'SQL2012PROD03'
db = 'Hydstra'
period_tab = 'PERIOD'
var_tab = 'VARIABLE'
site_tab = 'SITE'
qual_tab = 'QUALITY'
rates_tab = 'RATEPTS'

period_cols = ['STATION', 'DATASOURCE', 'VARFROM', 'VARIABLE', 'PERSTART', 'PEREND']
period_names = ['site', 'datasource', 'varfrom', 'varto', 'start', 'end']
var_cols = ['VARNUM', 'VARNAM', 'VARUNIT', 'SHORTNAME']
var_names = ['var_num', 'var_name', 'var_unit', 'var_short_name']
site_cols = ['STATION', 'STNAME', 'SHORTNAME']
site_names = ['site', 'site_name', 'site_short_name']
qual_cols = ['QUALITY', 'TEXT']
qual_names = ['qual_code', 'qual_name']
rates_cols = ['STATION', 'VARFROM', 'VARTO', 'DATEMOD']
rates_names = ['site', 'varfrom', 'varto', 'mod_date']

base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

mtype_dict = {'swl': [100, 'mean', r'swl\swl_data.csv'], 'precip': [10, 'tot', r'precip\precip_data.csv'], 'gwl': [110, 'mean', r'gwl\gwl_data.csv'], 'lakel': [130, 'mean', r'lakel\lakel_data.csv'], 'wtemp': [450, 'mean', r'wtemp\wtemp_data.csv'], 'flow': [[140, 143], 'mean', r'flow\flow_data.csv']}

### Export parameters
subdays = timedelta(days=2)
today1 = date.today()
end = (today1 - subdays).strftime('%Y-%m-%d')
start = '2010-01-01'

server1 = 'SQL2012DEV01'
database1 = 'HydstraArchive'
dtype_dict = {'wtemp': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'flow': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'precip': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 1)', 'qual_code': 'INT'}, 'swl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'gwl': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}, 'lakel': {'site': 'VARCHAR(19)', 'time': 'DATE', 'data': 'NUMERIC(10, 3)', 'qual_code': 'INT'}}

#sql_table_dict = {130: 'F_HY_Lakel_data', 100: 'F_HY_SWL_data', 140: 'F_HY_Flow_Data', 143: 'F_HY_Flow_Data'}
sql_table_dict = {130: 'lakel_data', 100: 'swl_data', 140: 'flow_data', 143: 'flow_data'}


#hy_period_csv = r'database\hydstra_period_2017-05-05.csv'
hy_period_csv = r'database\hydstra_period_' + str(today1) + '.csv'

### Import
#old_p = read_csv(join(base_dir, hy_period_csv))

period1 = rd_sql(server, db, period_tab, period_cols, where_col='DATASOURCE', where_val=['A'])
period1.columns = period_names
period1.loc[:, 'site'] = period1.site.str.strip()
period1.loc[:, 'datasource'] = period1.datasource.str.strip()

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

#save_df(period2, join(base_dir, hy_period_csv), index=False)
#
#old_p = read_csv(join(base_dir, hy_period_csv))

### Extract and save data

## Precip data
i = 'precip'
precip = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]), sites_chunk=30)

# Fix quality code
precip.loc[precip.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', precip.reset_index(), dtype_dict[i], drop_table=True)

## swl data
i = 'swl'
swl = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]))

# Fix quality code
swl.loc[swl.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', swl.reset_index(), dtype_dict[i], drop_table=True)

## gwl data
i = 'gwl'
gwl = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]))

gwl2 = gwl.reset_index()
gwl2.loc[:, 'site'] = gwl2.loc[:, 'site'].str.replace('_', '/')
gwl3 = gwl2.set_index(['site', 'time'])
gwl3.to_csv(join(base_dir, mtype_dict[i][2]))

# Fix quality code
gwl.loc[gwl.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', gwl2, dtype_dict[i], drop_table=True)

## lakel data
i = 'lakel'
lakel = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]))

# Fix quality code
lakel.loc[lakel.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', lakel.reset_index(), dtype_dict[i], drop_table=True)

## wtemp data
i = 'wtemp'
wtemp = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]))

# Fix quality code
wtemp.loc[wtemp.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', wtemp.reset_index(), dtype_dict[i], drop_table=True)

## Flow data
i = 'flow'
flow1 = rd_hydstra_by_var(140, start=start, end=end, data_type='mean', sites_chunk=10, print_sites=True)
flow2 = rd_hydstra_by_var(143, end=end, data_type='mean')

flow2.loc[:, 'data'] = flow2.loc[:, 'data'] * 0.001

flow = concat([flow1, flow2])

# Fix quality code
flow.loc[flow.qual_code == 18, 'qual_code'] = 50

# Output to sql table
flow.to_csv(join(base_dir, mtype_dict[i][2]))

write_sql(server1, database1, i + '_data', flow.reset_index(), dtype_dict[i], drop_table=True)


##############################
### Testing

site = [66401]
varto = 130

start1 = time()
t1 = rd_hydstra_db(site, start='1998-01-01', end='2017-01-01', data_type='mean', interval='day', varto=100)
end1 = time()

rem_dict = {'165131': [140, 140], '69302': [140, 140], '71106': [140, 140]}


### Determine the variables to extract
for i in rem_dict:
    period2 = period2[~((period2.site == i) & (period2.varfrom == rem_dict[i][0]) & (period2.varto == rem_dict[i][1]))]

### Extract the sites for the specific varto
period3 = period2[period2.varto == varto]


## lakel data
i = 'lakel'
lakel = rd_hydstra_by_var(mtype_dict[i][0], end=end, data_type=mtype_dict[i][1], export=True, export_path=join(base_dir, mtype_dict[i][2]))

# Fix quality code
lakel.loc[lakel.qual_code == 18, 'qual_code'] = 50

# Output to sql table
write_sql(server1, database1, i + '_data', lakel.reset_index(), dtype_dict[i], drop_table=True)

t2 = rd_hydstra_db([67603], start='2016-01-01', end='2016-01-03', data_type='mean', interval='hour', varto=130, varfrom=130)
t3 = rd_hydstra_db([67603], start='2016-01-01', end='2016-01-03', data_type='point', interval='hour', varto=130, varfrom=130)

t2.index = t2.index.droplevel('site')
t3.index = t3.index.droplevel('site')
t4 = t3.resample('H', label='right').mean().round(3)

concat([t2, t4], axis=1)

t2 = rd_hydstra_db([64608], start='2010-04-01', end='2010-05-01', data_type='mean', interval='hour', varfrom=100, varto=140)
#t5 = rd_hydstra_db([64608], start='2010-04-01', end='2010-05-01', data_type='mean', interval='day', varfrom=100, varto=140)
t3 = rd_hydstra_db([64608], start='2010-04-01', end='2010-05-01 01:00', data_type='point', interval='hour', varfrom=100, varto=140)

t2.index = t2.index.droplevel('site')
t3.index = t3.index.droplevel('site')
t5.index = t5.index.droplevel('site')

t4 = t3 * 60*5
t3_agg_h = t3.resample('H').mean().round(3)
t3_agg_d = t3_agg_h.resample('D').mean()
t4_agg_d = t4.resample('D').sum()/(60*60*24)

concat([t3_agg_d, t4_agg_d], axis=1)
concat([t2, t3_agg_h], axis=1)
concat([t5, t3_agg_d], axis=1)

time1 = time()
t2 = rd_hydstra_db([68502], start='1980-01-01 23:00', end='2017-10-01', data_type='mean', interval='hour', varfrom=100, varto=140)
time2 = time()
diff = time2 - time1
diff/len(t2)*1000

time1 = time()
t2 = rd_hydstra_db([70105], data_type='mean', interval='hour', varfrom=100, varto=140)
time2 = time()
diff = time2 - time1
diff/len(t2)*1000

t2 = rd_hydstra_db([68502], start='2010-01-01', end='2017-05-01', data_type='mean', interval='hour', varfrom=100, varto=140)
t2 = rd_hydstra_db([64608], start='2010-01-02', end='2010-05-01', data_type='mean', interval='hour', varfrom=100, varto=140)
t2 = rd_hydstra_db([64608], start='2010-04-01', end='2010-05-01 01:00', data_type='point', interval='hour', varfrom=100, varto=140)
t3 = rd_hydstra_db([64608], start='2010-04-01', end='2010-05-01', data_type='mean', interval='hour', varfrom=100, varto=140)

time1 = time()
hyd = openHyDb()
with hyd as h:
    ts_traces_request = h.query_by_dict(ts_traces_request)
time2 = time()
diff = time2 - time1
diff/len(t2)*1000

ts_traces_request = js1.copy()



##########################
### Inputs
varto = 140

server1 = 'SQL2012DEV01'
database1 = 'HydstraArchive'
server2 = 'SQL2012PROD03'
database2 = 'Hydstra'

site_col = 'site'
time_col = 'time'
data_col = 'data'
qual_col = 'qual_code'

period_tab = 'PERIOD'
var_tab = 'VARIABLE'
site_tab = 'SITE'
qual_tab = 'QUALITY'
rates_tab = 'RATEPTS'

period_cols = ['STATION', 'DATASOURCE', 'VARFROM', 'VARIABLE', 'PERSTART', 'PEREND']
period_names = ['site', 'datasource', 'varfrom', 'varto', 'start', 'end']
var_cols = ['VARNUM', 'VARNAM', 'VARUNIT', 'SHORTNAME']
var_names = ['var_num', 'var_name', 'var_unit', 'var_short_name']
site_cols = ['STATION', 'STNAME', 'SHORTNAME']
site_names = ['site', 'site_name', 'site_short_name']
qual_cols = ['QUALITY', 'TEXT']
qual_names = ['qual_code', 'qual_name']
rates_cols = ['STATION', 'VARFROM', 'VARTO', 'DATEMOD']
rates_names = ['site', 'varfrom', 'varto', 'mod_date']

### Check dates of existing data
table = sql_table_dict[varto]

max_date_stmt = site_stat_stmt(table, site_col, time_col, 'max')
max_date = rd_sql(server1, database1, stmt=max_date_stmt)
max_date.loc[:, site_col] = max_date.loc[:, site_col].astype(str)
max_date.loc[:, time_col] = to_datetime(max_date.loc[:, time_col])
max_date.columns = ['site', 'max_time']

min_date_stmt = site_stat_stmt(table, site_col, time_col, 'min')
min_date = rd_sql(server1, database1, stmt=min_date_stmt)
min_date.loc[:, site_col] = min_date.loc[:, site_col].astype(str)
min_date.loc[:, time_col] = to_datetime(min_date.loc[:, time_col])
min_date.columns = ['site', 'min_time']

data_dates = merge(min_date, max_date, on='site')
tab_max_date = max_date.max_time.max()

### Check dates of new data in Hydstra
period1 = rd_sql(server2, database2, period_tab, period_cols, where_col='DATASOURCE', where_val=['A'])
period1.columns = period_names
period1.loc[:, 'site'] = period1.site.str.strip()
period1.loc[:, 'datasource'] = period1.datasource.str.strip()
period1.loc[:, 'varfrom'] = to_numeric(period1.loc[:, 'varfrom'])
period1.loc[:, 'varto'] = to_numeric(period1.loc[:, 'varto'])
period1.loc[:, 'end'] = period1.loc[:, 'end'] - DateOffset(days=1)

period2 = period1[period1.varto.isin(period1.varto.round())].sort_values('site')
period2 = period2[period2.varto != 101]

### Check ratings to see if any have changed since max date (flow sites only)
if varto == 140:
    max_date_stmt = site_stat_stmt(rates_tab, 'STATION', 'DATEMOD', 'max')
    rates1 = rd_sql(server2, database2, stmt=max_date_stmt)
    rates1.columns = ['site', 'rate_date']
    rates1.loc[:, 'site'] = rates1.site.str.strip()
    rates2 = rates1[rates1['rate_date'] > tab_max_date]
    rates3 = period2[period2['site'].isin(rates2['site']) & (period2.varto == 140) & (period2.varfrom == 100)].copy()
    flow_sites = rates3.rename(columns={'start': 'pull_start', 'end': 'pull_end'})
else:
    flow_sites = DataFrame()

mtype_period = period2[period2.varto.isin([varto])]
combo_period = merge(data_dates, mtype_period, on='site', how='right')
combo_period1 = combo_period[(combo_period['min_time'].isnull()) | (combo_period['end'] > combo_period['max_time'])].copy()

combo_period1.loc[:, 'pull_start'] = combo_period1.loc[:, 'max_time'] + DateOffset(days=1)
combo_period1.loc[combo_period1['pull_start'].isnull(), 'pull_start'] = combo_period1.loc[combo_period1['pull_start'].isnull(), 'start']
combo_period1['pull_end'] = combo_period1['end']

combo_period2 = combo_period1.drop(['min_time', 'max_time', 'start', 'end'], axis=1).copy()

### Combine all sites to be extracted and remove duplicates
extract_sites = concat([combo_period2, flow_sites])
extract_sites1 = extract_sites.drop_duplicates(['site', 'datasource', 'varfrom', 'varto'], keep='last')


### Remove rows in the database that will be replaced










