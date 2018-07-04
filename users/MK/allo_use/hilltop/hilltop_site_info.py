# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""
from numpy import array, nan
from os import path, getcwd, listdir
from pandas import concat, DataFrame, Timestamp, to_datetime
from configparser import ConfigParser
from datetime import datetime, timedelta
from re import search, IGNORECASE, findall
from pdsql.mssql import rd_sql, to_mssql, create_table, del_table_rows
from hilltoppy.com import rd_hilltop_sites
from hilltoppy.util import convert_site_names

######################################################
### additional functions


def rd_dir(data_dir, ext, file_num_names=False, ignore_case=True):
    """
    Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
    """

    if ignore_case:
        files = array([filename for filename in listdir(data_dir) if search('.' + ext + '$', filename, IGNORECASE)])
    else:
        files = array([filename for filename in listdir(data_dir) if search('.' + ext + '$', filename)])

    if file_num_names:
        site_names = array([int(findall("\d+", fi)[0]) for fi in files])
        return files, site_names
    else:
        return files


def wap_sw_gw(server, database, table, wap_col='WAP', take_type_col='Activity'):
    """
    Function to determine which WAP is SW and GW.
    """
    allo_wap = rd_sql(col_names=[take_type_col, wap_col], server=server, database=database, table=table, rename_cols=['take_type', 'wap'])
    allo_wap = allo_wap[allo_wap.wap != 'Migration: Not Classified'].copy()
    allo_wap['wap'] = allo_wap['wap'].str.strip().str.upper()
    list_names1 = allo_wap['wap'].str.findall('[A-Z]+\d\d/\d\d\d\d')
    names_len_bool = list_names1.apply(lambda x: len(x)) == 1
    names2 = allo_wap['wap'].copy()
    names2[names_len_bool] = list_names1[names_len_bool].apply(lambda x: x[0])
    names2[~names_len_bool] = nan
    allo_wap['wap'] = names2
    allo_wap2 = allo_wap.dropna().drop_duplicates()
    allo_wap3 = allo_wap2.sort_values('take_type').drop_duplicates(subset='wap', keep='last').copy()
    return allo_wap3


#######################################################
### Parameters
## Import

print('load parameters')

py_dir = path.realpath(path.join(getcwd(), path.dirname(__file__)))

ini1 = ConfigParser()
ini1.read([path.join(py_dir, path.splitext(__file__)[0] + '.ini')])

base_dir = str(ini1.get('Input', 'base_dir'))
hts_dirs = [i.strip() for i in ini1.get('Input', 'hts_dirs').split(',')]

exclude_hts = [i.strip() for i in ini1.get('Input', 'exclude_hts').split(',')]

#sql_wap_use = {'server': 'SQL2012DEV01', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterWAPAlloc'}

wap_server = str(ini1.get('Input', 'wap_server'))
wap_database = str(ini1.get('Input', 'wap_database'))
wap_table = str(ini1.get('Input', 'wap_table'))

time_format = '%Y-%m-%d %H:%M:%S'

## Export
server = str(ini1.get('Output', 'out_server'))
database = str(ini1.get('Output', 'out_database'))

log_server = str(ini1.get('Output', 'log_server'))
log_database = str(ini1.get('Output', 'log_database'))
log_table = str(ini1.get('Output', 'log_table'))

#sql_export = {'table': 'HilltopTSUsageDaily', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
#sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

summ_table_name = 'HilltopUsageSiteSummLog'
data_table_name = 'HilltopUsageSiteDataLog'

summ_dtype = {'date': 'DATE', 'n_unique_sites': 'int', 'n_unique_waps': 'int', 'n_tel_sites': 'int', 'n_waps_not_in_accela': 'int', 'n_sites_not_converted_to_wap': 'int', 'n_sites_data_yesterday': 'int', 'n_sites_data_2days_ago': 'int', 'n_waps_data_yesterday': 'int', 'n_waps_data_2days_ago': 'int'}

data_dtype = {'date': 'DATE', 'site': 'VARCHAR(59)', 'data_source': 'VARCHAR(19)', 'mtype': 'VARCHAR(59)', 'unit': 'VARCHAR(9)', 'start_date': 'DATETIME', 'end_date': 'DATETIME', 'hts_file': 'VARCHAR(59)', 'folder': 'VARCHAR(19)', 'wap': 'VARCHAR(19)', 'data_yesterday': 'bit', 'data_2_days_ago': 'bit', 'in_accela': 'bit'}

summ_col = ['date', 'run_time_start', 'n_unique_sites', 'n_unique_waps', 'n_tel_sites', 'n_waps_not_in_accela', 'n_sites_not_converted_to_wap', 'n_sites_data_yesterday', 'n_sites_data_2days_ago', 'n_waps_data_yesterday', 'n_waps_data_2days_ago']
summ_pkeys = ['date', 'run_time_start']

data_col = ['date', 'site', 'data_source', 'mtype', 'unit', 'start_date', 'end_date', 'hts_file', 'folder', 'wap', 'data_yesterday', 'data_2_days_ago', 'in_accela']
data_pkeys = ['date', 'site', 'mtype', 'hts_file']

today = datetime.today()
run_time_start = today.strftime(time_format)
yest = today - timedelta(1)

#del_mssql_table_rows(server, database, summ_table_name)
#del_mssql_table_rows(server, database, data_table_name)

try:

    ###################################################
    ### Load in WAP site data

    allo_wap = wap_sw_gw(wap_server, wap_database, wap_table)
    allo_wap.columns = ['take_type', 'Site']

    ####################################################
    print('Extracting ts data from Hilltop')

    ht_sites_lst = []
    for i in hts_dirs:
        hts_files = rd_dir(path.join(base_dir, i), '.hts')
        for j in hts_files:
            if j not in exclude_hts:
                print(path.join(i, j))
                ## Sites
                sdata = rd_hilltop_sites(path.join(base_dir, i, j))
                if sdata.empty:
                        continue

                sdata = sdata[sdata.mtype != 'Regularity']
                sdata['hts_file'] = j.split('.hts')[0]
                i.replace('\\', '-')
                sdata['folder'] = i
                ht_sites_lst.append(sdata)

    ## Processing
    print('processing')
    ht_sites = concat(ht_sites_lst).copy()
    ht_sites = ht_sites.drop_duplicates().drop('divisor', axis=1)
    ht_sites['wap'] = convert_site_names(ht_sites.site)
    ht_sites['date'] = Timestamp(yest.date())
    ht_sites['start_date'] = to_datetime(ht_sites['start_date'], errors='coerce')
    ht_sites['end_date'] = to_datetime(ht_sites['end_date'], errors='coerce')
    days_since = (Timestamp(today.date()) - ht_sites['end_date']).dt.days
    data_yest = ht_sites['end_date'] >= Timestamp(yest.date())
    data_yest2 = ht_sites['end_date'] >= Timestamp(yest.date() - timedelta(1))
    ht_sites['days_since_last_data'] = days_since
    ht_sites['data_yesterday'] = data_yest
    ht_sites['data_2_days_ago'] = data_yest2
    ht_sites['in_accela'] = ht_sites['wap'].isin(allo_wap.Site)

    ## Summary table
    ht_sites2 = ht_sites.sort_values('end_date', ascending=False).drop_duplicates('site').copy()
    n_uniq_sites = len(ht_sites2['site'].unique())
    n_uniq_waps = len(ht_sites2['wap'].unique())
    n_yest = len(ht_sites2.loc[ht_sites2['data_yesterday'], 'site'].unique())
    n_yest2 = len(ht_sites2.loc[ht_sites2['data_2_days_ago'], 'site'].unique())
    n_yest_wap = len(ht_sites2.loc[ht_sites2['data_yesterday'], 'wap'].unique())
    n_yest2_wap = len(ht_sites2.loc[ht_sites2['data_2_days_ago'], 'wap'].unique())
    n_accela = len(ht_sites2.loc[~ht_sites2['in_accela'] & ht_sites2['wap'].notnull(), 'wap'].unique())
    n_no_wap = len(ht_sites2.loc[ht_sites2['wap'].isnull(), 'site'].unique())
    n_tel = len(ht_sites2.loc[ht_sites2['folder'] == 'Telemetry', 'site'].unique())

    summ_list = [str(yest.date()), run_time_start, n_uniq_sites, n_uniq_waps, n_tel, n_accela, n_no_wap, n_yest, n_yest2, n_yest_wap, n_yest2_wap]
    summ_df = DataFrame([summ_list], columns=summ_col)

    ## Tel summary table
    tel_data = ht_sites2[ht_sites2.folder == 'Telemetry'].copy()
    hts_grp = tel_data.groupby('hts_file')
    hts_yest = hts_grp['data_yesterday'].sum().astype(int)
    hts_yest2 = hts_grp['data_2_days_ago'].sum().astype(int)

    yest1 = tel_data[tel_data['data_yesterday']]

    wo1 = tel_data[tel_data.hts_file == 'WaterOutlook']

    tel_data[tel_data['days_since_last_data'] <= 30].groupby('hts_file')['site'].count()

    ### Save to SQL
    ## create tables if needed
    summ_bool1 = rd_sql(server, database, stmt="select OBJECT_ID('" + summ_table_name + "', 'U')").loc[0][0] is None
    data_bool1 = rd_sql(server, database, stmt="select OBJECT_ID('" + data_table_name + "', 'U')").loc[0][0] is None

    if summ_bool1:
        create_table(server, database, summ_table_name, dtype_dict=summ_dtype, primary_keys=summ_pkeys)

    if data_bool1:
        create_table(server, database, data_table_name, dtype_dict=data_dtype, primary_keys=data_pkeys)

    ## Save data
    to_mssql(summ_df, server, database, summ_table_name)
    to_mssql(ht_sites, server, database, data_table_name)

    ## log
    run_time_end = datetime.today().strftime(time_format)
    log1 = DataFrame([[run_time_start, summ_table_name, 'pass', 'all good', str(yest.date()), run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log1, log_server, log_database, log_table)
    log2 = DataFrame([[run_time_start, data_table_name, 'pass', 'all good', str(yest.date()), run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log2, log_server, log_database, log_table)
    print('complete')

except Exception as err:
    err1 = err
    print(err1)
    run_time_start = today.strftime(time_format)
    run_time_end = datetime.today().strftime(time_format)
    log3 = DataFrame([[run_time_start, summ_table_name, 'fail', str(err1), str(today), run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log3, log_server, log_database, log_table)


