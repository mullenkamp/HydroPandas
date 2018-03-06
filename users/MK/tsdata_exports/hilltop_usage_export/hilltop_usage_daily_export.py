# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""
from numpy import array, nan, ceil, array_split
from os import path, getcwd, listdir
from pandas import concat, DataFrame, merge, Timestamp
from configparser import ConfigParser
from time import sleep
from datetime import datetime, timedelta
from re import search, IGNORECASE, findall
from hydropandas.io.tools.mssql import rd_sql, to_mssql, del_mssql_table_rows
from hydropandas.io.tools.hilltop import rd_ht_quan_data, convert_site_names, proc_ht_use_data, rd_hilltop_sites
from hydropandas.tools.general.ts.resampling import grp_ts_agg
from hydropandas.io.tools.sql_arg_class import sql_arg

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

#py_dir = r'E:\ecan\git\HydroPandas\users\MK\tsdata_exports'

ini1 = ConfigParser()
ini1.read([path.join(py_dir, path.splitext(__file__)[0] + '.ini')])

#ini1.read([path.join(py_dir, 'hilltop_wq_export_weekly' + '.ini')])

#hts_files = [r'\\hilltop01\Hilltop\Data\WQGroundwater.hts', r'\\hilltop01\Hilltop\Data\WQSurfacewater.hts']

base_dir = str(ini1.get('Input', 'base_dir'))
hts_dirs = [i.strip() for i in ini1.get('Input', 'hts_dirs').split(',')]

exclude_hts = [i.strip() for i in ini1.get('Input', 'exclude_hts').split(',')]

#fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive', 'ending_2017': r'\\hilltop01\Hilltop\Data\Annual\ending_2017'}
#exclude_hts = ['Anonymous_AccToVol.hts', 'Anonymous_FlowToVolume.hts', 'RenameSites.hts', 'Telemetry.hts', 'Boraman2015-16.hts', 'FromWUS.hts', 'WUS_Consent2015.hts', 'AIC.hts', 'Boraman2015-16BU20170919.hts', 'Boraman2016-17BU20170919.hts', 'FromWUSBU20170919.hts', 'WUS_Consent2015.hts']

#sql_wap_use = {'server': 'SQL2012DEV01', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterWAPAlloc'}

wap_server = str(ini1.get('Input', 'wap_server'))
wap_database = str(ini1.get('Input', 'wap_database'))
wap_table = str(ini1.get('Input', 'wap_table'))

all_or_last = str(ini1.get('Input', 'all_or_last'))
sites_chunk = int(ini1.get('Input', 'sites_chunk'))

take_type_dict = {'Take Groundwater': 12, 'Take Surface Water': 9, 'Divert Surface Water': 9}

last_date_stmt = "select max(Date) from Log where Application='HilltopTSUsageDaily' and Level='pass'"

time_format = '%Y-%m-%d %H:%M:%S'

## Export
server = str(ini1.get('Output', 'out_server'))
database = str(ini1.get('Output', 'out_database'))
usage_table = str(ini1.get('Output', 'usage_table'))

log_server = str(ini1.get('Output', 'log_server'))
log_database = str(ini1.get('Output', 'log_database'))
log_table = str(ini1.get('Output', 'log_table'))

#sql_export = {'table': 'HilltopTSUsageDaily', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
#sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

today = datetime.today()

try:

    ########################################################
    #### Get last date of extraction

    if all_or_last == 'last':
        last_date1 = rd_sql(log_server, log_database, stmt=last_date_stmt).loc[0][0].date()
    else:
        last_date1 = datetime(1900, 1, 1).date()
    today1 = today.date()

    if last_date1 < today1:

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
                    sdata['hts_file'] = j
                    sdata['folder'] = i
                    ht_sites_lst.append(sdata)

        ht_sites = concat(ht_sites_lst).copy()
        ht_sites = ht_sites.drop_duplicates().drop('divisor', axis=1)

        ht_sites['site_fix'] = convert_site_names(ht_sites.site)
        ht_sites = ht_sites[ht_sites['site_fix'].notnull()].sort_values(['site_fix', 'hts_file'], ascending=False)

        sites1 = ht_sites.site_fix.unique()
        n_chunks = ceil(len(sites1) / float(sites_chunk))
        sites2 = array_split(sites1, n_chunks)

        ht_sites[ht_sites.site_fix.isin(sites2[10])]



        ####################################################
        print('Extracting ts data from Hilltop')

        ht_sites_lst = []
        ht_data = DataFrame()
        for i in hts_dirs:
            hts_files = rd_dir(i, '.hts')
            for j in hts_files:
                if j not in exclude_hts:
                    print(path.join(i, j))
                    ## Sites
                    sdata = rd_hilltop_sites(path.join(i, j))
                    if sdata.empty:
                            continue

                    sites1 = sdata.site.unique()
                    n_chunks = ceil(len(sites1) / float(sites_chunk))
                    sites2 = array_split(sites1, n_chunks)

                    sdata['hts_file'] = j
                    sdata['folder'] = path.split(i)[1]
                    ht_sites_lst.append(sdata)

                    for s in sites2:
                        ### Data
                        timer = 5
                        while timer > 0:
                            try:
                                ht_data1 = rd_ht_quan_data(path.join(i, j), sites=s.tolist(), start=str(last_date1), end=str(today1), agg_period='day', output_site_data=False, exclude_mtype=['Regularity'])
                                break
                            except Exception as err:
                                err1 = err
                                timer = timer - 1
                                if timer == 0:
                                    raise ValueError(err1)
                                else:
                                    print(err1)
                                    sleep(3)
#                        if ht_data.empty:
#                            ht_data = ht_data1.copy()
#                        else:
#                            ht_data = ht_data.combine_first(ht_data1)

                        print('Processing data')
                #        ht_sites = concat(ht_sites_lst)
                        ht_sites = sdata.copy()
                        ht_sites = ht_sites.drop_duplicates().drop('divisor', axis=1)

                        if not ht_sites.empty:

                            ht_site_names = convert_site_names(ht_sites.site)

                            ht2 = proc_ht_use_data(ht_data1, n_std=20)
                            #ht2.to_csv(path.join(output_base, out_data_w_m), header=True, encoding='utf8')

                            ht3 = ht2.reset_index()
                            site_names = ht3.site

                            site_names1 = convert_site_names(site_names)
                            ht3.loc[:, 'site'] = site_names1
                            ht4 = ht3[ht3.site.notnull()].copy()
                            ht4.rename(columns={'site': 'Site', 'time': 'Time', 'data': 'Value'}, inplace=True)

                            #ht5 = ht4.groupby(['Site', 'Time']).Value.sum().round(2)
                            ht5 = grp_ts_agg(ht4, 'Site', 'Time', 'D').sum().round()

                            ###################################################
                            #### Combine with WAP site info
                            ht6 = ht5.reset_index()

                            ht7 = merge(allo_wap, ht6, on='Site', how='right')

                            w1 = ht7[ht7.take_type.isnull()].Site.unique()

                            if len(w1) > 0:
                                sql1 = sql_arg()

                                wells1 = rd_sql(where_col={'WELL_NO': w1.tolist()}, **sql1.get_dict('well_details'))
                                wells1.rename(columns={'wap': 'Site'}, inplace=True)

                                w2 = merge(DataFrame(w1, columns=['Site']), wells1, on='Site')
                                sw1 = w2[w2.well_type.isin(['SA', 'RI', 'SD', 'SX', 'SG'])].Site
                                gw1 = w2[~w2.well_type.isin(['SA', 'RI', 'SD', 'SX', 'SG'])].Site

                                ht7.loc[ht7.Site.isin(sw1), 'take_type'] = 'Take Surface Water'
                                ht7.loc[ht7.Site.isin(gw1), 'take_type'] = 'Take Groundwater'

                            ht7 = ht7[ht7.Time != str(today1)]

                            ###############################################
                            ### Convert take_type to Hydro code

                            ht8 = ht7.dropna()[['Site', 'take_type', 'Time', 'Value']].copy()
                            ht8.replace({'take_type': take_type_dict}, inplace=True)
                            ht8['QualityCode'] = 200
                            ht8.rename(columns={'take_type': 'FeatureMtypeSourceID'}, inplace=True)
                            ht8.Time = ht8.Time.astype(str)

                            ##############################################
                            ### Compare to existing data and select only data that has been changed
                            old_data = rd_sql(server, database, usage_table, where_col={'Site': ht8.Site.unique().tolist()}, from_date=str(ht8.Time.min().date()), to_date=str(ht8.Time.max().date()), date_col='Time')
                            old_data['Value'] = old_data['Value'].round()
                            old_data.rename(columns={'Value': 'old'}, inplace=True)

                            combo1 = merge(ht8, old_data, on=['Site', 'FeatureMtypeSourceID', 'Time'], how='outer', indicator=True)

                            #############################################
                            ### Reformat and export
                            print('Saving data')

                            min_time = str(ht8.Time.min().date())
                            max_time = str(ht8.Time.max().date())

                            del_mssql_table_rows(from_date=min_time, to_date=max_time, date_col='Time', **sql_export)

                            to_mssql(ht8)

                        else:
                            raise ValueError('No new data in any of the hts files.')

                    else:
                        min_time = today1 - timedelta(days=1)

    ## log
    run_time_start = today.strftime(time_format)
    run_time_end = datetime.today().strftime(time_format)
    log1 = DataFrame([[run_time_start, '', 'pass', str(len(ht_sites)) + ' sites with new data have been added', min_time, run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log1)
    print('complete')

except Exception as err:
    err1 = err
    print(err1)
    run_time_start = today.strftime(time_format)
    run_time_end = datetime.today().strftime(time_format)
    log2 = DataFrame([[run_time_start, '', 'fail', str(err1), str(today), run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log2)


