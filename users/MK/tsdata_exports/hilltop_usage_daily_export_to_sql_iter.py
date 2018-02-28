# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""
import os
import pandas as pd
import numpy as np
from time import sleep
#from pymssql import connect
from datetime import datetime, timedelta
#from sqlalchemy import create_engine
from re import search, IGNORECASE, findall
from hydropandas.io.tools.mssql import rd_sql, to_mssql, del_mssql_table_rows
from hydropandas.io.tools.hilltop import rd_ht_quan_data, convert_site_names, proc_ht_use_data
from hydropandas.tools.general.ts.resampling import grp_ts_agg
from hydropandas.io.tools.sql_arg_class import sql_arg

######################################################
### additional functions


def rd_dir(data_dir, ext, file_num_names=False, ignore_case=True):
    """
    Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
    """

    if ignore_case:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename, IGNORECASE)])
    else:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename)])

    if file_num_names:
        site_names = np.array([int(findall("\d+", fi)[0]) for fi in files])
        return files, site_names
    else:
        return files


def wap_sw_gw(server='SQL2012PROD03', database='DataWarehouse', table='D_ACC_Act_Water_TakeWaterWAPAlloc', wap_col='WAP', take_type_col='Activity'):
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
    names2[~names_len_bool] = np.nan
    allo_wap['wap'] = names2
    allo_wap2 = allo_wap.dropna().drop_duplicates()
    allo_wap3 = allo_wap2.sort_values('take_type').drop_duplicates(subset='wap', keep='last').copy()
    return allo_wap3


#######################################################
### Parameters
## Import

fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive', 'ending_2017': r'\\hilltop01\Hilltop\Data\Annual\ending_2017'}
exclude_hts = ['Anonymous_AccToVol.hts', 'Anonymous_FlowToVolume.hts', 'RenameSites.hts', 'Telemetry.hts', 'Boraman2015-16.hts', 'FromWUS.hts', 'WUS_Consent2015.hts', 'AIC.hts', 'Boraman2015-16BU20170919.hts', 'Boraman2016-17BU20170919.hts', 'FromWUSBU20170919.hts', 'WUS_Consent2015.hts']

sql_wap_use = {'server': 'SQL2012DEV01', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterWAPAlloc'}

take_type_dict = {'Take Groundwater': 12, 'Take Surface Water': 9, 'Divert Surface Water': 9}

last_date_stmt = "select max(Time) from ExtractionLog where HydroTable='HilltopTSUsageDaily' and RunResult='pass'"

time_format = '%Y-%m-%d %H:%M:%S'

## Export

sql_export = {'table': 'HilltopTSUsageDaily', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

today = datetime.today()

try:

    ########################################################
    #### Get last date of extraction

    last_date1 = rd_sql(stmt=last_date_stmt, **sql_export).loc[0][0].date()
    today1 = today.date()

    if last_date1 < today1:

        ####################################################
        print('Extracting data from Hilltop')

        ht_sites_lst = []
        ht_data = pd.DataFrame()
        for i in fpath:
            hts_files = rd_dir(fpath[i], '.hts')
            for j in hts_files:
                if j not in exclude_hts:
                    print(os.path.join(fpath[i], j))
                    ### Data
                    timer = 5
                    while timer > 0:
                        try:
                            ht_data1, sdata = rd_ht_quan_data(os.path.join(fpath[i], j), start=str(last_date1), end=str(today1), agg_period='day', output_site_data=True, exclude_mtype=['Regularity'])
                            break
                        except Exception as err:
                            err1 = err
                            timer = timer - 1
                            if timer == 0:
                                raise ValueError(err1)
                            else:
                                print(err1)
                                sleep(3)
                    if sdata.empty:
                        continue
                    ## Sites
                    sdata['hts_file'] = j
                    sdata['folder'] = os.path.split(fpath[i])[1]
                    ht_sites_lst.append(sdata)
                    if ht_data.empty:
                        ht_data = ht_data1.copy()
                    else:
                        ht_data = ht_data.combine_first(ht_data1)

        print('Processing data')
        ht_sites = pd.concat(ht_sites_lst)
        ht_sites = ht_sites.drop_duplicates().drop('divisor', axis=1)

        if not ht_sites.empty:

            ht_site_names = convert_site_names(ht_sites.site)

            ht2 = proc_ht_use_data(ht_data, n_std=20)
            #ht2.to_csv(os.path.join(output_base, out_data_w_m), header=True, encoding='utf8')

            ht3 = ht2.reset_index()
            site_names = ht3.site

            site_names1 = convert_site_names(site_names)
            ht3.loc[:, 'site'] = site_names1
            ht4 = ht3[ht3.site.notnull()].copy()
            ht4.rename(columns={'site': 'Site', 'time': 'Time', 'data': 'Value'}, inplace=True)

            #ht5 = ht4.groupby(['Site', 'Time']).Value.sum().round(2)
            ht5 = grp_ts_agg(ht4, 'Site', 'Time', 'D').sum().round(2)

            ###################################################
            #### Load in the allocation info

            allo_wap = wap_sw_gw()
            allo_wap.columns = ['take_type', 'Site']

            ht6 = ht5.reset_index()

            ht7 = pd.merge(allo_wap, ht6, on='Site', how='right')

            w1 = ht7[ht7.take_type.isnull()].Site.unique()

            if len(w1) > 0:
                sql1 = sql_arg()

                wells1 = rd_sql(where_col={'WELL_NO': w1.tolist()}, **sql1.get_dict('well_details'))
                wells1.rename(columns={'wap': 'Site'}, inplace=True)

                w2 = pd.merge(pd.DataFrame(w1, columns=['Site']), wells1, on='Site')
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

            #############################################
            ### Reformat and export
            print('Saving data')

            min_time = str(ht8.Time.min().date())
            max_time = str(ht8.Time.max().date())

            del_mssql_table_rows(from_date=min_time, to_date=max_time, date_col='Time', **sql_export)

            to_mssql(ht8, **sql_export)

        else:
            raise ValueError('No new data in any of the hts files.')

    else:
        min_time = today1 - timedelta(days=1)

    ## log
    run_time_start = today.strftime(time_format)
    run_time_end = datetime.today().strftime(time_format)
    log1 = pd.DataFrame([[run_time_start, sql_export['table'], 'pass', str(len(ht_sites)) + ' sites with new data have been added', min_time, run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log1, **sql_log)
    print('complete')

except Exception as err:
    err1 = err
    print(err1)
    run_time_start = today.strftime(time_format)
    run_time_end = datetime.today().strftime(time_format)
    log2 = pd.DataFrame([[run_time_start, sql_export['table'], 'fail', str(err1), str(today), run_time_end]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime', 'RunTimeEnd'])
    to_mssql(log2, **sql_log)


