# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""
import os
import pandas as pd
from datetime import date
#from core.ecan_io.hilltop import rd_hilltop_data, rd_hilltop_sites, proc_ht_use_data, convert_site_names
from hydropandas.io.tools.hilltop import rd_hilltop_sites, rd_ht_quan_data, convert_site_names, proc_ht_use_data
from hydropandas.util.misc import rd_dir, save_df
from hydropandas.tools.general.ts.resampling import grp_ts_agg
from hydropandas.io.tools.mssql import rd_sql, del_mssql_table_rows, to_mssql
from core.allo_use.allo_use import wap_sw_gw
from hydropandas.io.tools.sql_arg_class import sql_arg

#######################################################
### Parameters
## Import
fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive', 'ending_2017': r'\\hilltop01\Hilltop\Data\Annual\ending_2017'}
exclude_hts = ['Anonymous_AccToVol.hts', 'Anonymous_FlowToVolume.hts', 'RenameSites.hts', 'Telemetry.hts', 'Boraman2015-16.hts', 'FromWUS.hts', 'WUS_Consent2015.hts', 'AIC.hts', 'Boraman2015-16BU20170919.hts', 'Boraman2016-17BU20170919.hts', 'FromWUSBU20170919.hts', 'WUS_Consent2015.hts']

sql_wap_use = {'server': 'SQL2012DEV01', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterWAPAlloc'}

take_type_dict = {'Take Groundwater': 12, 'Take Surface Water': 9, 'Divert Surface Water': 9}

#last_date_stmt = "SELECT name, object_id, create_date, modify_date FROM sys.tables where name='HilltopTSUsageDaily'"
last_date_stmt = "select max(Time) from ExtractionLog where HydroTable='HilltopTSUsageDaily' and RunResult='pass'"

## Export

sql_export = {'table': 'HilltopTSUsageDaily', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

try:

    ########################################################
    #### Get last date of extraction

    last_date1 = str(rd_sql(stmt=last_date_stmt, **sql_export).loc[0][0].date())
    today1 = str(date.today())

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
                ht_data1, sdata = rd_ht_quan_data(os.path.join(fpath[i], j), start=last_date1, end=today1, agg_period='day', output_site_data=True, exclude_mtype=['Regularity'])
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
    #ht_data = concat(ht_data_lst)

    #ht_data1 = ht_data.reset_index()

    #ht_sites.to_csv(os.path.join(output_base, sites_csv), index=False, encoding='utf8')

    #ht_data = rd_hilltop_data(hts1, sites=None, mtypes=None, start=None, end=None, agg_period='day', agg_n=1, fun=None)

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

    sql1 = sql_arg()

    wells1 = rd_sql(where_col={'WELL_NO': w1.tolist()}, **sql1.get_dict('well_details'))
    wells1.rename(columns={'wap': 'Site'}, inplace=True)

    w2 = pd.merge(pd.DataFrame(w1, columns=['Site']), wells1, on='Site')
    sw1 = w2[w2.well_type.isin(['SA', 'RI', 'SD', 'SX', 'SG'])].Site
    gw1 = w2[~w2.well_type.isin(['SA', 'RI', 'SD', 'SX', 'SG'])].Site

    ht7.loc[ht7.Site.isin(sw1), 'take_type'] = 'Take Surface Water'
    ht7.loc[ht7.Site.isin(gw1), 'take_type'] = 'Take Groundwater'
    ht7 = ht7[ht7.Time != today1]

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

    ## log
    log1 = pd.DataFrame([[today1, sql_export['table'], 'pass', 'all good', last_date1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
    to_mssql(log1, **sql_log)
    print('complete')

except Exception as err:
    err1 = err
    print(err1)
    log2 = pd.DataFrame([[today1, sql_export['table'], 'fail', str(err1), last_date1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
    to_mssql(log2, **sql_log)


