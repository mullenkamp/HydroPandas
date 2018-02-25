# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 08:51:46 2017

@author: MichaelEK
"""
from os import path, getcwd
import numpy as np
import pandas as pd
from configparser import ConfigParser
from datetime import datetime
from hydropandas.io.tools.hilltop import rd_ht_wq_data, rd_hilltop_sites
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table, del_mssql_table_rows, rd_sql

#############################################
### Parameters
## Import
print('load parameters')

py_dir = path.realpath(path.join(getcwd(), path.dirname(__file__)))

#py_dir = r'E:\ecan\git\HydroPandas\users\MK\tsdata_exports'

ini1 = ConfigParser()
ini1.read([path.join(py_dir, path.splitext(__file__)[0] + '.ini')])

#ini1.read([path.join(py_dir, 'hilltop_wq_export_weekly' + '.ini')])

#hts_files = [r'\\hilltop01\Hilltop\Data\WQGroundwater.hts', r'\\hilltop01\Hilltop\Data\WQSurfacewater.hts']

hts_files = [i.strip() for i in ini1.get('Input', 'hts_files').split(',')]

sample_params_list = ['Project', 'Cost Code', 'Technician', 'Sample ID', 'Sample Comment', 'Field Comment', 'Sample Appearance', 'Sample Colour', 'Sample Odour', 'Water Colour', 'Water Clarity']
mtype_params_list = ['Lab Method', 'Lab Name']

sample_params_list = [i.strip() for i in ini1.get('Input', 'sample_params_list').split(',')]
mtype_params_list = [i.strip() for i in ini1.get('Input', 'mtype_params_list').split(',')]

#sites_chunk = 100
sites_chunk = int(ini1.get('Input', 'sites_chunk'))

## Export

sites_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'Units': 'VARCHAR(29)', 'FromDate': 'DATETIME', 'ToDate': 'DATETIME'}

samples_dtype = {'SiteID': 'VARCHAR(19)', 'CollectionTime': 'DATETIME', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)'}

mtypes_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'CollectionTime': 'DATETIME', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)'}

col_name_convert = {'site': 'SiteID', 'mtype': 'MeasurementType', 'unit': 'Units', 'start_date': 'FromDate', 'end_date': 'ToDate', 'time': 'CollectionTime'}

server = str(ini1.get('Output', 'server'))
database = str(ini1.get('Output', 'database'))
sites_table = str(ini1.get('Output', 'sites_table'))
mtypes_table = str(ini1.get('Output', 'mtypes_table'))
samples_table = str(ini1.get('Output', 'samples_table'))

sites_pkeys = ['SiteID', 'MeasurementType']
samples_pkeys = ['SiteID', 'Param', 'CollectionTime']
mtypes_pkeys = ['SiteID', 'MeasurementType', 'Param', 'CollectionTime']

#sql_log = {'server': 'SQL2014DEV01', 'database': 'Apps_Trace_Log', 'table': 'Log'}
log_server = str(ini1.get('Output', 'log_server'))
log_database = str(ini1.get('Output', 'log_database'))
log_table = str(ini1.get('Output', 'log_table'))

### Start processing

try:

    ##############################################
    ### Determine if tables exist
    print('Determine if tables exist')

    today1 = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    sites_bool1 = rd_sql(server, database, stmt="select OBJECT_ID('" + sites_table + "', 'U')").loc[0][0] is None
    samples_bool1 = rd_sql(server, database, stmt="select OBJECT_ID('" + samples_table + "', 'U')").loc[0][0] is None
    mtypes_bool1 = rd_sql(server, database, stmt="select OBJECT_ID('" + mtypes_table + "', 'U')").loc[0][0] is None

    if samples_bool1:
        create_mssql_table(server, database, samples_table, dtype_dict=samples_dtype, primary_keys=samples_pkeys)
    else:
        del_mssql_table_rows(server, database, samples_table)

    if mtypes_bool1:
        create_mssql_table(server, database, mtypes_table, dtype_dict=mtypes_dtype, primary_keys=mtypes_pkeys)
    else:
        del_mssql_table_rows(server, database, mtypes_table)

    if sites_bool1:
        create_mssql_table(server, database, sites_table, dtype_dict=sites_dtype, primary_keys=sites_pkeys)
    else:
        del_mssql_table_rows(server, database, sites_table)

    ###############################################
    ### Extract data

    mtype_list = list(mtype_params_list)
    mtype_list.append('data')

    ## Extract site data
    print('Extract site data')
    sites_dict = {}
    for hts in hts_files:
        sites_info1 = rd_hilltop_sites(hts)
        sites_info2 = sites_info1.drop(['data_source', 'divisor'], axis=1)
        sites_info2.rename(columns=col_name_convert, inplace=True)
        sites_info2['SiteID'] = sites_info2['SiteID'].str.upper()
        sites_dict.update({hts: sites_info2})

    sites_all = pd.concat(list(sites_dict.values()))

    # Check for duplicate sites/mytpes
    dups = sites_all[sites_all.duplicated(sites_pkeys)]

    if not dups.empty:
        raise ValueError('There are duplicate sites and measurement types')

    ## Upload sites table to MSSQL
    print('Upload sites table to MSSQL')
    to_mssql(sites_all, server, database, sites_table)

    ## Extract ts data
    print('Extract ts data')
    for hts in hts_files:
        print(hts)
        hts_sites = sites_dict[hts].copy()

        sites1 = hts_sites.SiteID.unique()
        n_chunks = np.ceil(len(sites1) / float(sites_chunk))
        sites2 = np.array_split(sites1, n_chunks)

        ## Chunk out the data and into MSSQL

        for hts_sites in sites2:

            ## Pull out data and reorganize
            wq_data = rd_ht_wq_data(hts, sites=hts_sites.tolist(), output_site_data=False, sample_params=sample_params_list, mtype_params=mtype_params_list)

            wq_data1 = wq_data.melt(['site', 'mtype', 'time'], var_name='Param', value_name='Value')

            wq_data_mtype = wq_data1[wq_data1['Param'].isin(mtype_list)].copy()

            wq_data_sample = wq_data1[wq_data1['Param'].isin(sample_params_list)]
            wq_data_sample = wq_data_sample.drop('mtype', axis=1).drop_duplicates(['site', 'Param', 'time'])
            wq_data_sample['Value'] = wq_data_sample['Value'].str.strip()
            wq_data_sample = wq_data_sample[~((wq_data_sample['Value'] == '') | (wq_data_sample['Value'] == '..'))]

            # Rename columns
            wq_data_sample.rename(columns=col_name_convert, inplace=True)
            wq_data_mtype.rename(columns=col_name_convert, inplace=True)

            ## SQL upload
            to_mssql(wq_data_sample, server, database, samples_table)
            to_mssql(wq_data_mtype, server, database, mtypes_table)

    ## log
    log1 = pd.DataFrame([[today1, server, 'Python WQ upload', '1', 'INFO', 'python', 'completed successfully']], columns=['Date', 'Server', 'Application', 'Thread', 'Level', 'Logger', 'Message'])
    to_mssql(log1, log_server, log_database, log_table)
    print('complete')

except Exception as err:
    err1 = err
    print(err1)
    log2 = pd.DataFrame([[today1, server, 'Python WQ upload', '1', 'INFO', 'python', str(err1)]], columns=['Date', 'Server', 'Application', 'Thread', 'Level', 'Logger', 'Message'])
    to_mssql(log2, log_server, log_database, log_table)
    print('fail')


###########################################
### Testing

#sites_all[(sites_all.SiteID.str.contains('SQ20104', case=False)) & (sites_all.MeasurementType.str.contains('cond', case=False))].sort_values('MeasurementType')
#





















