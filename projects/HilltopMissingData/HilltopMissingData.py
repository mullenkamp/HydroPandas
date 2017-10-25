# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""

from core.ecan_io.hilltop import rd_hilltop_data, rd_hilltop_sites, proc_ht_use_data, convert_site_names, parse_dsn
from core.ecan_io import write_sql
from os.path import split
from pandas import concat, Series, read_csv, read_hdf, DataFrame, merge, Grouper, to_datetime, to_numeric
from ConfigParser import ConfigParser
from os import path, getcwd

#######################################################
#### Parameters

ini_file = 'HilltopMissingData.ini'

sites_dtype = {'site': 'VARCHAR(64)', 'mtype': 'VARCHAR(19)', 'unit': 'VARCHAR(19)', 'hts_file': 'VARCHAR(31)', 'folder': 'VARCHAR(44)', 'start_date': 'DATETIME', 'end_date': 'DATETIME'}

#### Load in ini parameters
py_dir = path.realpath(path.join(getcwd(), path.dirname(__file__)))

ini1 = ConfigParser()
ini1.read(path.join(py_dir, ini_file))

dsn_path = ini1.get('Input', 'dsn_file')
server = ini1.get('SQLOutput', 'server')
database = ini1.get('SQLOutput', 'database')
sites_table = ini1.get('SQLOutput', 'sites_table')

########################################################
### Run through all hts files

hts_files = parse_dsn(dsn_path)

ht_sites_lst = []
ht_data = DataFrame()
for j in hts_files:
    print(j)
    ## Sites
    sdata = rd_hilltop_sites(j)
    if sdata.empty:
        continue
    base_path, filename = split(j)
    sdata['hts_file'] = filename
    sdata['folder'] = base_path
    ht_sites_lst.append(sdata)

ht_sites = concat(ht_sites_lst).apply(lambda x: x.str.encode('utf-8'), axis=1)

#ht_sites.apply(lambda x: x.str.len(), axis=1).max()
ht_sites.loc[:, 'site'] = ht_sites.loc[:, 'site'].apply(lambda x: x.split('-M')[0])
ht_sites.loc[:, 'start_date'] = to_datetime(ht_sites.loc[:, 'start_date'])
ht_sites.loc[:, 'end_date'] = to_datetime(ht_sites.loc[:, 'end_date'])
ht_sites.loc[:, 'folder'] = ht_sites.loc[:, 'folder'].str.replace('\\', '-').str.replace('--', '')

write_sql(server, database, sites_table, ht_sites, sites_dtype, drop_table=True)















