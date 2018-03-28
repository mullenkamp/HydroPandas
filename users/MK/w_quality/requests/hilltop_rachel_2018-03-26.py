# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 08:13:24 2018

@author: michaelek
"""
from time import sleep, time
from os import path, getcwd
from numpy import ceil, array_split
from pandas import concat, DataFrame, merge, to_datetime
from configparser import ConfigParser
from datetime import datetime
from sqlalchemy.types import String
from hilltoppy import rd_ht_wq_data, rd_hilltop_sites

#####################################################
### Parameters

hts_file = r'\\hilltop01\Hilltop\Data\WQRec_2017_2018.hts'

sites = ['SQ35819', 'SQ32516', 'SQ26747', 'SQ26749', 'SQ26355', 'SQ20104']

sample_params_list = ['Project', 'Cost Code', 'Technician', 'Sample ID', 'Sample Comment', 'Field Comment', 'Sample Appearance', 'Sample Colour', 'Sample Odour', 'Water Colour', 'Water Clarity']
mtype_params_list = ['Lab Method', 'Lab Name']

out_path = r'D:\users\mikek\Projects\requests\rachel\2018-03-26'
out_data = 'wq_rec_output.csv'

#####################################################
### Extract data


wqdata = rd_ht_wq_data(hts_file, sites=sites, sample_params=sample_params_list, mtype_params=mtype_params_list)

wqdata.to_csv(path.join(out_path, out_data), index=False)








































