# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 11:03:51 2017

@author: michaelek
"""

from core.misc import rd_dir
from os import path
from pandas import read_csv, Series, concat

#############################
### Parameters

base_dir = r'Z:\Data\DataLoggers\Compliance\SP reports 2015-16\Z Manual Uploads - Uploaded\Uploaded 21-12-2016'

flow_dir = '0 Flow L per S'
vol_dir = '0 Volume M3'
accu_dir = '0 Accumulated M3'

export_path_flow = r'C:\ecan\local\Projects\requests\Ilja\2017-02-17\n_negatives_flow.csv'
export_path_vol = r'C:\ecan\local\Projects\requests\Ilja\2017-02-17\n_negatives_vol.csv'
export_path_accu = r'C:\ecan\local\Projects\requests\Ilja\2017-02-17\n_negatives_accu.csv'

############################
### Read files

## Flow data
f_dir = path.join(base_dir, flow_dir)
f_files = rd_dir(f_dir, 'csv')

neg_lst = []
for i in f_files:
#    t1 = read_csv(path.join(base_dir, flow_dir, i), header=None, usecols=[0,1,2], infer_datetime_format=True, parse_dates=[[0,1]], dayfirst=True)
    t1 = read_csv(path.join(f_dir, i), header=None, usecols=[2], skiprows=1)[2]
    neg_lst.append((t1 < 0 ).sum())

neg_s = Series(neg_lst, index=f_files)
neg_s.name = 'n_negatives'
neg_s.index.name = 'flow_file'
neg_s.to_csv(export_path_flow, header=True)


## volume data
f_dir = path.join(base_dir, vol_dir)
f_files = rd_dir(f_dir, 'csv')

neg_lst = []
for i in f_files:
#    t1 = read_csv(path.join(base_dir, flow_dir, i), header=None, usecols=[0,1,2], infer_datetime_format=True, parse_dates=[[0,1]], dayfirst=True)
    t1 = read_csv(path.join(f_dir, i), header=None, usecols=[2], skiprows=1)[2]
    neg_lst.append((t1 < 0 ).sum())

neg_s = Series(neg_lst, index=f_files)
neg_s.name = 'n_negatives'
neg_s.index.name = 'vol_file'
neg_s.to_csv(export_path_vol, header=True)


## accu data
f_dir = path.join(base_dir, accu_dir)
f_files = rd_dir(f_dir, 'csv')

neg_lst = []
for i in f_files:
#    t1 = read_csv(path.join(base_dir, flow_dir, i), header=None, usecols=[0,1,2], infer_datetime_format=True, parse_dates=[[0,1]], dayfirst=True)
    t1 = read_csv(path.join(f_dir, i), header=None, usecols=[2], skiprows=1)[2]
    neg_lst.append((t1 < 0 ).sum())

neg_s = Series(neg_lst, index=f_files)
neg_s.name = 'n_negatives'
neg_s.index.name = 'accu_file'
neg_s.to_csv(export_path_accu, header=True)





































































