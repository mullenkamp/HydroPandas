# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from pandas import read_table, to_datetime, DataFrame, read_csv, concat
from scipy import interpolate
import matplotlib.pyplot as plt
from numpy import arange, meshgrid
from ts_stats_v01 import precip_agg, w_resample

######################################
### Parameters

hltr_precip_csv = 'C:/ecan/Projects/Pareora/analysis/precip/hltr_pareora_precip1.csv'
hltr_loc_csv = 'C:/ecan/Projects/Pareora/analysis/precip/input/hltr_sites.csv'
hltr_loc_col = ['Data VCN_s', 'Data VCN_1', 'Data VCN_2']
hltr_loc_col2 = ['site', 'x', 'y']

precip_csv = 'C:/ecan/Projects/Pareora/analysis/precip/pareora_precip1.csv'
site_loc_csv = 'C:/ecan/Projects/Pareora/analysis/precip/input/pareora_sites.csv'
site_loc_col = ['Data VCN_s', 'Data VCN_1', 'Data VCN_2']
site_loc_col2 = ['site', 'x', 'y']

save_path1 = 'C:/ecan/Projects/Pareora/analysis/precip/hltr_pareora_precip1.csv'


#####################################
### Read in data

## HLTR
hltr_data = read_csv(hltr_precip_csv, index_col=0, parse_dates=True, infer_datetime_format=True)

hltr_sites = read_csv(hltr_loc_csv, usecols=hltr_loc_col)
hltr_sites.columns = hltr_loc_col2
hltr_sites.index = hltr_sites.site.astype('int')
hltr_sites2 = hltr_sites[hltr_loc_col2[1:3]]

## pareora

data = read_csv(precip_csv, index_col=0, parse_dates=True, infer_datetime_format=True)

sites = read_csv(site_loc_csv, usecols=site_loc_col)
sites.columns = site_loc_col2
sites.index = sites.site.astype('int')
sites2 = sites[site_loc_col2[1:3]]

####################################
### Run the stats

## HLTR
hltr_agg1 = precip_agg(hltr_data)
hltr_yr1 = precip_resample(hltr_agg1, period='water year')

hltr_mean1 = hltr_yr1.mean()
hltr_cv1 = hltr_yr1.std()/hltr_mean1

## Pareora
agg1 = precip_agg(data)
yr1 = precip_resample(agg1, period='water year')

mean1 = yr1.mean()
cv1 = yr1.std()/mean1

## Merge the two
both1 = concat([hltr_yr1, yr1], axis=1)

both1.plot()






