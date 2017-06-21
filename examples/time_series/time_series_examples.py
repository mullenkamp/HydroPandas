# -*- coding: utf-8 -*-
"""
Created on Fri May 13 10:34:17 2016

@author: MichaelEK
"""

import pandas as pd


    


#### Write time series data to csv

import import_time_series_v02 as imp

path1 ='C:/ecan/base_data/flow/RAW/ltr/ltr_DAILY_FLOW.CSV'

ts1 = imp.read_hydstra_csv(path1)

path2 = 'C:/ecan/base_data/flow/RAW/ltr'
save_path = 'C:/ecan/base_data/flow'

ts1 = imp.rw_hydrstra_dir(path2, save_path, 'ltr_flow_daily.csv', True, 25)

ts1.to_csv('C:/ecan/base_data/flow/ltr_flow_daily.csv')

# WUS import and export

csv_path = 'C:/ecan/base_data/usage/pareora_GW_usage.csv'




#### Select rows and columns of a dataframe based on index numbers
tseries2 = ts1.ix[:, [4, 5]]

ts2 = ts1.iloc[3000:3100, [0, 1]]

ts2 = ts1.iloc[:,[0,1,2,3,5]]

#### Create a new dataframe by removing specific indices
ts = tseries2.drop(tseries2.index[[3,6,7,8]])[:10]


tseries2.resample('M').mean()


'C:/ecan/base_data/flow/ltr_flow_daily.tsv'

path ='C:/ecan/base_data/flow/RAW/ltr/ltr_DAILY_FLOW.CSV'

# empty dataframe

temp1 = pd.DataFrame(index=[1,2], columns=[1,2,3,4])

# merge time series dataframes

temp2 = pd.concat([ts1, ts2], axis=1)

#########################################
# linear regression

from scipy import stats
import numpy as np

ts2 = ts1.iloc[:,[0,1,2,3,5]].resample('3M').mean()

ts3 = ts2.dropna()

slope, inter, r_val, p_val, RMSE = stats.linregress(ts3.iloc[:, 0], ts3.iloc[:, 1])

ts_temp = pd.Panel

index1 = range(2)

temp1 = [pd.DataFrame(index=[1, 2], columns=[1, 2, 3, 4]) for i in index1]


#########################################
# Timing functions

import timeit
import ts_stats_v01 as tst 

start = timeit.default_timer()

#malf4 = tst.malf7d(ts1, export=True)
stats1 = flow_stats(ts1, export=True)

stop = timeit.default_timer()

print stop - start 

