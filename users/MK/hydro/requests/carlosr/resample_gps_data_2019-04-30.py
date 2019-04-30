# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 13:19:54 2019

@author: michaelek
"""
import os
import pandas as pd

pd.options.display.max_columns = 10

#######################################
### Parameters

base_path = r'E:\ecan\local\Projects\requests\carlosr\2019-04-30'

data_csv = 'Track_04apr2019.csv'
output_csv = 'resampled_track_2019-04-30.csv'

#######################################
### Resample data

## Read in data
data1 = pd.read_csv(os.path.join(base_path, data_csv), infer_datetime_format=True, index_col='DateTime', parse_dates=['DateTime'])

## Change timezone from UTC +0 to UTC +12
data1.index = data1.index - pd.DateOffset(hours=12)

## Resample
data2 = data1.resample('250L').first().interpolate()

## Save results
data2.to_csv(os.path.join(base_path, output_csv))
























