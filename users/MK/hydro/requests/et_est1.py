# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 15:03:58 2017

@author: michaelek
"""

from fao import sol_dec, sunset_hour_angle
import convert
from misc_fun import rd_dir
from pandas import read_csv, Series, DataFrame, concat, MultiIndex
from os import path
from import_fun import rd_ts
import numpy as np
import matplotlib.pyplot as plt

########################################
#### Parameters

in_path = r'C:\ecan\local\Projects\requests\ET_est\data'

lat = -43.680301 * (numpy.pi / 180.0)
phi = lat*np.pi/180

out_path = r'C:\ecan\local\Projects\requests\ET_est\data\results'
out_most = 'set1.csv'
out_one = 'set2.csv'


#########################################
#### Run through all files

files = Series(rd_dir(in_path, 'csv'))

names = files.str.split('.', expand=True)[0].values
mul_index = MultiIndex.from_product([names, ['ETo', 'precip']])

df_list = []
for i in files:
    ### Read data
    d1 = rd_ts(path.join(in_path, i), header=[0,1])
    site = i.split('.')[0]

    ### Estimate parameters
    day_yr = d1.index.dayofyear
    delta = 0.409*np.sin(2*np.pi*day_yr/365-1.39)
    d_r = 1+0.033*np.cos(2*np.pi*day_yr/365)
    w_s = np.arccos(-np.tan(phi)*np.tan(delta))
    tmean = d1['MeanTempHourly'].values.flatten() - 273.15
    tmin = d1['MinTempHourly'].values.flatten() - 273.15
    tmax = d1['MaxTempHourly'].values.flatten() - 273.15
    R_a = 24*60/np.pi*0.082*d_r*(w_s*np.sin(phi)*np.sin(delta)+np.cos(phi)*np.cos(delta)*np.sin(w_s))

    et0 = (0.0023 * (tmean + 17.8) * (tmax - tmin) ** 0.5 * 0.408 * R_a).round(2)
    et1 = Series(et0, index=d1.index, name=site[0])
    et_rain = concat([et1, d1['APrecip']], axis=1)
    et_rain.columns = ['ETo', 'precip']
    df_list.append(et_rain)

df = concat(df_list, axis=1)
df.columns = mul_index

df.to_csv(path.join(out_path, out_most))

#tCU = Series((tmax-tmin), index=d1.index, name=site[0])


files2 = Series(rd_dir(in_path + '\\other', 'csv'))
names = files2.str.split('.', expand=True)[0].values
mul_index = MultiIndex.from_product([names, ['ETo', 'precip']])

df_list = []
for i in files2:
    ### Read data
    d1 = rd_ts(path.join(in_path + '\\other', i), header=[0,1])
    site = i.split('.')[0].split('_')

    ### Estimate parameters
    day_yr = d1.index.dayofyear
    delta = 0.409*np.sin(2*np.pi*day_yr/365-1.39)
    d_r = 1+0.033*np.cos(2*np.pi*day_yr/365)
    w_s = np.arccos(-np.tan(phi)*np.tan(delta))
    tmean = d1['MeanTempDaily'].values.flatten() - 273.15
    if site[1] == '40':
        tdiff = t40
    elif site[1] == '90':
        tdiff = t90
    elif site[1] == 'CU':
        tdiff = tCU
    R_a = 24*60/np.pi*0.082*d_r*(w_s*np.sin(phi)*np.sin(delta)+np.cos(phi)*np.cos(delta)*np.sin(w_s))

    et0 = (0.0023 * (tmean + 17.8) * (tdiff) ** 0.5 * 0.408 * R_a).round(2)
    et1 = Series(et0, index=d1.index, name=site[0])
    et_rain = concat([et1, d1['APrecip']], axis=1)
    et_rain.columns = ['ETo', 'precip']
    df_list.append(et_rain)

df_other = concat(df_list, axis=1)
df_other.columns = mul_index


df_other.to_csv(path.join(out_path, out_one))


#df_all = concat([df, df_other], axis=0)





























































