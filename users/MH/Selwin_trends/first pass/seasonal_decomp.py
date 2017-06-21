"""
Author: matth
Date Created: 9/03/2017 3:30 PM
"""

from __future__ import division
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

data = pd.read_csv("P:/Groundwater/Trend_analysis/Data/modified_data/Level/well_water_level_fully_filled.csv")
well_start_dates = pd.read_csv("P:/Groundwater/Trend_analysis/Data/modified_data/Level/well_start_dates.csv")
well_start_dates.start_date = pd.to_datetime(well_start_dates.start_date)
well_start_dates = well_start_dates.set_index('WELL_NO')
data['DMY'] = pd.to_datetime(data['DMY'])
data = data.set_index('DMY')
well_list = list(data.keys())
result_trend_list = []
result_seasonal_list = []
result_resid_list = []
p_outdir = "P:/Groundwater/Trend_analysis/Data/Figures/level_data_figures/seasonal_decomp/"
for well in well_list:
    temp_data = data[well]
    temp_data = temp_data[temp_data.index >= well_start_dates.loc[well,'start_date']]
    res = sm.tsa.seasonal_decompose(temp_data,freq=12)
    result_trend_list.append(res.trend)
    result_seasonal_list.append(res.seasonal)
    result_resid_list.append(res.resid)
    fig = res.plot()
    fig.savefig(p_outdir + '{}.png'.format(well.replace('/','_')))
    plt.close(fig)

r_outdir = "P:/Groundwater/Trend_analysis/Data/modified_data/Level/seasonal_decomp/"
result_trend = pd.concat(result_trend_list, axis=1)
result_seasonal = pd.concat(result_seasonal_list, axis=1)
result_resid = pd.concat(result_resid_list, axis=1)
result_trend.to_csv(r_outdir + 'trend.csv')
result_resid.to_csv(r_outdir + 'resid.csv')
result_seasonal.to_csv(r_outdir + 'seasonal.csv')


print('done')