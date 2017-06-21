"""
Author: matth
Date Created: 30/03/2017 4:23 PM
"""

from __future__ import division
from core import env
import pandas as pd
from scipy.stats import theilslopes
import os

base_dir = env.sci("Groundwater/Trend_analysis/Data/organized data")

years = [1952, 1975, 1985, 1995, 2005]
model_types = ['additive', 'multiplicative']
for dir_ in ['groundwater', 'surfacewater']:
    for year in years:
        outdir = '{bd}/{d}/Seasonal_Decomposition/{d}_{y}_std'.format(bd=base_dir, d=dir_, y=year)
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        try:
            data = pd.read_csv('{}/trend_additive.csv'.format(outdir),index_col=0)
        except IOError:
            continue
        data = data.dropna(how='any')
        outdata = pd.DataFrame(index=data.keys())
        for key in data.keys():
            t_slopes = theilslopes(data[key])
            outdata.loc[key,'median_slope'] = t_slopes[0]
            outdata.loc[key,'median_intercept'] = t_slopes[1]
            outdata.loc[key,'lower_slope'] = t_slopes[2]
            outdata.loc[key,'upper_slope'] = t_slopes[3]
        outdata.to_csv('{}/theil_slopes_{}.csv'.format(outdir,year))

