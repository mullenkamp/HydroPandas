"""
Author: matth
Date Created: 16/03/2017 11:30 AM
"""

from __future__ import division
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import os

base_dir = "P:/Groundwater/Trend_analysis/Data/organized data"

years = [1952, 1975, 1985, 1995, 2005]
model_types = ['additive', 'multiplicative']
for m_type in model_types:
    for dir_ in ['groundwater', 'surfacewater']:
        print('completeing {} models for {}'.format(m_type,dir_))
        for year in years:
            outdir = '{bd}/{d}/Seasonal_Decomposition/{d}_{y}_std'.format(bd=base_dir, d=dir_, y=year)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            poutdir = '{od}/plots_{m}'.format(od=outdir, m=m_type)
            if not os.path.exists(poutdir):
                os.makedirs(poutdir)

            indir = '{bd}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
            try:
                data = pd.read_csv(indir,index_col=0)
            except IOError:
                continue
            data.index = pd.to_datetime(data.index)

            well_list = list(data.keys())
            result_trend_list = []
            result_seasonal_list = []
            result_resid_list = []
            for well in well_list:
                temp_data = data[well]
                try:
                    res = sm.tsa.seasonal_decompose(temp_data, freq=12, model=m_type)
                except ValueError as val:
                    print('{}, {}: {}'.format(well, year, val))
                result_trend_list.append(res.trend)
                result_seasonal_list.append(res.seasonal)
                result_resid_list.append(res.resid)
                fig = res.plot()

                fig.savefig(poutdir + '/{}.png'.format(well.replace('/', '_')), dpi=150)
                plt.close(fig)

            result_trend = pd.concat(result_trend_list, axis=1)
            result_seasonal = pd.concat(result_seasonal_list, axis=1)
            result_resid = pd.concat(result_resid_list, axis=1)
            result_trend.to_csv(outdir + '/trend_{}.csv'.format(m_type))
            result_resid.to_csv(outdir + '/resid_{}.csv'.format(m_type))
            result_seasonal.to_csv(outdir + '/seasonal_{}.csv'.format(m_type))
