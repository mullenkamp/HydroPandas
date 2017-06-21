"""
Author: matth
Date Created: 24/03/2017 8:37 AM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt



base_dir = env.sci("Groundwater/Trend_analysis/Data/organized data")
years = [1952, 1975, 1985, 1995, 2005]
for dir_ in ['groundwater', 'surfacewater']:
    for year in years:
        outdir = '{bd}/{d}/fourier'.format(bd=base_dir, d=dir_, y=year)
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        indir = '{bd}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        try:
            data = pd.read_csv(indir, index_col=0)
        except IOError:
            continue
        data.index = pd.to_datetime(data.index)

        well_list = list(data.keys())
        f_all = np.fft.fftfreq(data.index.size)
        f_all[1:] = 1 / f_all[1:]
        idx_12 = np.where(np.isclose(f_all, 12, atol=0.1))

        outdata = pd.DataFrame(index=f_all)
        outdata2 = pd.DataFrame(index=f_all)
        working_data = {}
        outdata.index.names = ['month']
        outdata2.index.names = ['month']
        outdata['year'] = np.array(outdata.index)/12
        outdata2['year'] = np.array(outdata.index)/12

        for key in well_list:
            temp_data = np.array(data[key])
            ft = np.fft.fft(temp_data)
            a = np.abs(ft) / len(ft)
            a[1:] = a[1:] * 2
            p = np.angle(ft)
            outdata['{}_a'.format(key)] = a
            outdata2['{}_a'.format(key)] = a
            working_data[key] = ft
            outdata['{}_p'.format(key)] = p

        outdata.to_csv('{}/{}_analysis.csv'.format(outdir, year))
        outdata2.to_csv('{}/{}_analysis_aonly.csv'.format(outdir, year))

        if not os.path.exists('{}/{}_functions'.format(outdir, year)):
            os.makedirs('{}/{}_functions'.format(outdir, year))

        n = data.index.size
        for key in data.keys():
            to_inverse_const = np.zeros((n), dtype=np.complex128)
            to_inverse_const[0] = working_data[key][0]
            zth_order = np.abs(np.fft.ifft(to_inverse_const))

            temp_data = np.array(outdata['{}_a'.format(key)])
            idx = np.where((temp_data >= temp_data[idx_12]) & (f_all > 0))
            f_vals = f_all[idx]
            ft_vals = np.array(working_data[key])[idx]
            cos_funct = pd.DataFrame(index=data.index)
            for i, f, ft in zip(idx[0],f_vals,ft_vals):
                to_inverse = np.zeros((n),dtype=np.complex128)
                to_inverse[0] = working_data[key][0]
                to_inverse[i] = ft

                cos_funct['f_{:03d}'.format(int(round(f)))] = 2*np.abs(np.fft.ifft(to_inverse)) - zth_order # times 2 to account for the symetry that we excluded
            cos_funct.to_csv('{}/{}_functions/{}.csv'.format(outdir, year,key.replace('/','_')))

        for key in data.keys():
            to_inverse_const = np.zeros((n), dtype=np.complex128)
            to_inverse_const[0] = working_data[key][0]
            zth_order = np.abs(np.fft.ifft(to_inverse_const))

            idx = np.where(f_all > 0)
            f_vals = f_all[idx]
            ft_vals = np.array(working_data[key])[idx]
            cos_funct = pd.DataFrame(index=data.index)
            for i, f, ft in zip(idx[0], f_vals, ft_vals):
                to_inverse = np.zeros((n), dtype=np.complex128)
                to_inverse[0] = working_data[key][0]
                to_inverse[i] = ft

                cos_funct['f_{}'.format(f)] = 2*np.abs(np.fft.ifft(to_inverse))- zth_order
            cos_funct.to_csv('{}/{}_functions/{}_all_functions.csv'.format(outdir, year,key.replace('/','_')))


















