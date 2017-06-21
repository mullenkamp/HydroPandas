"""
Author: matth
Date Created: 16/03/2017 11:29 AM
"""

from __future__ import division
import pandas as pd
import numpy as np
import glob
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from fill_data_by_correlation import fill_df_by_similar_well

basedir = "P:/Groundwater/Trend_analysis/Data/organized data"
dir_list = ['groundwater', 'surfacewater']
years = [1975, 1985, 1995, 2005]
values_file = open(basedir + "/groundwater/filled_data_by_time_period/overview_filling.txt", mode='a')
num_non_null = 6 # number in a row to get a start date
min_cor = 0.75 # minimum corellation coeffient.
max_gap = 12
values_file.write("num of Start_Date years off: 0"
                  "corr_coef min: .75"
                  "max gap:12\n")

for dir_ in dir_list:
    outdir = '{od}/{d}/filled_data_by_time_period/'.format(od=basedir, d=dir_)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for year in years:
        # load data
        monthdata = pd.read_csv('{od}/{d}/original_by_time_period/month_{d}_data_{y}.csv'.format(od=basedir, d=dir_,
                                                                                                 y=year), index_col=0)
        monthdata.index = pd.to_datetime(monthdata.index)
        if monthdata.empty:
            print('{},{} is empty'.format(dir_, year))
            continue

        # fill 1 gaps by linear interpolation
        if dir_ == 'groundwater':
            limits = 1
        elif dir_ =='surfacewater': # for the non base flow data the limits were also 1 sorry future matt for the hackishness
            limits = 5


        filled_data_back = monthdata.fillna(method='bfill', limit=limits)
        filled_data_forward = monthdata.fillna(method='ffill', limit=limits)
        if dir_ == 'surfacewater':
            filled_data_forward.iloc[0:3,:] = filled_data_back.iloc[0:3,:] # to ensure we get tfull records
        idx = pd.isnull(monthdata)
        monthdata[idx] = (filled_data_back[idx] + filled_data_forward[idx]) / 2

        if dir_ == 'surfacewater':
            # drop all columns that still have month data
            monthdata = monthdata.dropna(how='any', axis=1)

            # write data
            monthdata.to_csv('{od}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(od=basedir,
                                                                                                        d=dir_, y=year))
            continue

        # interpolate by linear regression to high correlation wells
        outdata, lr_dict, outwells, stats_output_overview = fill_df_by_similar_well(monthdata,max_gap, num_non_null,min_corr_c=min_cor,
                                                                                    return_stats=True)

        # drop any columns that could not be filled
        outdata = outdata.dropna(how='any',axis=1)

        values_file.write('year:{} num_wells:{}\n'.format(year, len(outdata.keys())))

        # write data
        outdata.to_csv('{od}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(od=basedir, d=dir_,
                                                                                                  y=year))
        stats_output_overview.to_csv('{od}/{d}/filled_data_by_time_period/filling stats_{y}.csv'.format(od=basedir,
                                                                                                        d=dir_, y=year))


# create annual filled data from the monthly filled data

for dir_ in dir_list:
    for year in years:
        if dir_ == 'surfacewater' and year == 1975:
            continue

        data = pd.read_csv('{od}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(od=basedir, d=dir_,
                                                                                                      y=year))
        outdata = pd.melt(data,
                          id_vars=['DMY'],
                          value_vars=list(data.keys()).remove('DMY'),
                          var_name='Site',
                          value_name='value')

        outdata = outdata.dropna(how='any')
        outdata.DMY = pd.to_datetime(outdata.DMY)
        dates = pd.DatetimeIndex(outdata.DMY)
        outdata['year'] = dates.year

        g = outdata.groupby(['year','Site'])
        outdata = g.aggregate({'value': np.median}).reset_index()
        outdata = pd.pivot_table(outdata, values='value', index='year', columns='Site')
        outdata.to_csv('{od}/{d}/filled_data_by_time_period/filled_annual_{d}_data_{y}.csv'.format(od=basedir, d=dir_,
                                                                                                   y=year))

# Plot data for QA/QC on wells (not done on surface water as it is only simple filling)
print('plotting')
plot = False

if plot: # used to not plot as fouad want's me to quickly run through a bunch of senarios
    filled_data_list = glob.glob("P:/Groundwater/Trend_analysis/Data/organized data/groundwater/filled_data_by_time_period/filled_month_groundwater_data_*.csv")
    unfilled_data_list = glob.glob("P:/Groundwater/Trend_analysis/Data/organized data/groundwater/original_by_time_period/month_groundwater_data_*.csv")
    filled_data_list.sort()
    unfilled_data_list.sort()
    outdir = 'P:/Groundwater/Trend_analysis/Data/organized data/groundwater/filled_data_by_time_period/diagnostic_plots'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for filled, unfilled in zip(filled_data_list,unfilled_data_list):
        group = filled.split('_')[-1].strip('.csv')

        unfilled_data = pd.read_csv(unfilled, index_col='DMY')
        filled_data = pd.read_csv(filled, index_col='DMY')

        unfilled_data.index = pd.to_datetime(unfilled_data.index)

        well_list = filled_data.keys()
        x = pd.to_datetime(filled_data.index)
        for well in well_list:
            y_filled = filled_data[well][pd.notnull(filled_data[well])]
            y_unfilled = unfilled_data[well][pd.notnull(filled_data[well])]
            x = pd.to_datetime(y_filled.index)
            ax1 = plt.axes()
            plt.subplot(111)

            plt.plot(x, np.array(y_filled),'r',label='Filled Data')
            plt.plot(y_unfilled.index, np.array(y_unfilled),'b', label='Unfilled Data')
            plt.legend()
            plt.xlabel('Time')
            plt.ylabel('Water Level (m)')
            plt.xticks(pd.date_range(start=str(x.min().year//5*5),end=str((x.max().year//5+1)*5),freq='5AS'))
            plt.minorticks_on()
            ax1.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))
            ax1.xaxis.grid(True, which='minor', alpha=0.2)
            ax1.xaxis.grid(True, which='major', alpha=0.2)
            plt.savefig("{}/{}_{}.png".format(outdir, group, well.replace('/','_')),
                        orientation='landscape', papertype=None, dpi=600)

            plt.close()
