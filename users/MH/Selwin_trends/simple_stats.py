"""
Author: matth
Date Created: 7/04/2017 11:05 AM
"""

from __future__ import division
from core import env
import numpy as np
import os
import pandas as pd
from core.ecan_io import rd_sql, sql_db
from copy import deepcopy

base_dir = env.sci("Groundwater/Trend_analysis/Data/organized data")
well_details = rd_sql(**sql_db.wells_db.well_details)
well_details = well_details.set_index('WELL_NO')

years = [1952, 1975, 1985, 1995, 2005]
for dir_ in ['groundwater', 'surfacewater']:
    outdir = '{bd}/{d}/simple_stats'.format(bd=base_dir, d=dir_)
    # set up dirs and load data
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for year in years:

        # calculate monthly statistics
        indir = '{bd}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        try:
            data = pd.read_csv(indir, index_col=0)
        except IOError:
            continue
        data.index = pd.to_datetime(data.index)

        data2 = deepcopy(data)
        data_org = deepcopy(data)
        # calculate yearly groundwater index
        for key in data2.keys():
            hmax = data2[key].max()
            hmin = data2[key].min()
            data2[key] = (data2[key] - hmin) / (hmax - hmin) * 100

        data2.to_csv('{}/filled_gw_yearly_index_{}.csv'.format(outdir, year))

        data2['year'] = data2.index.year
        sites = list(data2.keys())
        sites.remove('year')
        gwyi = pd.melt(data2,
                            id_vars=['year'],
                                value_vars=sites,
                                var_name='site',
                                value_name='level')

        g = gwyi.groupby(['year','site'])
        gw_median = g.aggregate({'level': np.median}).reset_index()
        gw_min = g.aggregate({'level': np.min}).reset_index()
        gw_max = g.aggregate({'level': np.max}).reset_index()

        gw_median = pd.pivot_table(gw_median, values='level', index='year', columns='site')
        gw_min = pd.pivot_table(gw_min, values='level', index='year', columns='site')
        gw_max = pd.pivot_table(gw_max, values='level', index='year', columns='site')

        gw_median.to_csv('{}/annual_yearly_index_median_{}.csv'.format(outdir,year))
        gw_min.to_csv('{}/annual_yearly_index_min_{}.csv'.format(outdir,year))
        gw_max.to_csv('{}/annual_yearly_index_max_{}.csv'.format(outdir,year))





        plotdata2 = pd.DataFrame(index=range(1, 13))
        for i in plotdata2.index:
            tempdata = data[data.index.month == i]
            tempdata = tempdata.stack()
            plotdata2.loc[i, 'min'] = tempdata.min()
            plotdata2.loc[i, 'max'] = tempdata.max()
            plotdata2.loc[i, 'mean'] = tempdata.mean()
            plotdata2.loc[i, 'sd'] = tempdata.std()
            plotdata2.loc[i, '75th'] = tempdata.quantile(0.75)
            plotdata2.loc[i, 'median'] = tempdata.median()
            plotdata2.loc[i, '25th'] = tempdata.quantile(0.25)

        plotdata2.to_csv('{}/monthly_stats_{}.csv'.format(outdir, year))

        # calculate monthly groundwater index
        for key in data.keys():
            for i in range(0, 13):
                idx = data.index.month == i
                hmax = data[key][idx].max()
                hmin = data[key][idx].min()
                data[key][idx] = (data[key][idx] - hmin) / (hmax - hmin) * 100

        data.to_csv('{}/monthly_gw_index_{}.csv'.format(outdir, year))

        data['year'] = data.index.year
        # groupby statisitics for the monthly index median, min, and max by year
        sites = list(data.keys())
        sites.remove('year')
        gwindex = pd.melt(data,
                            id_vars=['year'],
                                value_vars=sites,
                                var_name='site',
                                value_name='level')

        g = gwindex.groupby(['year','site'])
        gw_median = g.aggregate({'level': np.median}).reset_index()
        gw_min = g.aggregate({'level': np.min}).reset_index()
        gw_max = g.aggregate({'level': np.max}).reset_index()

        gw_median = pd.pivot_table(gw_median, values='level', index='year', columns='site')
        gw_min = pd.pivot_table(gw_min, values='level', index='year', columns='site')
        gw_max = pd.pivot_table(gw_max, values='level', index='year', columns='site')

        gw_median.to_csv('{}/annual_gwindex_median_{}.csv'.format(outdir,year))
        gw_min.to_csv('{}/annual_gwindex_min_{}.csv'.format(outdir,year))
        gw_max.to_csv('{}/annual_gwindex_max_{}.csv'.format(outdir,year))

        #max min median gwlevel for each well for each year
        data_org['year'] = data_org.index.year
        sites = list(data_org.keys())
        sites.remove('year')
        gwlevel = pd.melt(data_org,
                            id_vars=['year'],
                                value_vars=sites,
                                var_name='site',
                                value_name='level')

        g = gwlevel.groupby(['year','site'])
        gw_median = g.aggregate({'level': np.median}).reset_index()
        gw_min = g.aggregate({'level': np.min}).reset_index()
        gw_max = g.aggregate({'level': np.max}).reset_index()

        gw_median = pd.pivot_table(gw_median, values='level', index='year', columns='site')
        gw_min = pd.pivot_table(gw_min, values='level', index='year', columns='site')
        gw_max = pd.pivot_table(gw_max, values='level', index='year', columns='site')

        gw_median.to_csv('{}/annual_level_median_{}.csv'.format(outdir,year))
        gw_min.to_csv('{}/annual_level_min_{}.csv'.format(outdir,year))
        gw_max.to_csv('{}/annual_level_max_{}.csv'.format(outdir,year))

        # annual stuff
        indir = '{bd}/{d}/filled_data_by_time_period/filled_annual_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        try:
            data = pd.read_csv(indir, index_col=0)
        except IOError:
            continue
        if year == 1985 and dir_ == 'surfacewater':
            continue

        # calculate annual statistics
        outdata = pd.DataFrame(index=data.index)
        outdata['min'] = data.min(axis=1)
        outdata['max'] = data.max(axis=1)
        outdata['mean'] = data.mean(axis=1)
        outdata['sd'] = data.std(axis=1)
        outdata['median'] = data.median(axis=1)
        outdata['q25'] = data.quantile(axis=1, q=0.25)
        outdata['q75'] = data.quantile(axis=1, q=0.75)

        outdata.to_csv('{}/yearly_stats_{}.csv'.format(outdir, year))


