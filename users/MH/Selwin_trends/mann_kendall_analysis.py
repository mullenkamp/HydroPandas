"""
Author: matth
Date Created: 16/03/2017 11:30 AM
"""

from __future__ import division
import pandas as pd
from core.ecan_io import rd_sql, sql_db
from core.stats import mann_kendall_obj, seasonal_kendall
import os

base_dir = "P:/Groundwater/Trend_analysis/Data/organized data"
well_details = rd_sql(**sql_db.wells_db.well_details)
well_details = well_details.set_index('WELL_NO')
years = [1975, 1985, 1995, 2005]
for dir_ in ['groundwater', 'surfacewater']:
    outdir = '{bd}/{d}/Mann_Kendall_Analysis'.format(bd=base_dir, d=dir_)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    sites = []
    yearly_data_dict = {}
    monthly_data_dict = {}

    # load data
    for year in years:
        yearly_path = '{bd}/{d}/original_by_time_period/annual_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        monthly_path = '{bd}/{d}/original_by_time_period/month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        yearly_data_dict[year] = pd.read_csv(yearly_path, index_col=0)
        monthly_data_dict[year] = pd.read_csv(monthly_path, index_col=0)
        monthly_data_dict[year].index = pd.to_datetime(monthly_data_dict[year].index)
        sites.extend(yearly_data_dict[year].keys())
        sites.extend(monthly_data_dict[year].keys())

    # condense site list and set up output data
    sites = list(set(sites))

    outdata_monthly = pd.DataFrame(index=sites)
    outdata_yearly = pd.DataFrame(index=sites)
    # calculate mann-kendalls
    for year in years:
        for site in sites:
            try:
                temp_data = pd.DataFrame({'data':monthly_data_dict[year][site]})
                temp_data['month'] = temp_data.index.month
                temp = seasonal_kendall(temp_data,'data','month')
                outdata_monthly.loc[site, 'smk_{}'.format(year)] = temp.trend

            except KeyError as val:
                pass

            try:
                temp_data = pd.DataFrame({'data':yearly_data_dict[year][site]})
                temp = mann_kendall_obj(temp_data,data_col='data')
                outdata_yearly.loc[site, 'mk_{}'.format(year)] = temp.trend
            except KeyError as val:
                pass

    outpath_m = '{bd}/{d}/Mann_Kendall_Analysis/monthly_mann_kendal.csv'.format(bd=base_dir, d=dir_)
    outpath_y = '{bd}/{d}/Mann_Kendall_Analysis/annual_mann_kendal.csv'.format(bd=base_dir, d=dir_)

    if dir_ == 'groundwater':
        for key in outdata_monthly.index:
            lat = well_details.loc[key,'NZTMY']
            lon = well_details.loc[key,'NZTMX']
            outdata_monthly.loc[key,'lon'] = lon
            outdata_monthly.loc[key,'lat'] = lat

        for key in outdata_yearly.index:
            lat = well_details.loc[key,'NZTMY']
            lon = well_details.loc[key,'NZTMX']
            outdata_yearly.loc[key,'lon'] = lon
            outdata_yearly.loc[key,'lat'] = lat

    outdata_monthly.to_csv(outpath_m)
    outdata_yearly.to_csv(outpath_y)

years = [1952, 1975, 1985, 1995, 2005]
for dir_ in ['groundwater', 'surfacewater']:
    sites = []
    yearly_data_dict = {}
    monthly_data_dict = {}

    # load data
    for year in years:
        yearly_path = '{bd}/{d}/filled_data_by_time_period/filled_annual_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        monthly_path = '{bd}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        try:
            if year == 1952:
                yearly_data_dict[year] = None
            else:
                yearly_data_dict[year] = pd.read_csv(yearly_path, index_col=0)
            monthly_data_dict[year] = pd.read_csv(monthly_path, index_col=0)
            monthly_data_dict[year].index = pd.to_datetime(monthly_data_dict[year].index)
            if year != 1952:
                sites.extend(yearly_data_dict[year].keys())
            sites.extend(monthly_data_dict[year].keys())
        except:
            yearly_data_dict[year] = None
            monthly_data_dict[year] = None

    # condense site list and set up output data
    sites = list(set(sites))

    outdata_monthly = pd.DataFrame(index=sites)
    outdata_yearly = pd.DataFrame(index=sites)
    # calculate mann-kendalls
    for year in years:
        for site in sites:
            try:
                if monthly_data_dict[year] is None:
                    raise KeyError # hackish way of getting past some tacked on data
                temp_data = pd.DataFrame({'data': monthly_data_dict[year][site]})
                temp_data['month'] = temp_data.index.month
                temp = seasonal_kendall(temp_data, 'data', 'month')
                outdata_monthly.loc[site, 'smk_{}'.format(year)] = temp.trend

            except KeyError as val:
                pass

            try:
                if yearly_data_dict[year] is None:
                    raise KeyError
                temp_data = pd.DataFrame({'data': yearly_data_dict[year][site]})
                temp = mann_kendall_obj(temp_data, data_col='data')
                outdata_yearly.loc[site, 'mk_{}'.format(year)] = temp.trend
            except KeyError as val:
                pass

    outpath_m = '{bd}/{d}/Mann_Kendall_Analysis/filled_monthly_mann_kendal.csv'.format(bd=base_dir, d=dir_)
    outpath_y = '{bd}/{d}/Mann_Kendall_Analysis/filled_annual_mann_kendal.csv'.format(bd=base_dir, d=dir_)

    if dir_ == 'groundwater':
        for key in outdata_monthly.index:
            lat = well_details.loc[key, 'NZTMY']
            lon = well_details.loc[key, 'NZTMX']
            outdata_monthly.loc[key, 'lon'] = lon
            outdata_monthly.loc[key, 'lat'] = lat

        for key in outdata_yearly.index:
            lat = well_details.loc[key, 'NZTMY']
            lon = well_details.loc[key, 'NZTMX']
            outdata_yearly.loc[key, 'lon'] = lon
            outdata_yearly.loc[key, 'lat'] = lat

    outdata_monthly.to_csv(outpath_m)
    outdata_yearly.to_csv(outpath_y)



