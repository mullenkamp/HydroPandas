"""
Author: matth
Date Created: 16/03/2017 11:28 AM
"""

from __future__ import division
import pandas as pd
import numpy as np
import datetime as dt
from gap_analysis import get_gap_len_pos_monthly
from fill_data_by_correlation import n_non_null_idx
from copy import deepcopy
import os

def annual_median(series):
    """
    quick function to be passed to groupby aggrigate
    :param series:
    :return:
    """
    if len(series) < 9: #groupby automatically excludes nan values
        return np.nan
    else:
        return series.median()


def remove_by_startidx(data, start_non_null, start_idx):
    """
    returns the dataframe with columns removed if they have too large of data gaps or if data starts after the start of
     the dataframe assumes data has a datetime index.
    :param data: pd.dataframe with a time index
    :param start_non_null: the number of non-null values in a row that must be present for the start of the data
    :return:
    """
    keys = data.keys()
    for key in keys:
        # check start date
        data_start_date = n_non_null_idx(data[key], start_non_null, raise_none=False)
        if pd.isnull(data_start_date):
            data = data.drop(key, axis=1)
        elif data_start_date > start_idx:
            data = data.drop(key, axis=1)

    return data


########################################################################################################################
#################################################### flow data #########################################################
########################################################################################################################

flow_dir = "P:/Groundwater/Trend_analysis/Data/organized data/surfacewater/original_data/"

# create monthly median values of flow data
all_data1 = pd.read_excel(flow_dir + "HYCSV_ListTemp_KBMOINGW_working.xlsx")

org_flow_data = pd.melt(all_data1,
                        id_vars=['DMY'],
                        value_vars=['STYX',
                                    'AVON',
                                    'HOON HAY',
                                    'HEATHCOTE',
                                    'KAITUNA',
                                    'HALSWELL',
                                    'L-2',
                                    'SELWYN',
                                    'HAWKINS',
                                    'HORORATA',
                                    'DOYLESTON',
                                    'HARTS',
                                    'LEE'],
                        var_name='Site',
                        value_name='flow')

org_flow_data = org_flow_data.dropna(how='any')
dates = pd.DatetimeIndex(org_flow_data.DMY)
org_flow_data.DMY = pd.to_datetime(org_flow_data.DMY)
org_flow_data['year'] = dates.year
org_flow_data['month'] = dates.month

# do group statistics
g = org_flow_data.groupby(['year', 'month', 'Site'])
flow_monthly_med = g.aggregate({'flow': np.median}).reset_index()


# hackishly add data to fix a changed data problem (no datt between 08-11 1986
temp = pd.DataFrame({'year':[1986,1986,1986,1986],
                     'month':[8, 9, 10, 11],
                     'Site':['DOYLESTON', 'DOYLESTON', 'DOYLESTON', 'DOYLESTON'],
                     'flow':[-999, -999, -999, -999]})
flow_monthly_med = flow_monthly_med.append(temp,ignore_index=True)

flow_monthly_med['day'] = 15
flow_monthly_med['DMY'] = pd.to_datetime(flow_monthly_med[['year', 'month', 'day']])
# write un-modified data
flow_monthly_med = pd.pivot_table(flow_monthly_med, values='flow', index='DMY', columns='Site')
flow_monthly_med = flow_monthly_med[flow_monthly_med.index < dt.datetime(2017, 1, 1)]
flow_monthly_med.DOYLESTON[flow_monthly_med.DOYLESTON == -999] = np.nan
flow_monthly_med.to_csv(flow_dir + 'monthly_surfacewater_data.csv')

# create annual median values of flow data
g2 = org_flow_data.groupby(['year', 'Site'])
flow_annual_med = g2.aggregate({'flow': annual_median}).reset_index()
flow_annual_med = pd.pivot_table(flow_annual_med, values='flow', index='year', columns='Site')
flow_annual_med = flow_annual_med[flow_annual_med.index < 2017]
flow_annual_med.to_csv(flow_dir + 'annual_surfacewater_data.csv')

########################################################################################################################
############################################ well data #################################################################
########################################################################################################################

# format groundwater monthly data
well_dir = "P:/Groundwater/Trend_analysis/Data/organized data/groundwater/original_data/"

# load data
org_well_data = pd.read_excel(well_dir + "All_readings_from_DB.xlsx", sheetname=1)

well_det = pd.read_excel(well_dir + "Wells_2_analyse.xls")

well_ref_dict = {}
for key, val in zip(well_det.WELL_NO, well_det.REFERENCE_RL):
    well_ref_dict[key] = val

# check duplicates
temp_len1 = len(org_well_data.WELL_NO)
org_well_data = org_well_data.drop_duplicates(['WELL_NO', 'DMY'])
temp_len2 = len(org_well_data.WELL_NO)
if temp_len1 != temp_len2:
    raise ValueError('duplicates found in dataset, please manage')

# drop NAN value
org_well_data = org_well_data[(pd.notnull(org_well_data.WELL_NO)) & (pd.notnull(org_well_data.AverageWL)) &
                              (pd.notnull(org_well_data.DMY))]
org_well_data.DMY = pd.to_datetime(org_well_data.DMY)

# turn into 'pivot table'
well_monthly_data = pd.pivot_table(org_well_data, values='AverageWL', index='DMY', columns='WELL_NO')

# transform to water level
for key in well_monthly_data.keys():
    well_monthly_data[key] += well_ref_dict[key]

for key in ['L36/0015', 'L36/1076', 'L36/0424', 'M35/8967', 'M35/8380', 'M35/8372']:
    try:
        well_monthly_data = well_monthly_data.drop(key, 1)
    except:
        print('{} not found in dataframe'.format(key))

well_monthly_data = well_monthly_data[well_monthly_data.index < dt.datetime(2017, 1, 1)]
well_monthly_data.to_csv(well_dir + 'monthly_groundwater_data.csv')

# create annual median values for groundwater data
org_well_data = org_well_data.rename(columns={'Year': 'year'})
g3 = org_well_data.groupby(['year', 'WELL_NO'])
well_annual_med = g3.aggregate({'AverageWL': annual_median}).reset_index()
well_annual_med = pd.pivot_table(well_annual_med, values='AverageWL', index='year', columns='WELL_NO')

# transform to water level
for key in well_annual_med.keys():
    well_annual_med[key] += well_ref_dict[key]

for key in ['L36/0015', 'L36/1076', 'L36/0424', 'M35/8967', 'M35/8380', 'M35/8372']:
    try:
        well_annual_med = well_annual_med.drop(key, 1)
    except:
        print('{} not found in dataframe'.format(key))
well_annual_med = well_annual_med[well_annual_med.index < 2017]
well_annual_med.to_csv(well_dir + 'annual_groundwater_data.csv')

########################################################################################################################
############################################ subset data ###############################################################
########################################################################################################################
basedir = "P:/Groundwater/Trend_analysis/Data/organized data"
dir_list = ['groundwater', 'surfacewater']
years = [1975, 1985, 1995, 2005]

for dir in dir_list:
    if not os.path.exists('{od}/{d}/original_by_time_period'.format(od=basedir, d=dir)):
        os.makedirs('{od}/{d}/original_by_time_period'.format(od=basedir, d=dir))


    # load data
    monthdata = pd.read_csv('{od}/{d}/original_data/monthly_{d}_data.csv'.format(od=basedir, d=dir), index_col=0)
    monthdata.index = pd.to_datetime(monthdata.index)
    monthdata = monthdata[monthdata.index >= pd.to_datetime('01/01/1975')]

    yeardata = pd.read_csv('{od}/{d}/original_data/annual_{d}_data.csv'.format(od=basedir, d=dir), index_col=0)
    for year in years:
        month_temp = deepcopy(monthdata)
        year_temp = deepcopy(yeardata)
        # remove all column that have too large of gaps or whose data starts after the start date
        month_temp = remove_by_startidx(month_temp, 6, pd.to_datetime("15/01/{}".format(year)))
        year_temp = remove_by_startidx(year_temp, 2, year)

        # subset by time
        starttime = dt.datetime(year, 1, 1)
        month_temp = month_temp[month_temp.index >= starttime]
        year_temp = year_temp[year_temp.index >= year]


        # remove all 2017 data
        month_temp = month_temp[month_temp.index < dt.datetime(2017, 1, 1)]
        year_temp = year_temp[year_temp.index < 2017]

        month_temp.to_csv('{od}/{d}/original_by_time_period/month_{d}_data_{y}.csv'.format(od=basedir, d=dir, y=year))
        year_temp.to_csv('{od}/{d}/original_by_time_period/annual_{d}_data_{y}.csv'.format(od=basedir, d=dir, y=year))
