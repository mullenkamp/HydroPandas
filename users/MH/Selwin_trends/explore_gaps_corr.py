"""
Author: matth
Date Created: 21/03/2017 10:57 AM
"""

from __future__ import division
import pandas as pd
import numpy as np
from gap_analysis import get_gap_len_pos_monthly
from fill_data_by_correlation import n_non_null_idx
from copy import deepcopy


orgdata = pd.read_csv("P:\Groundwater\Trend_analysis\Data\organized data\groundwater\original_data\monthly_groundwater_data.csv",
                   index_col=0)
orgdata.index = pd.to_datetime(orgdata.index)
orgdata = orgdata[orgdata.index >= pd.to_datetime('01/01/1975')]
years = [1975, 1985, 1995, 2005]
for year in years:
    data = deepcopy(orgdata)
    for key in data.keys():
        start_date = n_non_null_idx(data[key],6, raise_none=False)
        if pd.isnull(start_date):
            data = data.drop(key, axis=1)
        elif start_date > pd.to_datetime("15/01/{}".format(year)):
            data = data.drop(key, axis=1)


    bywell = pd.DataFrame(index=data.keys())


    corr_coeffs = []
    # create array of corcoeffs
    for key1 in data.keys():
        temp_coefs = []
        for key2 in data.keys():
            if key1 == key2:
                continue
            temp = data[[key1,key2]]
            temp_coefs.append(temp.corr().iloc[0, 1])

        corr_coeffs.extend(temp_coefs)
        temp_coefs = np.array(temp_coefs)
        bywell.loc[key1, 'coeffs_max'] = temp_coefs.max()
        bywell.loc[key1, 'coeffs_75'] = np.percentile(temp_coefs, 75)
        bywell.loc[key1, 'coeffs_mean'] = temp_coefs.mean()
    corr_coeffs = np.array(corr_coeffs)

    # create array of gaps
    gaps = []
    for key in data.keys():
        gap_id, gap_length, start, end = get_gap_len_pos_monthly(data[key])
        start_date = n_non_null_idx(data[key],6,raise_none=False)
        idx = (pd.to_datetime(start) > start_date) & (pd.to_datetime(start) > pd.to_datetime("01/01/{}".format(year)))
        gap_length = gap_length[idx]
        if len(gap_length) == 0:
            bywell.loc[key,'max_gap'] = 0
        else:
            bywell.loc[key,'max_gap'] = gap_length.max()
        gaps.extend(gap_length)

    gaps = [x for x in gaps if x != 1]
    gaps = np.array(gaps)
    bywell.to_csv(r"P:\Groundwater\Trend_analysis\Data\organized data\groundwater\original_by_time_period\explore_data_{}.csv".format(year))
