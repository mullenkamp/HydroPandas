"""
Author: matth
Date Created: 23/03/2017 2:17 PM
"""

from __future__ import division
from core import env
import pandas as pd
from fill_data_by_correlation import n_non_null_idx, fill_df_by_similar_well
from gap_analysis import get_gap_len_pos_monthly
from copy import deepcopy


base_dir = env.sci('Groundwater/Trend_analysis/Data/organized data')
orgdata = pd.read_csv("{}/groundwater/original_data/monthly_groundwater_data.csv".format(base_dir), index_col=0)
orgdata.index = pd.to_datetime(orgdata.index)
orgdata = orgdata[orgdata.index > pd.to_datetime('14/11/1952')]
data = deepcopy(orgdata)

temp = pd.DataFrame(index=orgdata.keys())
for key in data.keys():
    start_date = n_non_null_idx(data[key], 6, raise_none=False)
    if pd.isnull(start_date):
        data = data.drop(key, axis=1)
        continue
    elif start_date > pd.to_datetime ('01/01/1960'):
        data = data.drop(key, axis=1)
        continue

    gap_id, gap_length, start, end = get_gap_len_pos_monthly(data[key])
    idx = pd.to_datetime(start) > start_date
    temp.loc[key,'max_len'] = gap_length[idx].max()
    temp.loc[key,'num'] = gap_id[idx].max()+1

    if gap_length[idx].max() > 12:
        data = data.drop(key, axis=1)

temp = temp.loc[list(data.keys()), :]
temp.to_csv("{}/groundwater/original_by_time_period/long_gaps.csv".format(base_dir))
data.to_csv("{}/groundwater/original_by_time_period/long_data.csv".format(base_dir))

# fill data
# linear interpolation 1
filled_data_back = orgdata.fillna(method='bfill', limit=1)
filled_data_forward = orgdata.fillna(method='ffill', limit=1)
idx = pd.isnull(orgdata)
orgdata[idx] = (filled_data_back[idx] + filled_data_forward[idx]) / 2

outdata, lr_dict, outwells, stats_output_overview = fill_df_by_similar_well(orgdata,12, 6,min_corr_c=0,
                                                                                    return_stats=True)
stats_output_overview = stats_output_overview.loc[list(data.keys()),:]
stats_output_overview.to_csv('{}/groundwater/filled_data_by_time_period/long_filling_stats.csv'.format(base_dir))

for key in data.keys():
    idx = pd.isnull(data[key])
    data[key][idx] = outdata[key][idx]

data.to_csv('{}/groundwater/filled_data_by_time_period/filled_month_groundwater_data_1952.csv'.format(base_dir))