"""
Author: matth
Date Created: 6/03/2017 9:27 AM
"""

from __future__ import division

from copy import deepcopy

import numpy as np
import pandas as pd

from users.MH.Selwin_trends.gap_analysis import calc_gaps

all_data = pd.read_excel(r"P:\Groundwater\Trend_analysis\Data\2_Flow_data_excel\HYCSV_ListTemp_KBMOINGW_working.xlsx")

all_data2 = pd.melt(all_data,
                    id_vars=['DMY'],
                    value_vars=['STYX','AVON','HOON HAY','HEATHCOTE','KAITUNA','HALSWELL','L-2','SELWYN','HAWKINS','HORORATA','DOYLESTON','HARTS','LEE'],
                    var_name='Site',
                    value_name='flow')

all_data2 = all_data2.dropna(how='any')
dates = pd.DatetimeIndex(all_data2.DMY)
all_data2.DMY = pd.to_datetime(all_data2.DMY)
all_data2['year'] = dates.year
all_data2['month'] = dates.month
# do group statistics
g = all_data2.groupby(['year','month','Site'])
monthly_med = g.aggregate({'flow':np.median}).reset_index()
monthly_med['day'] = 15
monthly_med['datetime'] = pd.to_datetime(monthly_med[['year','month','day']])

# write un-modified data
outdir = "P:/Groundwater/Trend_analysis/Data/modified_data/Flow/"
monthly_med = pd.pivot_table(monthly_med, values='flow', index='datetime', columns='Site')
monthly_med.to_csv(outdir + 'flow_level_unfilled.csv')
calc_gaps(outdir + 'flow_gaps_unfilled.csv', monthly_med)

#fill gaps of length 1
filled_data_back = monthly_med.fillna(method='bfill', limit=1)
filled_data_forward = monthly_med.fillna(method='ffill', limit=1)
filled_data = deepcopy(monthly_med)
idx = pd.isnull(filled_data)
filled_data[idx] = (filled_data_back[idx] + filled_data_forward[idx]) / 2

filled_data.to_csv(outdir + 'flow_level_1gaps_filled.csv')
calc_gaps(outdir + 'flow_gaps_without_1gaps.csv', filled_data)




