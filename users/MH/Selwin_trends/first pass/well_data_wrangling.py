"""
Author: matth
Date Created: 2/03/2017 10:13 AM
"""

from __future__ import division

from copy import deepcopy

import pandas as pd

from core.stats import seasonal_kendall
from users.MH.Selwin_trends.fill_data_by_correlation import fill_df_by_similar_well, n_non_null_idx
from users.MH.Selwin_trends.gap_analysis import calc_gaps

if __name__ == '__main__':

    outdir = "P:/Groundwater/Trend_analysis/Data/modified_data/Level/"

    # load data
    all_data = pd.read_excel(r"P:\Groundwater\Trend_analysis\Data\1_Level_data_excel\All_readings_from_DB.xlsx",
                             sheetname=1)

    well_det = pd.read_excel(r"P:\Groundwater\Trend_analysis\Data\1_Level_data_excel\Wells_2_analyse.xls")

    well_ref_dict = {}
    for key, val in zip(well_det.WELL_NO, well_det.REFERENCE_RL):
        well_ref_dict[key] = val

    # check duplicates
    temp_len1 = len(all_data.WELL_NO)
    all_data = all_data.drop_duplicates(['WELL_NO', 'DMY'])
    temp_len2 = len(all_data.WELL_NO)
    if temp_len1 != temp_len2:
        raise ValueError('duplicates found in dataset, please manage')
    # drop NAN value
    all_data = all_data[(pd.notnull(all_data.WELL_NO)) & (pd.notnull(all_data.AverageWL)) & (pd.notnull(all_data.DMY))]
    all_data.DMY = pd.to_datetime(all_data.DMY)

    # turn into 'pivot table'

    working_data = pd.pivot_table(all_data, values='AverageWL', index='DMY', columns='WELL_NO')

    # transform to water level
    for key in working_data.keys():
        working_data[key] += well_ref_dict[key]

    for key in ['L36/0015', 'L36/1076', 'L36/0424', 'M35/8967', 'M35/8380', 'M35/8372']:
        try:
            working_data = working_data.drop(key, 1)
        except:
            print('{} not found in dataframe'.format(key))

    working_data.to_csv(outdir + 'well_water_level.csv')
    calc_gaps(outdir+'data_gaps.csv', working_data)

    # calculate seasonal_mann_kendall
    print('calculating mann kendall stats')
    s_kendal_output = pd.DataFrame(index=working_data.keys(), columns=['trend','p', 's'])
    for key in working_data.keys():
        temp_data = pd.DataFrame({key:working_data[key]})
        temp_data['month'] = working_data.index.month
        temp_test = seasonal_kendall(temp_data,key,'month',alpha=0.05,rm_na=True)
        s_kendal_output.loc[key,'trend'] = temp_test.trend
        s_kendal_output.loc[key,'p'] = temp_test.p
        s_kendal_output.loc[key,'s'] = temp_test.s
    s_kendal_output.to_csv(outdir+ 'season_mann_kendall_results.csv')

    # fill gaps of len 1 and recalculate gaps
    filled_data_back =  working_data.fillna(method='bfill',limit=1)
    filled_data_forward = working_data.fillna(method='ffill',limit=1)
    filled_1gaps_data = deepcopy(working_data)
    idx = pd.isnull(filled_1gaps_data)
    filled_1gaps_data[idx] = (filled_data_back[idx] + filled_data_forward[idx]) / 2

    filled_1gaps_data.to_csv(outdir + 'well_water_level_1gaps_filled.csv')
    calc_gaps(outdir + 'well_gaps_without_1gaps.csv', filled_1gaps_data)

    # fill all gaps that we can
    print('filling data gaps greater than 1')
    #set number of non-null
    non_null = 6
    full_filled, lr_dict, outwells, stats_output_overview = fill_df_by_similar_well(filled_1gaps_data,
                                                                                    non_null,return_stats=True)
    # plot regression diagnostic plots for all regressions used to fill data
    reg_plot_outdir = "P:/Groundwater/Trend_analysis/Data/Figures/level_data_figures/regression diagnostic figures/"
    for i,key in enumerate(lr_dict.keys()):
        if i%10 == 0:
            print('{} of {} plotted'.format(i,len(lr_dict.keys())))
        for key2 in lr_dict[key].keys():
            model = lr_dict[key][key2]
            model.plot(show=False,savepath='{}{} vs {} regression.png'.format(reg_plot_outdir,
                                                                              key.replace('/','_'),
                                                                              key2.replace('/','_')))

    full_filled.to_csv(outdir + 'well_water_level_fully_filled.csv')
    stats_output_overview.to_csv(outdir + 'fill_stats.csv')

    start_dates = pd.DataFrame(index=full_filled.keys(),columns=['start_date'])
    for idx in start_dates.index:
        start_dates.loc[idx,'start_date'] = n_non_null_idx(full_filled[idx],non_null)
    start_dates.to_csv(outdir + 'well_start_dates.csv')

