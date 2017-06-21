"""
Author: matth
Date Created: 7/03/2017 3:26 PM
"""

from __future__ import division

from copy import deepcopy

import numpy as np
import pandas as pd

from core.stats.LR_class import LR
from gap_analysis import get_gap_len_pos_monthly


def fill_df_by_similar_well(indata, max_gap, n_sd, min_corr_c=0, return_stats=False):
    """

    :param indata: pandas dataframe will all data and time index
    :param well_to_fill:
    :param n_sd: number of non-null values to indicate the start of a time series
    :param min_corr_c: the minimum value of a corellation coffient to include in the filling
    :return: filled dataframe
    """
    outdata = deepcopy(indata)
    outwells = pd.DataFrame(index=outdata.index, columns=outdata.columns, dtype=object)
    # compute start date for all wells (dict)
    start_date_dict = {}
    for key in indata.keys():
        start_date_dict[key] = n_non_null_idx(indata[key],n_sd)
    # fill data by series
    lr_dict = {}
    for key in outdata.keys():
        gap_id, gap_length, start, end = get_gap_len_pos_monthly(outdata[key])
        if len(gap_length) == 0:
            pass
        elif gap_length.max() > max_gap:
            continue

        if return_stats:
            lr_dict[key], outdata[key], outwells[key] = fill_well_by_similar_well(indata, key, start_date_dict, n_sd,
                                                                                  min_corr_c=min_corr_c,
                                                                                  return_stats=return_stats)
        else:
            outdata[key] = fill_well_by_similar_well(indata, key, start_date_dict, n_sd, min_corr_c=min_corr_c)

    if return_stats:
        stats_output_overview = pd.DataFrame(index=lr_dict.keys(),
                                             columns=['num_data_fill',
                                                      'num_wells_used',
                                                      'wells_used',
                                                      'cor_coef',
                                                      'per_data',
                                                      'SE'])
            #compile an overview spreadsheet
            #combine wells_used etc. into one string entry deliminated with ;'s
        for well in stats_output_overview.index:
            temp_data = indata[well][indata[well].index >= start_date_dict[well]]
            num_data_filled = pd.isnull(temp_data).sum()
            owell = outwells[well]
            stats_output_overview.loc[well,'num_data_fill'] = num_data_filled
            stats_output_overview.loc[well,'num_wells_used'] = len(set(owell[pd.notnull(owell)]))
            well_use = list(set(owell[pd.notnull(owell)]))
            stats_output_overview.loc[well,'wells_used'] = ';'.join(well_use)
            stats_output_overview.loc[well,'per_data'] = ';'.join(['{:2.1f}'.format(len(owell[owell == e])/num_data_filled*100) for e in well_use])
            stats_output_overview.loc[well,'cor_coef'] = ';'.join(['{:.3f}'.format(lr_dict[well][e].corr) for e in well_use])
            stats_output_overview.loc[well,'SE'] = ';'.join([str(lr_dict[well][e].stderr) for e in well_use])
            stats_output_overview.loc[well,'still_has_gaps'] = (pd.isnull(outdata[well]).sum() !=0)


        return outdata, lr_dict, outwells, stats_output_overview
    else:
        return outdata

def n_non_null_idx(series, n, raise_none=True):
    """
    return the index of the first insance of n non-null values in a row
    :param series: time series of well data
    :param n: the number of non-null values in a row to evaluate
    :return: start date as datetime object or np.nan if raise_none=False and no such value
    """
    null = pd.notnull(series)
    n_in_row_check = 0
    loop_counter = 0
    while n_in_row_check == 0:
        loop_counter += 1
        if loop_counter >= len(series)-n: # stop runaway
            if raise_none:
                raise ValueError('there are not that many consecutive non-null values')
            else:
                return np.nan
        n_in_row_check = 1
        for i in range(n):
            n_in_row_check *= null.iloc[i + loop_counter - 1]
    outdata = series.index[loop_counter - 1]
    return outdata



def fill_well_by_similar_well(indata, well_to_fill, well_start_date_dict,n,return_stats=False, min_corr_c=0):
    """

    :param indata: dataframe
    :param well_to_fill: string of well num description of the well
    :param n: number of consecutive non null values to define start of data
    :param well_start_date_dict: dictionary of well_no: startdates
    :param return_stats: if true return array of wells that were filled
    :param min_corr_c: the minimum correlation coffient used to fill data
    :return: series of filled data
    """

    # compute correlation (for all wells relative to current well) and Linear Regression
    well_list = list(indata.keys())
    well_list.remove(well_to_fill)
    well_list = np.array(well_list)
    well_cor_coef = np.zeros(well_list.shape)*np.nan
    lr_dict = {}
    for i, well in enumerate(well_list):
        temp = indata[[well_to_fill,well]]
        well_cor_coef[i] = temp.corr().iloc[0, 1]
        lr_dict[well] = LR(indata[well],indata[well_to_fill])

    # create data frame from coreelation values and startdate
    idx = np.argsort(well_list)
    well_list = well_list[idx]
    well_cor_coef = well_cor_coef[idx]
    well_list_temp =well_start_date_dict.keys()
    well_list_temp.remove(well_to_fill)
    well_list_temp = np.array(well_list_temp)
    well_list_temp.sort()
    well_start_date  = np.zeros(well_list_temp.shape,dtype=object)
    for i, key in enumerate(well_list_temp):
        well_start_date[i] = well_start_date_dict[key]
    # make a dataframe of all useful information
    fill_data_options = pd.DataFrame(data={'cor_val':well_cor_coef, 'start_date':well_start_date},index=well_list)
    fill_data_options = fill_data_options.sort_values('cor_val',ascending=False)

    # fill each missing value with best option
    outdata = deepcopy(indata[well_to_fill])
    out_wells = pd.Series(index=outdata.index, dtype=object)
    for time, val in outdata[pd.isnull(outdata)].iteritems():
        if pd.notnull(val):
            continue

        # find find well with higest corellation coeffient that has data at that time to fill value
        x_predict = np.nan
        loop_counter = 0
        while pd.isnull(x_predict):
            loop_counter += 1
            if loop_counter > 1000:
                raise ValueError('run away while loop')

            if loop_counter > len(fill_data_options.index):
                out_wells.loc[time] = np.nan
                outdata.loc[time] = np.nan
                break
            fill_well = fill_data_options.index[loop_counter-1]
            x_predict = indata.loc[time,fill_well]

        if lr_dict[fill_well].corr < min_corr_c:
            out_wells.loc[time] = np.nan
            outdata.loc[time] = np.nan
        else:
            val = lr_dict[fill_well].predict(x_predict)
            outdata.loc[time] = val
            out_wells.loc[time] = fill_well

    if return_stats:
        key_list = list(set(out_wells[pd.notnull(out_wells)]))
        lr_short_dict = {key: lr_dict[key] for key in key_list}

        return lr_short_dict, outdata, out_wells
    else:
        return outdata


if __name__ == '__main__':
    test = pd.Series([np.nan,1,2,np.nan,1,2,3,4,5,np.nan,1,2,3,4,5,6,7,8,9])
    print(test)
    print(n_non_null_idx(test, 2))
    print(n_non_null_idx(test, 5))
    print(n_non_null_idx(test, 6))
    print(n_non_null_idx(test, 25,raise_none=False))

