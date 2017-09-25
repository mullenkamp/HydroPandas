# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 25/09/2017 5:13 PM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from warnings import warn


def map_rch_to_array(hdf, method, period_center, mapping_shp, period_length=10, return_irr_demand=False,
                     rch_quanity='total_drainage'):
    """
    takes a hdf and maps it into an array for the model, assumes that the hdf file is a water year amalgimation
    :param hdf: the hdf file that contains the water year average of the LSR from the LSRM
    :param method: one of:
                            'mean': mean of the full dataset
                            'period_mean': mean of the period defined above if the period is longer than the data set
                                           this is equivelent to 'mean' but warning is passed
                            '3_lowest_con_mean': mean of the three lowest consecuative years
                            'lowest_year': simply the lowest year
    :param mapping_shp: the path to the shapefile that links the data to the geometry
    :param period_center: the year of the center (int)
    :param period_length: the length of the period in years (int) must be even
    :param return_irr_demand: if true then returen both recharge and average annual (spatially summed) irrigation demand (single number)
    :param rch_quanity: the value to use
    :return: array of the shape of (smt.rows, smt.cols) if return irr_demand then (rch, irr_demand)
    """
    # quick check of inputs
    if method not in ['mean','period_mean', '3_lowest_con_mean', 'lowest_year']:
        raise ValueError('unsuported method')

    if not isinstance(period_center, int) and period_center is not None:
        raise TypeError('period_center must be int or None')

    if not isinstance(period_length, int) and period_length is not None:
        raise TypeError('period_length must be int or None')

    if isinstance(period_length,int):
        if period_length % 2 != 0:
            raise ValueError('period length must be even')

    outdata = smt.shape_file_to_model_array(mapping_shp, 'site', True)
    data = pd.read_hdf(hdf).reset_index()
    data.loc[:, 'year'] = [e.year for e in data.time]
    if method == 'mean':
        map_data = data.groupby('site').aggregate({rch_quanity: np.mean})
        if return_irr_demand:
            ird = data.groupby('year').aggregate({'irr_demand': np.sum})['irr_demand'].mean()
    else:
        data = data.loc[(data.year >= period_center - period_length / 2) & (data.year <= period_center + period_length / 2)]

        year_data = data.groupby('year').aggregate({rch_quanity: np.sum, 'irr_demand': np.sum})

        expected_years = range(int(period_center-period_length/2), int(period_center+period_length/2+1))
        if not all(np.in1d(expected_years,year_data.index)):
            warn('not all period years present taking mean of full dataset, years missing: {}'.format(set(expected_years) - set(year_data.index)))

        if method == 'period_mean':
            map_data = data.groupby('site').aggregate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.irr_demand.mean()
        elif method == 'lowest_year':
            low_year = year_data[rch_quanity].argmin()
            map_data = data.loc[data.year == low_year].groupby('site').aggregate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.loc[low_year, 'irr_demand']
        elif method == '3_lowest_con_mean':
            if len(year_data) <3:
                raise ValueError('3 year consectuative mean cannot be calculated on data with less than 3 years')
            elif len(year_data) == 3:
                warn('only 3 years of data, same as period mean')
            low_year = year_data.rolling(3).sum()[rch_quanity].argmin()
            low_years = [low_year - e for e in [2, 1, 0]]
            map_data = data.loc[np.in1d(data.year, low_years)].groupby('site').aggregate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.loc[low_years, 'irr_demand'].mean()
        else:
            raise ValueError('should not get here')
    outdata[np.isnan(outdata)] = -999
    temp_dict = map_data[rch_quanity].to_dict()
    temp_dict[-999] = np.nan
    #todo below is temporary to avoid missing keys
    print ('missing keys:')
    print (len(set(outdata.flatten()) - set(temp_dict.keys())))
    print (set(outdata.flatten()) - set(temp_dict.keys()))
    for i in range(outdata.shape[0]):
        for j in range(outdata.shape[1]):
            try:
                outdata[i,j] = temp_dict[outdata[i,j]]
            except KeyError:
                outdata[i,j] = np.nan
    #outdata = vec_translate(outdata, temp_dict) #todo reinstate later, simply working with a differnt method that is more robust

    if return_irr_demand:
        return outdata, ird
    else:
        return outdata


# quickest way to assign is to loop or to use below
def vec_translate(a, d):
    return np.vectorize(d.__getitem__)(a)


if __name__ == '__main__':
    test_path = r"K:\niwa_netcdf\lsrm\lsrm_results\vcsn_65perc.h5"
    hdf_path = r"K:\niwa_netcdf\lsrm\lsrm_results\water_year_means\wym_vcsn_50perc.h5"
    map_shp = r"K:\niwa_netcdf\lsrm\lsrm_results\test\output_test1.shp"
    methods = ['mean', 'period_mean', '3_lowest_con_mean', 'lowest_year']
    test = map_rch_to_array(hdf=hdf_path,
                            method=methods[0],
                            period_center=2012,
                            mapping_shp=map_shp,
                            period_length=10,
                            return_irr_demand=False)
    print 'done'