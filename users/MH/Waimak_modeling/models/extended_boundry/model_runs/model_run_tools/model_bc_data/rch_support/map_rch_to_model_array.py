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


def map_rch_to_array(hdf, method, period_center, mapping_shp, period_length=10, return_irr_demand=False,
                     rch_quanity='total_drainage'):
    """
    takes a hdf and maps it into an array for the model, assumes that the hdf file is a water year amalgimation
    :param hdf: the hdf file that contains the water year average of the LSR from the LSRM
    :param method: one of ['mean', '10_year_mean', '3_lowest_con_mean', 'lowest_year']
    :param mapping_shp: the path to the shapefile that links the data to the geometry
    :param period_center: the year of the center (int)
    :param period_length: the length of the period in years (int)
    :param return_irr_demand: if true then returen both recharge and average annual (spatially summed) irrigation demand (single number)
    :param rch_quanity: the value to use
    :return: array of the shape of (smt.rows, smt.cols) if return irr_demand then (rch, irr_demand)
    """
    # quick check of inputs
    if method not in ['10_year_mean', '3_lowest_con_mean', 'lowest_year']:
        raise ValueError('unsuported method')

    if not isinstance(period_center, int) and period_center is not None:
        raise TypeError('period_center must be int or None')

    if not isinstance(period_length, int) and period_length is not None:
        raise TypeError('period_length must be int or None')

    outdata = smt.shape_file_to_model_array(mapping_shp, 'site', True)
    data = pd.read_hdf(hdf).reset_index()
    data.loc[:, 'year'] = data.time.year
    if method == 'mean':
        map_data = data.groupby('site').aggrigate({rch_quanity: np.mean})
        if return_irr_demand:
            ird = data.groupby('year').aggrigate({'irr_demand': np.sum})['irr_demand'].mean()
    else:
        data = data.loc[(data.year >= period_center - period_length / 2) & (data.year <= period_center + period_length / 2)]

        year_data = data.groupby('year').aggrigate({rch_quanity: np.sum, 'irr_demand': np.sum})

        if method == '10_year_mean':
            map_data = data.groupby('site').aggrigate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.irr_demand.mean()
        elif method == 'lowest_year':
            low_year = year_data[rch_quanity].argmin()
            map_data = data.loc[data.year == low_year].groupby('site').aggrigate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.loc[low_year, 'irr_demand']
        elif method == '3_lowest_con_mean':
            low_year = year_data.rolling(3).sum().argmin()
            low_years = [low_year - e for e in [2, 1, 0]]
            map_data = data.loc[np.in1d(data.year, low_years)].groupby('site').aggrigate({rch_quanity: np.mean})
            if return_irr_demand:
                ird = year_data.loc[low_years, 'irr_demand'].mean()
        else:
            raise ValueError('should not get here')

    outdata = vec_translate(outdata, map_data[rch_quanity].to_dict())

    if return_irr_demand:
        return outdata, ird
    else:
        return outdata


# quickest way to assign is to loop or to use below
def vec_translate(a, d):
    return np.vectorize(d.__getitem__)(a)
