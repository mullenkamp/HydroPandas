# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 16:48:38 2018

@author: MichaelEK
"""
import os
import numpy as np
import pandas as pd
from hydropandas.tools.general.general.extra_pandas import pd_grouby_fun


def w_agg(x, fun='sum', axis=1):
    """
    Aggregates a dataframe of data into a single series.
    """
    if not isinstance(x.index, pd.DatetimeIndex):
        print("DataFrame does not have a DatetimeIndex")
    if fun == 'mean':
        agg1 = x.mean(axis=axis)
    elif fun == 'sum':
        agg1 = x.sum(axis=axis)

    return agg1


def interp_resample(df, res_code, level=None):
    """
    Function to properly set up a resampling class for discrete data. This assumes a linear interpolation between data points.

    Parameters
    ----------
    df: DataFrame
        DataFrame with a time index.
    res_code: str
        Pandas resampling code. e.g. 'D'.
    level: str or int
        For a MultiIndex, level (name or number) to use for resampling. Level must be datetime-like.

    Returns
    -------
    Resampling class
    """
    df1 = (df + df.shift(-1))/2
    out1 = df1.resample(res_code, level=level)
    return out1


def resample(self, resample_code='A-JUN', fun='mean'):
    """
    Time series resampling function. Returns a Hydro class object with resampled data.

    Parameters
    ----------
    resample_code : str
        The Pandas resampling code for the resampling process (e.g. 'A' for annual, 'A-JUN' for annual ending in June (water year), 'D' for day, 'W' for week, 'M' for month')
    fun : str
        The function that should be applied. Any function that Pandas can handle in a groupby object.
    """

    ### Set up parameters
    fun1 = pd_grouby_fun(fun)

    ### Run resampling
    data = self.data

    df1 = data.groupby([pd.Grouper(level='mtype'), pd.Grouper(level='site'), pd.Grouper(level='time', freq=resample_code)])
    df2 = fun1(df1)

    ### Recreate hydro class object
    new1 = self.add_data(df2.reset_index(), 'time', 'site', 'mtype', 'data', dformat='long', add=False)
    return new1


def w_resample(x, pd_time_code='A-JUN', fun='sum', min_ratio=0.75, agg=False, agg_fun='mean', digits=3, interp=False, discrete=False, export=False, export_path='', export_name='precip_stats.csv'):
    """
    NEED TO UPDATE! CLEAN AND INCLUDE THE discrete PARAMETER USING interp_resample.

    Function to resample time series precip or flow data.

    Arguments:\n
    period -- The length of time that the data should be resampled. Options are: 'days', 'weeks', 'months', 'years', and 'water years' (and the singular).\n
    n_periods -- the number of 'periods' to aggregate over. E.g. when 'period' is set to 'months' and n_periods is equal to 2, then it aggregates over a two month period.\n
    fun -- The function that should be used to resample ('mean' or 'sum').\n
    min_ratio -- The minimum proportion of data to total days for the final estimate.\n
    agg -- Should the columns be aggregated together?\n
    agg_fun -- The function for the aggregation ('mean' or 'sum').\n
    interp -- Should missing days be interpolated by the period mean?\n
    export -- Should the results be exported?\n
    export_path -- Directory where the results will be exported.\n
    export_name -- The name of the csv file to be exported.
    """

    # Make sure the object is a data frame and regular
    df = x.copy()

    # Warn about duplicate stations
    if sum(df.columns.duplicated()) > 0:
        print("Duplicate stations!!! Please check!!!")
        df = df.loc[:, df.columns.duplicated() == False]

    # Aggregate stations if desired
    if agg:
        df = w_agg(df, fun=agg_fun)

    # Resample
    df_grp = df.resample(pd_time_code)

    # Determine number of days in each period
    count1 = df_grp.count()
    tot_count1 = df_grp.size()
    diff_count = count1.sub(tot_count1, axis='index').abs()
    max1 = max(tot_count1)
    tot_count1.iloc[[0, -1]] = max1
    ratio1 = count1.div(tot_count1, axis='index')

    # Apply function to resample
    if fun == 'mean':
        res1 = df_grp.mean()
        if isinstance(res1, pd.DataFrame):
            for i in res1.columns:
                res1.loc[ratio1[i] < min_ratio, i] = np.nan
        else:
            res1.loc[ratio1 < min_ratio] = np.nan
    elif fun == 'sum':
        res1 = df_grp.sum()
        mean1 = df_grp.mean()
        if isinstance(res1, pd.DataFrame):
            for i in res1.columns:
                if interp:
                    res1.loc[ratio1[i] < 0.95, i] = res1.loc[ratio1[i] < 0.95, i] + mean1.loc[ratio1[i] < 0.95, i] * \
                                                                                    diff_count.loc[ratio1[i] < 0.95, i]
                res1.loc[ratio1[i] < min_ratio, i] = np.nan
        else:
            if interp:
                res1.loc[ratio1 < 0.95] = res1.loc[ratio1 < 0.95] + mean1.loc[ratio1 < 0.95] * diff_count.loc[
                    ratio1 < 0.95]
            res1.loc[ratio1 < min_ratio] = np.nan

    # Export data and return dataframe
    if export:
        res1.round(digits).to_csv(os.path.join(export_path, export_name))
    return (res1.round(digits))


def res(ts, dformat, wide_index=[0, 1], period='water year', n_periods=1, fun='sum', min_ratio=0.75, agg=False,
        agg_fun='mean', digits=3, interp=False, export=False, export_path='', export_name='precip_stats.csv'):
    """
    Function to resample time series precip or flow data.

    Arguments:\n
    period -- The length of time that the data should be resampled. Options are: 'days', 'weeks', 'months', 'years', and 'water years' (and the singular).\n
    n_periods -- the number of 'periods' to aggregate over. E.g. when 'period' is set to 'months' and n_periods is equal to 2, then it aggregates over a two month period.\n
    fun -- The function that should be used to resample ('mean' or 'sum').\n
    min_ratio -- The minimum proportion of data to total days for the final estimate.\n
    agg -- Should the columns be aggregated together?\n
    agg_fun -- The function for the aggregation ('mean' or 'sum').\n
    interp -- Should missing days be interpolated by the period mean?\n
    export -- Should the results be exported?\n
    export_path -- Directory where the results will be exported.\n
    export_name -- The name of the csv file to be exported.
    """

    # Make sure the object is a data frame and regular
    df = ts.copy()

    if dformat == 'wide':

        # Warn about duplicate stations
        if sum(df.columns.duplicated()) > 0:
            raise ValueError("Duplicate stations!!! Please check!!!")

        # Aggregate stations if desired
        if agg:
            df = w_agg(df, fun=agg_fun)

        # Resample
        df_grp = df.resample(str(n_periods) + time_switch(period))

    elif dformat == 'long':

        # Warn about duplicate stations
        if sum(df.index.levels[0].duplicated()) > 0:
            raise ValueError("Duplicate stations!!! Please check!!!")

        df_grp = df.groupby(
            [pd.Grouper(level=wide_index[0]), pd.Grouper(level=wide_index[1], freq=str(n_periods) + time_switch(period))])

    else:
        raise ValueError('dformat must be either long or wide')

    # Determine number of days in each period
    count1 = df_grp.count()
    tot_count1 = df_grp.size()
    diff_count = count1.sub(tot_count1, axis='index').abs()
    max1 = max(tot_count1)
    tot_count1.iloc[[0, -1]] = max1
    ratio1 = count1.div(tot_count1, axis='index')

    # Apply function to resample
    if dformat == 'wide':

        if fun == 'mean':
            res1 = df_grp.mean()
            if isinstance(res1, pd.DataFrame):
                for i in res1.columns:
                    res1.loc[ratio1[i] < min_ratio, i] = np.nan
            else:
                res1.loc[ratio1 < min_ratio] = np.nan
        elif fun == 'sum':
            res1 = df_grp.sum()
            mean1 = df_grp.mean()
            if isinstance(res1, pd.DataFrame):
                for i in res1.columns:
                    if interp:
                        res1.loc[ratio1[i] < 0.95, i] = res1.loc[ratio1[i] < 0.95, i] + mean1.loc[ratio1[i] < 0.95, i] * \
                                                                                        diff_count.loc[
                                                                                            ratio1[i] < 0.95, i]
                    res1.loc[ratio1[i] < min_ratio, i] = np.nan
            else:
                if interp:
                    res1.loc[ratio1 < 0.95] = res1.loc[ratio1 < 0.95] + mean1.loc[ratio1 < 0.95] * diff_count.loc[
                        ratio1 < 0.95]
                res1.loc[ratio1 < min_ratio] = np.nan

    elif dformat == 'long':

        if fun == 'mean':
            res1 = df_grp.mean()
        if fun == 'sum':
            res1 = df_grp.sum()
        res1[ratio1 < min_ratio] = np.nan

    # Export data and return dataframe
    if export:
        res1.round(digits).to_csv(os.path.join(export_path, export_name))
    return res1.round(digits)