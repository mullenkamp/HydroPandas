# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:33:42 2017

@author: MichaelEK
"""
import numpy as np
import pandas as pd


def precip_stats(df):
    """
    Function to claculate specific stats for precipitation data.

    df -- A Pandas Series in long format with 'site' and 'time' as the multiindex.
    """

    ### Check data structure
    if not isinstance(df, pd.Series):
        raise ValueError('df must be a pandas Series')
    if not isinstance(df.index, pd.MultiIndex):
        raise ValueError('The index of df must be a MultiIndex')
    if not all(np.in1d(['site', 'time'], df.index.names)):
        raise ValueError("The MultiIndex names must be 'site' and 'time'")
    else:
        index_names = df.index.names
#        site_index = index_names.index('site')
        time_index = index_names.index('time')
    if not isinstance(df.index.levels[time_index], pd.DatetimeIndex):
        time1 = pd.to_datetime(df.index.levels[time_index])
        df.index = df.index.set_levels(time1, time_index)

    ### Do stats
    grp1 = df.groupby(level=['site'])
    start = grp1.apply(lambda x: x.first_valid_index()[1])
    start.name = 'Start time'
    end = grp1.apply(lambda x: x.last_valid_index()[1])
    end.name = 'End time'
    stats1 = grp1.describe()[['50%', '75%', 'mean', 'max', 'count']].round(2)
    d_count = grp1.count()
    periods = (end - start).astype('timedelta64[D]').astype('int') + 1
    mis_days = periods - d_count
    mis_days.name = 'Missing days'
    mis_days_ratio = (mis_days/periods).round(2)
    mis_days_ratio.name = 'Missing ratio'
    years = (d_count/365.25).round(1)
    years.name = 'Tot data yrs'

    ### Assemble results and return
    out1 = pd.concat([stats1, start, end, mis_days, mis_days_ratio, years], axis=1)
    return out1



