# -*- coding: utf-8 -*-
"""
General time series tools.
"""
import pandas as pd


def df_first_valid(df):
    """Get the time index of the first non-na value"""
    def func(x):
        if x.first_valid_index() is None:
            return None
        else:
            return x.first_valid_index()
    df2 = df.apply(func, axis=0)
    return df2


def df_last_valid(df):
    """Get the time index of the last non-na value"""
    def func(x):
        if x.last_valid_index() is None:
            return None
        else:
            return x.last_valid_index()
    df2 = df.apply(func, axis=0)
    return df2


# def stats(self, mtypes, sites=None, below_median=False):
#     """
#     Function to produce stats for specific mytpes.
#
#     mtypes : str
#         A single str easurement type.
#     sites : str, int, or list
#         A str, int, or list of sites that should be included.
#     below_median : bool
#         Specific for the 'flow' stats.
#     """
#
#     if isinstance(mtypes, str):
#         if 'river_flow_cont' in mtypes:
#             data = self.sel_ts(mtypes=['river_flow_cont'], sites=sites, pivot=True)
#             if data.index.inferred_freq != 'D':
#                 data = data.resample('D').mean()
#             stats1 = flow_stats(data, below_median=below_median)
#             return stats1
#         if 'atmos_precip_cont' in mtypes:
#             data = self.sel_ts(mtypes=['atmos_precip_cont'], sites=sites)
#             data.index = data.index.droplevel('mtype')
#             stats1 = precip_stats(data)
#             return stats1
#         if 'aq_wl_cont' in mtypes:
#             data = self.sel_ts(mtypes=['aq_wl_cont'], sites=sites)
#             data.index = data.index.droplevel('mtype')
#             stats1 = precip_stats(data)
#             return stats1
#     else:
#         raise ValueError('No stats for the mtype')


def tsreg(ts, freq=None, interp=False):
    """
    Function to regularize a time series object (pandas).
    The first three indeces must be regular for freq=None!!!

    ts -- pandas time series dataframe.\n
    freq -- Either specify the known frequency of the data or use None and
    determine the frequency from the first three indices.\n
    interp -- Should linear interpolation be applied on all missing data?
    """

    if freq is None:
        freq = pd.infer_freq(ts.index[:3])
    ts1 = ts.resample(freq).mean()
    if interp:
        ts1 = ts1.interpolate('time')

    return ts1
