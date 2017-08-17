# -*- coding: utf-8 -*-
"""
General time series tools.
"""
#from pandas.tseries.resample import Resampler
#from pandas.core.groupby import GroupBy


def resample(self, period='water year', n_periods=1, fun='mean'):
    """
    Time series resampling function. Returns a Hydro class object with resampled data.

    period -- The period that the data should be resampled over ('day', 'month', 'year', 'water year').\n
    n_periods -- The number of periods.\n
    fun -- The function that should be applied. Any function that Pandas can handle.
    """
    from pandas.core.groupby import SeriesGroupBy, GroupBy
    from pandas import Grouper
    from core.misc import time_switch

    ### Set up parameters
    time_code = str(n_periods) + time_switch(period)
    if fun in GroupBy.__dict__.keys():
        fun1 = GroupBy.__dict__[fun]
    elif fun in SeriesGroupBy.__dict__.keys():
        fun1 = SeriesGroupBy.__dict__[fun]
    else:
        raise ValueError('Need to use the right function name.')

    ### Run resampling
    data = self.data

    df1 = data.groupby([Grouper(level='mtype'), Grouper(level='site'), Grouper(level='time', freq=time_code)])
    df2 = fun1(df1)

    ### Recreate hydro class object
    new1 = self.add_data(df2.reset_index(), 'time', 'site', 'mtype', 'data', dformat='long', add=False)
    return(new1)


def stats(self, mtypes=None, sites=None, below_median=False):
    """
    Function to produce stats for specific mytpes.

    mtype -- A single str easurement type (required).\n
    sites -- A list of sites that should be included (optional).\n
    below_median -- Specific for the 'flow' stats.
    """
    from core.ts.sw.stats import flow_stats
    from core.ts.met.met_stats import precip_stats

    if isinstance(mtypes, str):
        if mtypes == 'flow':
            data = self.sel_ts(mtypes=['flow'], sites=sites, pivot=True)
            if data.index.inferred_freq != 'D':
                data = data.resample('D').mean()
            stats1 = flow_stats(data, below_median=below_median)
            return(stats1)
        if mtypes == 'precip':
            data = self.sel_ts(mtypes=['precip'], sites=sites)
            data.index = data.index.droplevel('mtype')
            stats1 = precip_stats(data)
            return(stats1)
        if mtypes == 'gwl':
            data = self.sel_ts(mtypes=['gwl'], sites=sites)
            data.index = data.index.droplevel('mtype')
            stats1 = precip_stats(data)
            return(stats1)


