# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:30:36 2017

@author: MichaelEK
Extra functions for Pandas.
"""
import numpy as np
from pandas.core.groupby import SeriesGroupBy, GroupBy


def grp_mode(df, grp_cols, val_col):
    """
    Groupby mode for Pandas DataFrames.
    """

    df1 = df.groupby(grp_cols)[val_col].value_counts()
    df1.name = 'count'
    df2 = df1.reset_index(val_col).drop('count', axis=1)
    levels = np.arange(0, len(grp_cols)).tolist()
    df3 = df2.groupby(level=levels)[val_col].first().reset_index()
    return df3


def pd_grouby_fun(fun_name):
    """
    Function to make a function specifically to be used on pandas groupby objects from a string code of the associated function.
    """

    if fun_name in GroupBy.__dict__.keys():
        fun1 = GroupBy.__dict__[fun_name]
    elif fun_name in SeriesGroupBy.__dict__.keys():
        fun1 = SeriesGroupBy.__dict__[fun_name]
    else:
        raise ValueError('Need to use the right function name.')
    return fun1
