# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:30:36 2017

@author: MichaelEK
Extra functions for Pandas.
"""
import numpy as np
import pandas as pd
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


def pd_grouby_fun(fun_name, df):
    """
    Function to make a function specifically to be used on pandas groupby objects from a string code of the associated function.
    """
    if type(df) == pd.Series:
        fun1 = SeriesGroupBy.__dict__[fun_name]
    elif type(df) == pd.DataFrame:
        fun1 = GroupBy.__dict__[fun_name]
    else:
        raise ValueError('df should be either a Series or DataFrame.')
    return fun1
