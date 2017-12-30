# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:30:36 2017

@author: MichaelEK
Simple functions for Pandas.
"""
from numpy import arange


def grp_mode(df, grp_cols, val_col):
    """
    Groupby mode for Pandas DataFrames.
    """

    df1 = df.groupby(grp_cols)[val_col].value_counts()
    df1.name = 'count'
    df2 = df1.reset_index(val_col).drop('count', axis=1)
    levels = arange(0, len(grp_cols)).tolist()
    df3 = df2.groupby(level=levels)[val_col].first().reset_index()
    return df3