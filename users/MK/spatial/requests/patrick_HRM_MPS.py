# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 14:12:34 2017

@author: michaelek
"""

# Example

data = 'T:\Temp\Patrick\geostats\HRM_MPS_Summary_FULL.csv'
n_levels = 4
export_path = 'T:\Temp\Patrick\geostats'

def grid_agg(data, n_levels, xcol='xcentre', ycol='ycentre', zcol='zcentre', catcol='prob_cat', export_path='results'):
    """
    Function to aggregate elevation categories.

    Arguments:\
    """
    from pandas import read_csv, cut
    from numpy import linspace
    from os import path

    def grp_mode(df, grp_cols, val_col):
        """
        Groupby mode for Pandas DataFrames.
        """
        from numpy import arange

        df1 = df.groupby(grp_cols)[val_col].value_counts()
        df1.name = 'count'
        df2 = df1.reset_index(val_col).drop('count', axis=1)
        levels = arange(0, len(grp_cols)).tolist()
        df3 = df2.groupby(level=levels)[val_col].first().reset_index()
        return(df3)

    ##########################
    ### Read in data

    t1 = read_csv(data)

    #########################
    ### Prepare data

    zgrp1, int1 = cut(t1[zcol], n_levels, retbins=True)
    zgrp = cut(t1[zcol], n_levels, labels=int1[1:]).astype('float')

    #########################
    ### Aggregate data with mode

    q3 = grp_mode(t1, [xcol, ycol, zgrp], catcol)

    #########################
    ### Reorder data

    q4 = q3.pivot_table(index=[xcol, ycol], columns=zcol, values=catcol).fillna(0)

    ########################
    ### Save data

    for i in q4:
        q4[i].to_csv(path.join(export_path, 'output_' + str(i) + '.csv'), header=True)
    return(q4)
