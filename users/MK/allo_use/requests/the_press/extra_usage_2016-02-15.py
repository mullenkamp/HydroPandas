# -*- coding: utf-8 -*-
"""
Created on Fri Feb 15 10:06:27 2019

@author: michaelek
"""
import os
import pandas as pd

pd.options.display.max_columns = 10


def grp_ts_agg(df, grp_col, ts_col, freq_code, discrete=False, **kwargs):
    """
    Simple function to aggregate time series with dataframes with a single column of sites and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str or list of str
        Column name that contains the sites.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').
    discrete : bool
        Is the data discrete? Will use proper resampling using linear interpolation.

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if isinstance(grp_col, str):
            grp_col = [grp_col]
        else:
            grp_col = grp_col[:]
        if discrete:
            val_cols = [c for c in df1.columns if c not in grp_col]
            df1[val_cols] = (df1[val_cols] + df1[val_cols].shift(-1))/2
        grp_col.extend([pd.Grouper(freq=freq_code, **kwargs)])
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')

#########################################
### Parameters

base_dir = r'E:\ecan\local\Projects\requests\the_press\2019-01-25'
input_csv = 'data_2018.csv'

datetime_col = ['DateTime']
flow_cols = ['M-S10-AW01-FLW-MTR.PresentValue', 'M-S10-AW02-FLW-MTR.PresentValue']

######################################
### Load data and reformat

cols = datetime_col.copy()
cols.extend(flow_cols)

data1 = pd.read_csv(os.path.join(base_dir, input_csv), usecols=cols).dropna()
data1['DateTime'] = pd.to_datetime(data1['DateTime'])
data1.columns = ['DateTime', 'flow1', 'flow2']

data1['tot_flow'] = data1.flow1 + data1.flow2

######################################
### Calc usage per water year

data1['tot_flow'] = (data1['tot_flow'] + data1['tot_flow'].shift(-1))/2

data1.set_index('DateTime', inplace=True)

data1.resample('A-JUN')['tot_flow'].mean() * 365/2 * 24* 60 * 60 / 1000


































