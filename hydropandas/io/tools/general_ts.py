# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 09:07:24 2018

@author: MichaelEK
"""
import pandas as pd
from hydropandas.tools.general.ts.misc import tsreg


def rd_ts(csv, index=1, header='infer', skiprows=0, reg=False, **kwargs):
    """
    Simple function to read in time series data and make it regular if needed.
    """

    ts = pd.read_csv(csv, parse_dates=[index - 1], infer_datetime_format=True, index_col=0, dayfirst=True,
                  skiprows=skiprows, header=header)
    if reg:
        ts = tsreg(ts, **kwargs)
    return ts








