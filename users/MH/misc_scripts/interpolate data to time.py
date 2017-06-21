"""
quick interpolation to time script, but there are probably better ways of doing this
Author: matth
Date Created: 9/02/2017 12:36 PM
"""

from __future__ import division

import numpy as np
import scipy.interpolate as int
import pandas as pd
import time
import datetime as dt


def inter_data_to_time (data, time_name, data_name, outpath, time_format="%d/%m/%Y %H:%M", new_freq='T'):
    """
    interpolates time data to standard grid and saves as CSV
    :param data: pandas dataframe with orignial data
    :param time_name: the string to access the time data
    :param data_name: the string to access the data to be interpolated
    :param outpath: path of saved csv
    :param time_format: string (as datetime) for time format
    :param new_freq: frequency code (from pd.date_range)
    :return: nothing
    """

    datetimes = [dt.datetime.strptime(e,time_format) for e in data[time_name]]
    datetimes2 = [time.mktime(e.timetuple()) for e in datetimes]
    data_val = np.array(data[data_name])

    inter = int.interp1d(datetimes2,data_val)

    timeout = list(pd.date_range(min(datetimes),max(datetimes),freq=new_freq))
    timeout2 = np.array([time.mktime(e.timetuple()) for e in timeout])

    data_out = inter(timeout2)

    out = pd.DataFrame(data={time_name: timeout, data_name: data_out})

    out.to_csv(outpath)

if __name__ == '__main__':
    path = r"P:\Groundwater\Matt_Hanson\Advice\2017_01_31 pdp a review\M35_5144_Baro.CSV"
    data = pd.read_csv(path, names=['time', 'baro'], skiprows=4)
    outpath = r"P:\Groundwater\Matt_Hanson\Advice\2017_01_31 pdp a review\M35_5144_Baro_interpolated.CSV"
    inter_data_to_time(data,'time','baro',outpath)

