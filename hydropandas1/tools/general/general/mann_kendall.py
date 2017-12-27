"""
Author: matth
Date Created: 7/02/2017 12:49 PM
"""

from __future__ import division
import numpy as np
import pandas as pd
from scipy.stats import norm
#todo create a .bat file for lisa to run these.

def mann_kendall_test(x, alpha = 0.05):
    """
    retrieved from https://mail.scipy.org/pipermail/scipy-dev/2016-July/021413.html
    Input:
        x:   a vector of data
        alpha: significance level (0.05 default)

    Output:
        trend: tells the trend (increasing, decreasing or no trend)
        h: True (if trend is present) or False (if trend is absence)
        p: p value of the significance test
        z: normalized test statistics

    Examples
    --------
      >>> x = np.random.rand(100)
      >>> trend,h,p,z = mann_kendall_test(x,0.05)
    """
    n = len(x)

    # calculate S
    s = 0
    for k in range(n-1):
        for j in range(k+1,n):
            s += np.sign(x[j] - x[k])

    # calculate the unique data
    unique_x = np.unique(x)
    g = len(unique_x)

    # calculate the var(s)
    if n == g: # there is no tie
        var_s = (n*(n-1)*(2*n+5))/18
    else: # there are some ties in data
        tp = np.zeros(unique_x.shape)
        for i in range(len(unique_x)):
            tp[i] = sum(unique_x[i] == x)
        var_s = (n*(n-1)*(2*n+5) + np.sum(tp*(tp-1)*(2*tp+5)))/18

    if s>0:
        z = (s - 1)/np.sqrt(var_s)
    elif s == 0:
            z = 0
    elif s<0:
        z = (s + 1)/np.sqrt(var_s)

    # calculate the p_value
    p = 2*(1-norm.cdf(abs(z))) # two tail test
    h = abs(z) > norm.ppf(1-alpha/2)

    if (z<0) and h:
        trend = 'decreasing'
    elif (z>0) and h:
        trend = 'increasing'
    else:
        trend = 'no trend'

    return trend, h, p, z

def _mann_kendall_test(x, alpha = 0.05):
    """
    the duplicate from above is to return more parameters and put into the mann kendall class
    retrieved from https://mail.scipy.org/pipermail/scipy-dev/2016-July/021413.html
    Input:
        x:   a vector of data
        alpha: significance level (0.05 default)

    Output:
        trend: tells the trend (increasing, decreasing or no trend)
        h: True (if trend is present) or False (if trend is absence)
        p: p value of the significance test
        z: normalized test statistics

    Examples
    --------
      >>> x = np.random.rand(100)
      >>> trend,h,p,z = mann_kendall_test(x,0.05)
    """
    x = np.array(x)
    n = len(x)

    # calculate S
    s = 0
    for k in range(n-1):
        for j in range(k+1,n):
            s += np.sign(x[j] - x[k])

    # calculate the unique data
    unique_x = np.unique(x)
    g = len(unique_x)

    # calculate the var(s)
    if n == g: # there is no tie
        var_s = (n*(n-1)*(2*n+5))/18
    else: # there are some ties in data
        tp = np.zeros(unique_x.shape)
        for i in range(len(unique_x)):
            tp[i] = sum(unique_x[i] == x)
        var_s = (n*(n-1)*(2*n+5) + np.sum(tp*(tp-1)*(2*tp+5)))/18

    if s>0:
        z = (s - 1)/np.sqrt(var_s)
    elif s == 0:
            z = 0
    elif s<0:
        z = (s + 1)/np.sqrt(var_s)
    else:
        raise ValueError('shouldnt get here')

    # calculate the p_value
    p = 2*(1-norm.cdf(abs(z))) # two tail test
    h = abs(z) > norm.ppf(1-alpha/2)

    if (z<0) and h:
        trend = 'decreasing'
    elif (z>0) and h:
        trend = 'increasing'
    else:
        trend = 'no trend'

    return trend, h, p, z, s, var_s

class mann_kendall_obj(object):
    "assumes a pandas dataframe or series with a time index"
    def __init__(self, data, alpha=0.05, data_col=None, rm_na = True):
        if rm_na:
            data = data.dropna(how='any')
        if data_col is not None:
            test_data = data[data_col]
        else:
            test_data = data
        test_data = test_data.sort_index()
        self.trend, self.h, self.p, self.z, self.s ,self.var_s = _mann_kendall_test(test_data, alpha=alpha)

class seasonal_kendall (object):
    """
    an object to hold and calculate seasonal kendall trends
    """
    def __init__(self, df, data_col, season_col, alpha=0.05, rm_na = True):
    # todo it would be good to check this object more rigeriously (programatically)
        if rm_na:
            self.data = df.dropna(how='any')
        else:
            self.data = df
        self.data_col = data_col
        self.season_col = season_col
        self.alpha = alpha

        # get list of seasons
        self.season_vals = list(set(np.array(self.data[self.season_col])))
        #todo write some check about data assumptions
        # calulate the seasonal MK values
        self._season_outputs = {}
        self.s = 0
        self.var_s = 0
        for season in self.season_vals:
            tempdata = self.data[data_col][self.data[season_col]==season].sort_index()
            self._season_outputs[season] = mann_kendall_obj(data=tempdata,alpha=self.alpha,rm_na=rm_na)
            self.var_s += self._season_outputs[season].var_s
            self.s += self._season_outputs[season].s

        if self.s >0:
            self.z = (self.s-1)/np.sqrt(self.var_s)
        elif self.s == 0:
            self.z = 0
        elif self.s < 0:
            self.z = (self.s + 1) / np.sqrt(self.var_s)
        else:
            raise ValueError('unexpected error')

        self.p = 2 * (1 - norm.cdf(abs(self.z)))  # two tail test
        self.h = abs(self.z) > norm.ppf(1 - alpha / 2)

        if (self.z < 0) and self.h:
            self.trend = 'decreasing'
        elif (self.z > 0) and self.h:
            self.trend = 'increasing'
        else:
            self.trend = 'no trend'


if __name__ == '__main__':
    data = [
        6.6,
    7.3,
    7.5,
    8.2,
    7.8,
    #np.nan,
    6.8,
    9.6,
    9.3,
    21,
    ]
    print(mann_kendall_test(data))

    test_data = pd.read_excel(r"C:\Users\MattH\Documents\MK_test2.xlsx")
    test = seasonal_kendall(test_data,'Monthly Annual Mean Flow','Month')
    # should be decreasing