# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 9/11/2017 3:41 PM
"""

from __future__ import division
from core import env
import pandas as pd

def rd_pst_parameters(pst_file):
    skiplines_top = 0
    nrows=-1 #to account for the extra that gets generated
    start = False
    stop=False
    with open(pst_file) as f:
        while not stop:
            if start:
                nrows +=1
                if '* observation groups' in f.readline():
                    stop =True
            else:
                skiplines_top +=1
                if '* parameter data' in f.readline():
                    start=True
    data = pd.read_table(pst_file,skiprows=skiplines_top,nrows=nrows,delim_whitespace=True,
                         names=['temp1','temp2','initial','lower','upper','temp3','scale','offset','flag'],
                         index_col=0)

    data.loc[:,'initial'] +=data.loc[:,'offset']
    data.loc[:,'lower'] +=data.loc[:,'offset']
    data.loc[:,'upper'] +=data.loc[:,'offset']
    return data

def param_from_rec(rec_file,series_name):
    skiplines_top = 3
    nrows=-3 #to account for the extra that gets generated
    start = False
    stop=False
    with open(rec_file) as f:
        while not stop:
            if start:
                line = f.readline()
                if 'Model command number' in line:
                    stop = True
                if line =='\n':
                    continue
                nrows +=1
            else:
                skiplines_top +=1
                if 'Parameter definitions:-' in f.readline():
                    start=True
    data = pd.read_table(rec_file,skiprows=skiplines_top,nrows=nrows,delim_whitespace=True,
                         names=['transform','change','initial','lower','upper'],
                         index_col=0)
    out = data.loc[:, 'initial']
    out.name = series_name
    return



if __name__ == '__main__':
    test = param_from_rec(r'C:\Users\MattH\Desktop\from_brioch_9_11_2017\AW_PHILOW_PIEZO\AW_PHILOW_PIEZO\aw_ex_philow_piez.rec')
    print('done')

