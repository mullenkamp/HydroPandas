# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""

import plotly.offline as py
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyhydrotel import get_mtypes, get_sites_mtypes, get_ts_data
import statsmodels.api as sm
from hydrointerp.util import tsreg
import colorlover as cl

pd.options.display.max_columns = 10

######################################
### Parameters

server = 'sql2012prod05'
database = 'hydrotel'

site = '66401'
mtypes = ['water level', 'water level detided']

from_date = '2018-01-01'
to_date = '2018-03-01'

freq_int = 149

output_path = r'E:\ecan\shared\projects\de-tide\de-tide_2018-10-15.html'

######################################
### Get data

mtypes1 = get_sites_mtypes(server, database, sites=site)

tsdata = get_ts_data(server, database, mtypes, site, from_date, to_date, None)

tsdata1 = tsreg(tsdata.unstack(1).reset_index().drop(['ExtSiteID'], axis=1).set_index('DateTime')).interpolate('time')

roll1 = tsdata1[['water level']].rolling(12, center=True).mean().dropna()
roll1.columns = ['smoothed original']

s1 = sm.tsa.seasonal_decompose(roll1, freq=freq_int, extrapolate_trend=12)

s2 = s1.seasonal.copy()

tsdata2 = roll1[s2 < s2.quantile(0.3)].dropna().copy()

tsdata3 = tsdata2.asfreq('5T').interpolate('pchip')
tsdata3.columns = ['de-tided']

combo1 = pd.concat([roll1, tsdata3, tsdata1['water level detided']], axis=1).dropna()

########################################
###

colors1 = cl.scales['3']['qual']['Set2']

orig = go.Scattergl(
    x=combo1.index,
    y=combo1['smoothed original'],
    name = 'smoothed original',
    line = dict(color = colors1[0]),
    opacity = 0.8)

new_detide = go.Scattergl(
    x=combo1.index,
    y=combo1['de-tided'],
    name = 'de-tided',
    line = dict(color = colors1[1]),
    opacity = 0.8)

old_detide = go.Scattergl(
    x=combo1.index,
    y=combo1['water level detided'],
    name = 'old de-tided',
    line = dict(color = colors1[2]),
    opacity = 0.8)

data = [orig, new_detide, old_detide]

layout = dict(
    title='De-tiding example',
    yaxis={'title':'water level (m)'},
    dragmode='pan')

config = {"displaylogo": False, 'scrollZoom': True, 'showLink': False}

fig = dict(data=data, layout=layout)
py.plot(fig, filename = output_path, config=config)






















