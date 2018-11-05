# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""

import plotly.offline as py
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
import numpy as np
#pd.core.common.is_list_like = pd.api.types.is_list_like
#
#import pandas_datareader.data as web
#
#df = web.DataReader("aapl", 'morningstar',
#                    datetime(2015, 1, 1),
#                    datetime(2016, 7, 1)).reset_index()
#
#data = [go.Scatter(x=df.Date, y=df.High)]
#
#py.iplot(data)


df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv")

data = [go.Scatter(
          x=df.Date,
          y=df['AAPL.Close'])]

py.plot(data)

layout = go.Layout(xaxis = dict(
                   range = ['2017-01-01',
                            '2017-02-01']
    ))

fig = go.Figure(data = data, layout = layout)
py.plot(fig)


layout = dict(
    title='Time Series with Rangeslider',
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1,
                     label='1m',
                     step='month',
                     stepmode='backward'),
                dict(count=6,
                     label='6m',
                     step='month',
                     stepmode='backward'),
                dict(step='all')
            ])
        ),
        rangeslider=dict(
            visible = True
        ),
        type='date'
    )
)

fig = go.Figure(data = data, layout = layout)
py.plot(fig)


mapbox_access_token = 'pk.eyJ1IjoibXVsbGVua2FtcDEiLCJhIjoiY2ptb2RmdXBjMTNibDNxcjJ5ZnplODZ4ZyJ9.ZLNsffO5EjjXsU1uzMQNFA'

data = [
    go.Scattermapbox(
        lat=['45.5017'],
        lon=['-73.5673'],
        mode='markers',
        marker=dict(
            size=14
        ),
        text=['Montreal'],
    )
]

layout = go.Layout(
    autosize=True,
    hovermode='closest',
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=45,
            lon=-73
        ),
        pitch=0,
        zoom=5
    ),
)

fig = dict(data=data, layout=layout)

py.plot(fig, filename=r'E:\ecan\git\HydroPandas\users\MK\plotting\plotly\Montreal-Mapbox.html')

#######################

x = np.random.randn(1000)
y = np.random.randn(1000) + 1

data = [
    go.Histogram2dContour(
        x=x,
        y=y
    )
]

py.plot(data, filename='Example 2D Histogram Contour')










































