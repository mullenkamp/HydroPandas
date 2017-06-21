# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')

#Rainfall barplots
RG = pd.read_csv('S:/KateSt/OTOP/Rainfall/403711.csv', index_col='Year')
RG = RG.replace(r'\s+',np.nan,regex=True)
RG = RG.astype('float')

#pandas/matplotlib ggplot
RGPlot = RG.plot.box()
RGPlot = plt.xlabel('Month')
RGPlot = plt.ylabel('Rainfall (mm)')
RGPlot = plt.title('Rocky Gully')

#seaborn
RGPlot = sns.boxplot(RG)
RGPlot.set(xlabel='Month',ylabel='Rainfall (mm)',title='Rocky Gully')

#plotly
'''
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from plotly.graph_objs import *
init_notebook_mode(connected=True)
plot([{"x":[1,2,3],"y":[3,1,6]}])

import plotly.tools as tls
import plotly.plotly as py
py.sign_in('KateSt','xeet29tqk2')
import cufflinks as cf
import plotly.graph_objs
cf.go_offline()
cf.RG.box(12).plot(kind='box')
RG.plot(kind='box')
'''

#Allocation boxplots
Opuha = pd.read_csv('S:/KateSt/OTOP/Allocation/Opuha River Allocation3.csv',index_col='Year')
OpuhaPlot = Opuha.plot.bar(stacked=True)
OpuhaPlot = plt.xlabel('Year')
OpuhaPlot = plt.ylabel('Allocation (L/s)')
OpuhaPlot = plt.title('Opuha River Allocation')

Opuha2 = pd.read_csv('S:/KateSt/OTOP/Allocation/Opuha River Allocation.csv')
Opuha3 = Opuha2[["Year","Flow","Source"]].groupby(['Year','Source']).sum()
Opuha4 = Opuha3.unstack()
OpuhaPlot2 = Opuha4.plot.area(stacked=True)
OpuhaPlot = plt.xlabel('Year')
OpuhaPlot = plt.ylabel('Allocation (L/s)')
OpuhaPlot = plt.title('Opuha River Allocation')
OpuhaPlot = plt.legend(["Groundwater","Surface water"], loc='upper left', title=None)


