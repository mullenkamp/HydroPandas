"""
Author: matth
Date Created: 30/03/2017 4:02 PM
"""

from __future__ import division
from core import env
from sklearn.preprocessing import robust_scale
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import animation

# tutorial: https://jakevdp.github.io/blog/2012/08/18/matplotlib-animation-tutorial/

data_path = env.sci("Groundwater/Trend_analysis/Data/organized data/groundwater/filled_data_by_time_period/filled_month_groundwater_data_1995.csv")
data = pd.read_csv(data_path,index_col=0)

data_norm = data

fig = plt.figure()
ax = plt.axes()
#line, = ax.plot(x,y,color=[]plt.cm.get_cmap('coolwarm'))