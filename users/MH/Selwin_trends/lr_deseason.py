"""
Author: matth
Date Created: 24/03/2017 8:25 AM
"""

from __future__ import division
from core import env
import pandas as pd
from core.stats import LR

indir = env.sci(r"Groundwater\Trend_analysis\Data\organized data\groundwater\Seasonal_Decomposition\groundwater_1985_std\trend_additive.csv")

data = pd.read_csv(indir, index_col=0)
data.index = pd.to_datetime(data.index)

x = range(len(data.index))
for key in data.keys():
    temp = LR(x,data[key])
    temp.plot()

