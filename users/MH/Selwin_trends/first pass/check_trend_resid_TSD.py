"""
Author: matth
Date Created: 14/03/2017 11:37 AM
"""

from __future__ import division
import pandas as pd
from plot_level_data import evn_plots_through_time

trend = pd.read_csv(r"P:\Groundwater\Trend_analysis\Data\modified_data\Level\seasonal_decomp\trend.csv",
                    index_col='DMY')
org_data = pd.read_csv(r"P:\Groundwater\Trend_analysis\Data\modified_data\Level\well_water_level_fully_filled.csv",
                       index_col='DMY')
outpath = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_non-trend_5-95.png"
outpath2 = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_non-trend_25-75.png"
plotdata = org_data-trend
plotdata.index = pd.to_datetime(plotdata.index)
plotdata = plotdata[plotdata.index >= pd.to_datetime(01/01/1950)]
evn_plots_through_time(plotdata,outpath)
evn_plots_through_time(plotdata,outpath2, inc_5_95=False)
