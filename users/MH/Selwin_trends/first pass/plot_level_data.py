"""
Author: matth
Date Created: 8/03/2017 2:49 PM
"""

from __future__ import division
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from copy import deepcopy


def evn_plots_through_time(plotdata, outpath, inc_5_95 = True):
    x = pd.to_datetime(plotdata.index)
    for key in plotdata.keys():
        plotdata[key] = plotdata[key]/plotdata[key].median()

    fig, ax2 = plt.subplots()
    if inc_5_95:
        plt.plot(x, plotdata.quantile(q=.95,axis=1),'r',label='95th')
    plt.plot(x, plotdata.quantile(q=.75,axis=1),'m',label='75th')
    plt.plot(x, plotdata.quantile(q=.25,axis=1),'g',label='25th')
    if inc_5_95:
        plt.plot(x, plotdata.quantile(q=.05,axis=1),'b',label='5th')
    plt.plot(x, plotdata.mean(axis=1),'k',label='Mean')
    plt.plot(x, plotdata.quantile(q=.50,axis=1),'y',label='Median')
    plt.xticks(pd.date_range(start=str(x.min().year // 5 * 5), end=str((x.max().year // 5 + 1) * 5), freq='5AS'))
    plt.minorticks_on()
    plt.xlabel('Time')
    plt.ylabel('Value / Time Series Median')
    plt.legend()
    ax2.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))
    ax2.xaxis.grid(True, which='minor', alpha=0.2)
    ax2.xaxis.grid(True, which='major', alpha=0.2)

    if outpath is not None:
        plt.savefig(outpath, orientation='landscape',papertype=None,dpi=600)
        plt.close()
    else:
        return fig,ax2
unfilled_data = pd.read_csv(r"P:\Groundwater\Trend_analysis\Data\modified_data\Level\well_water_level.csv", index_col='DMY')
filled_data = pd.read_csv(r"P:\Groundwater\Trend_analysis\Data\modified_data\Level\well_water_level_fully_filled.csv", index_col='DMY')

unfilled_data.index = pd.to_datetime(unfilled_data.index)

well_list = unfilled_data.keys()
x = pd.to_datetime(filled_data.index)
for well in well_list:
    y_filled = filled_data[well][pd.notnull(filled_data[well])]
    y_unfilled = unfilled_data[well][pd.notnull(filled_data[well])]
    x = pd.to_datetime(y_filled.index)
    ax1 = plt.axes()
    plt.subplot(111)

    plt.plot(x, np.array(y_filled),'r',label='Filled Data')
    plt.plot(y_unfilled.index, np.array(y_unfilled),'b', label='Unfilled Data')
    plt.legend()
    plt.xlabel('Time')
    plt.ylabel('Water Level (m)')
    plt.xticks(pd.date_range(start=str(x.min().year//5*5),end=str((x.max().year//5+1)*5),freq='5AS'))
    plt.minorticks_on()
    ax1.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))
    ax1.xaxis.grid(True, which='minor', alpha=0.2)
    ax1.xaxis.grid(True, which='major', alpha=0.2)
    #plt.savefig("P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\unfilled_filled_plots\{}.png".format(well.replace('/','_')),
    #            orientation='landscape', papertype=None, dpi=600)

    plt.close()


#envolope plots of all data on time series
plotdata = deepcopy(unfilled_data[unfilled_data.index >= pd.to_datetime('01/01/1950')])
outpath = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_5-95.png"
evn_plots_through_time(plotdata,outpath)



#export the data for Zeb
outplotdata = pd.DataFrame(index=plotdata.index, columns=['95th', '75th', '50th', 'mean', '25th', '5th'])
outplotdata['95th'] = plotdata.quantile(0.95, axis=1)
outplotdata['75th'] = plotdata.quantile(0.75,axis=1)
outplotdata['50th'] = plotdata.median(axis=1)
outplotdata['25th'] = plotdata.quantile(0.25,axis=1)
outplotdata['5th'] = plotdata.quantile(0.05,axis=1)
outplotdata['mean'] = plotdata.mean(axis=1)
outplotdata['num_of_obs'] = plotdata.count(axis=1)

outdir = "P:/Groundwater/Trend_analysis/Data/modified_data/Level/"
outplotdata.to_csv(outdir + 'normalized_data_selwyn.csv')
outdir = None
# again but with the 5th and 95th removed
outpath = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_25-75.png"
evn_plots_through_time(plotdata,outpath, inc_5_95=False)




#envolope plots of all data by month # could do subplot with groupings
plotdata.index = pd.to_datetime(plotdata.index)
plotdata2 = pd.DataFrame(index=range(1,13), columns=['95th', '75th', '50th', 'mean', '25th', '5th'])
for i in plotdata2.index:
    tempdata = plotdata[plotdata.index.month == i]
    tempdata = tempdata.stack()
    plotdata2.loc[i,'95th'] = tempdata.quantile(0.95)
    plotdata2.loc[i,'75th'] = tempdata.quantile(0.75)
    plotdata2.loc[i,'50th'] = tempdata.median()
    plotdata2.loc[i,'25th'] = tempdata.quantile(0.25)
    plotdata2.loc[i,'5th'] = tempdata.quantile(0.05)
    plotdata2.loc[i,'mean'] = tempdata.mean()
x = plotdata2.index
ax2 = plt.axes()
plt.plot(x, plotdata2['95th'],'r', label='95th')
plt.plot(x, plotdata2['75th'],'m', label='75th')
plt.plot(x, plotdata2['50th'],'y', label='Median')
plt.plot(x, plotdata2['25th'],'g', label='25th')
plt.plot(x, plotdata2['5th'],'b', label='5th')
plt.plot(x, plotdata2['mean'],'k', label='Mean')
plt.xlabel('Months')
plt.ylabel('Value / Time Series Median')
plt.legend()
plt.xticks(range(1,13), ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Nov','Dec'])
ax2.xaxis.grid(True, which='major', alpha=0.2)

plt.savefig("P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_monthly_varience.png",
            orientation='landscape',papertype=None,dpi=600)
plt.close()

# env plots of deseasonalized data
plotdata = pd.read_csv("P:/Groundwater/Trend_analysis/Data/modified_data/Level/seasonal_decomp/trend.csv",index_col='DMY')
plotdata.index = pd.to_datetime(plotdata.index)
plotdata = plotdata[plotdata.index >= pd.to_datetime(01/01/1950)]

outpath = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_deseason_5-95.png"
evn_plots_through_time(plotdata,outpath)
outpath = "P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\overview_of_varience_deseason_25-75.png"
evn_plots_through_time(plotdata,outpath, inc_5_95=False)
