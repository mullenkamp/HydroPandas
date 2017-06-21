"""
Author: matth
Date Created: 8/03/2017 3:20 PM
"""

from __future__ import division
import pandas as pd
import matplotlib.pyplot as plt

enso  = pd.read_table("P:\Groundwater\Trend_analysis\Data\climate\NOAA_best_ENSO_1month.txt",
                      names=['year',1,2,3,4,5,6,7,8,9,10,11,12],
                      delim_whitespace=True)
enso2 = pd.melt(enso,
                    id_vars=['year'],
                    value_vars=[1,2,3,4,5,6,7,8,9,10,11,12],
                    var_name='month',
                    value_name='enso')
enso2['day'] = 15
enso2['DMY'] = pd.to_datetime(enso2[['year','month','day']])
enso2 = enso2.sort_values('DMY')
enso2.to_csv("P:\Groundwater\Trend_analysis\Data\climate\NOAA_best_ENSO.csv")

plotdata = pd.read_csv("P:/Groundwater/Trend_analysis/Data/modified_data/Level/seasonal_decomp/trend.csv",index_col='DMY')
plotdata.index = pd.to_datetime(plotdata.index)
plotdata = plotdata[plotdata.index >= pd.to_datetime(01/01/1950)]

enso2 = enso2[enso2.DMY >= plotdata.index.min()]
inc_5_95 = False
x = pd.to_datetime(plotdata.index)
for key in plotdata.keys():
    plotdata[key] = plotdata[key] / plotdata[key].median()

fig, (ax2, ax3, ax4) = plt.subplots(3)
if inc_5_95:
    ax2.plot(x, plotdata.quantile(q=.95, axis=1), 'r', label='95th')
ax2.plot(x, plotdata.quantile(q=.75, axis=1), 'm', label='75th')
ax2.plot(x, plotdata.quantile(q=.25, axis=1), 'g', label='25th')
if inc_5_95:
    ax2.plot(x, plotdata.quantile(q=.05, axis=1), 'b', label='5th')
ax2.plot(x, plotdata.mean(axis=1), 'k', label='Mean')
ax2.plot(x, plotdata.quantile(q=.50, axis=1), 'y', label='Median')
#ax2.xticks(pd.date_range(start=str(x.min().year // 5 * 5), end=str((x.max().year // 5 + 1) * 5), freq='5AS'))
#ax2.minorticks_on()
#ax2.xlabel('Time')
#ax2.ylabel('Value / Time Series Median')
#ax2.legend()
#ax2.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))
#ax2.xaxis.grid(True, which='minor', alpha=0.2)
#ax2.xaxis.grid(True, which='major', alpha=0.2)

ax3.plot(enso2.DMY,enso2.enso.rolling(12,center=True).mean())

sam = pd.read_table(r"P:\Groundwater\Trend_analysis\Data\climate\SAM_NOAA_CPC_AO.txt",delim_whitespace=True)
sam['day'] = 15
sam['DMY'] = pd.to_datetime(sam[['year','month','day']])

sam2 = sam[sam['DMY']>= plotdata.index.min()]
ax4.plot(sam2.DMY,sam2.sam.rolling(12,center=True).mean())

plt.savefig(r"P:\Groundwater\Trend_analysis\Data\Figures\level_data_figures\deseasonailed_25_75_ENSO_SAM.png", dpi=600)

plt.close()






