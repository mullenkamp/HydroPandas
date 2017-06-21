# -*- coding: utf-8 -*-
"""
Hydrograph plotting example.
"""

from ts_stats_fun import w_resample
from import_fun import rd_ts, rd_hydrotel
from hydro_plot_fun import hydrograph_plot

##########################################
#### Parameters

## Data
flow_site = [71109]
rain_site = [409310, 408401]
flow_period = 'min'
flow_n_periods = 15
precip_period = 'hour'
precip_n_period = 1

## Plotting
start = '2017-01-21 12:00:00'
end = '2017-01-23 12:00:00'
time_format='%d-%m %H:%M'
x_period='hours'
x_n_periods=4
x_grid = True
precip_units='mm/hour'
path = r'C:\ecan\local\Projects\requests\Otematata'
filename = '71109_plot.png'

########################################
#### Load in data
flow1 = rd_hydrotel(flow_site, use_site_name=True)[start:end]
precip1 = rd_hydrotel(rain_site, use_site_name=True, dtype='Rainfall')[start:end]

########################################
#### Resample data as needed
precip = w_resample(precip1, period=precip_period, n_periods=precip_n_period, fun='sum')
flow = w_resample(flow1, period=flow_period, n_periods=flow_n_periods, fun='mean')

########################################
#### Plot hydrograph and precip
plt2 = hydrograph_plot(flow, precip, x_period=x_period, x_n_periods=x_n_periods, time_format=time_format, precip_units=precip_units, x_grid=x_grid, export_path=path, export_name=filename)


