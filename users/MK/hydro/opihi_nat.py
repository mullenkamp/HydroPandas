# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 11:47:59 2016

@author: MichaelEK
"""

#### Import packages

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat, flow_ros, crc_band_flow, restr_days
from stats_fun import lin_reg
from import_fun import rd_hydstra_csv, rd_ts, rd_sql, rd_henry, rd_hydrotel, rd_vcn
from ts_stats_fun import w_resample, ts_comp, flow_dur, gauge_proc, fre_accrual, malf7d, flow_stats
from spatial_fun import sel_sites_poly
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot
from geopandas import read_file


######################################
#### Define parameters

### Input
hyd_flow_csv = 'S:/Surface Water/shared/base_data/flow/RAW/opihi_flow.csv'
otop_shp = 'S:/Surface Water/shared/GIS_base/vector/catchments/otop_catchments.shp'

## GIS Database

server = 'SQL2012PROD05'
database = 'GIS'
gaugings_table = 'vGAUGING_NZTM'
where_col_gauge = 'SiteNumber'
col_names_gauge = ['SiteNumber']

## Output



####################################
#### Basics with flow data

### Flow
## Hydstra

flow = rd_hydstra_csv(hyd_flow_csv)
flow.columns = flow.columns.astype(int)
flow.columns

## Import gaugings data

gaugings = rd_sql(server, database, gaugings_table, col_names_gauge, where_col_gauge, flow.columns.tolist(), geo_col=True)



# Run stats for Huts and SH1 on the Pareora
stats1 = flow_stats(flow)
stats1
malf, alf, alf_mising = malf7d(flow)
malf

#fdc_70105, fdc_70103 = flow_dur(flow)

# Resample the time series for mean flow for a water year
period = 'water year'
fun = 'mean'

mean_flow_yr = w_resample(flow[sites], period=period, fun=fun)

# Resample the time series for mean flow over a 4 month period
period = 'month'
n_periods = 4

mean_flow_yr = w_resample(flow[sites], period=period, n_periods=n_periods, fun=fun)

##########################################
#### Select sites based on a polygon extent shapefile

otop_sites = sel_sites_poly(otop_shp, min_sites_shp)

# Select only the site ID and the geometry layer
id_col = 'SiteID'
otop_sites_id = otop_sites[[id_col, 'geometry']]
otop_sites_id

# number of sites
len(otop_sites_id)

# Read in a shapefile
otop_catch = read_file(otop_shp)

# Plot the geometry of the shp to verify
otop_catch.plot()

########################################
#### Read in other data

### Databases

gauge1 = rd_sql(server, database, g_table, fields1)
gauge1

# rename columns
gauge1.columns = names1
gauge1.columns

# Round flows to 2 decimal places and put back into dataframe
gauge1['flow'] = gauge1['flow'].round(2)
gauge1

# make the time column a time series data type for the dataframe
time1 = to_datetime(gauge1.date)
gauge1['date'] = time1

# Remove duplicate sites and times
gauge2 = gauge1.groupby(['site','date']).first()
gauge2a = gauge2.reset_index()
gauge2a

# pivot the table to put the stations as the columns
gauge3 = gauge2a.pivot('date', 'site', 'flow')
gauge3

### Or use the custom functions to import various data
# Organized with one column of flow data
gauge3 = rd_henry(sites)
gauge3

# Organized by sites for each columns and as a time series
gauge4 = rd_henry(sites, sites_by_col=True)
gauge4

### VCSN data - precip
# with a polygon shapefile as input
col_id = 'Catchmen_1'
vcsn1, pts1 = rd_vcn(otop_shp, id_col=col_id, data_type='precip', export=False)
vcsn1

# add a 1000 meter buffer around the polygon to get more of the surrounding stations
buffer_dis = 1000
vcsn2, pts2 = rd_vcn(otop_shp, id_col=col_id, data_type='precip', buffer_dis=buffer_dis, export=False)
vcsn2


#######################################
#### Reliability of supply

## from the low flows database
sites = [70105]
lfd = restr_days(sites, export=False)

## from new model
ros1 = flow_ros(sites)

# Check that the new model is right
huts = ros1[70105]['pareora_a']
huts_restr = huts[huts < 100]

huts_flow = flow[70105]
huts_flow[huts_restr.index]


















