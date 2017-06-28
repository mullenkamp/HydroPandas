# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 11:47:59 2016

@author: MichaelEK
"""

#### Import packages

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from core.misc import printf
from core.allo_use import flow_ros, crc_band_flow, restr_days
from core.stats import lin_reg
from core.ecan_io import rd_hydstra_csv, rd_ts, rd_sql, rd_henry, rd_hydrotel, rd_vcn, rd_hydstra_db
from core.ts.sw import flow_dur, gauge_proc, fre_accrual, malf7d, flow_stats
from core.ts import w_resample, ts_comp
from core.spatial import sel_sites_poly
from core.ts.plot import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot
from geopandas import read_file


######################################
#### Define parameters

### Input
hyd_flow_csv = 'S:/Surface Water/shared/base_data/flow/RAW/flow_set2.csv'
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'
min_sites_shp = 'S:/Surface Water/shared/GIS_base/vector/low_flows/min_flows_sites_Cant.shp'
otop_shp = 'S:/Surface Water/shared/GIS_base/vector/catchments/otop_catchments.shp'

sites = [69603]

## Databases
# Query fields - Be sure to use single quotes for the names!!!
fields1 = ['SiteNo', 'SampleDate', 'Flow']

# Equivelant short names for analyses - Use these names!!!
names1 = ['site', 'date', 'flow']

# Gaugings data
server = 'SQL2012PROD03'
database = 'DataWarehouse'
g_table = 'DataWarehouse.dbo.F_SG_BGauging'

## Output



####################################
#### Basics with flow data

### Flow
## Hydstra

hyd_flow = rd_hydstra_csv(hyd_flow_csv)

# Or even better...directly from the hydstra database for the most recent data

hyd_flow2 = rd_hydstra_db(sites, interval='day')

## read in processed flow data
flow = rd_ts(flow_csv)

# Check the column names
flow.columns

# convert site names from string to integer
flow.columns = flow.columns.astype(int)
flow.columns

# Look at the site Pareora at Huts
flow[70105]

# Run stats for Huts and SH1 on the Pareora
stats1 = flow_stats(flow[sites])
stats1
malf, alf, alf_mising = malf7d(flow[sites])
malf

fdc_70105, fdc_70103 = flow_dur(flow[sites])

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
lfd = restr_days(sites, export=False)

## from new model
ros1 = flow_ros(sites)

# Check that the new model is right
huts = ros1[70105]['pareora_a']
huts_restr = huts[huts < 100]

huts_flow = flow[70105]
huts_flow[huts_restr.index]


















