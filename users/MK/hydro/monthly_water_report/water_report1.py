# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 16:27:09 2017

@author: MichaelEK
"""

from geopandas import read_file
from core.classes.hydro import hydro, all_mtypes
from datetime import datetime
from pandas import DateOffset, to_datetime, date_range

###################################################
#### Parameters

sw_poly_shp = r'P:\Surface Water Quantity\Projects\Freshwater Report\Water_Report_layers.shp'
swl_points_shp = r'P:\Surface Water Quantity\Projects\Freshwater Report\WL_recorders.shp'

site_col = 'SITENUMBER'

qual_codes = [10, 18, 20, 30, 50, 11, 21, 40]

##################################################
#### Read in data

sw_poly = read_file(sw_poly_shp)
swl_points = read_file(swl_points_shp)

flow1 = hydro().get_data('flow', swl_points[site_col], qual_codes)
precip = hydro().get_data(mtypes='precip', sites=sw_poly_shp, qual_codes=qual_codes)

stats_flow = flow1.stats('flow')
stats_precip = precip.stats('precip')

### Select precip sites with long-ish record
latest1 = stats_precip['End time'].max()
past1 = latest1 - DateOffset(months=6)
all_dates = date_range(start=past1, end=latest1)

precip_sites = stats_precip[(stats_precip['Tot data yrs'] >= 10) & stats_precip['End time'].isin(all_dates)]

precip1 = precip.sel('precip', sites=precip_sites.index)


##################################################
#### Run basic stats

precip_sites_geo = precip1.geo_loc.copy()
precip_data = precip1.data
precip_data.index = precip_data.index.droplevel('mtype')





#################################################
#### Plot

base = sw_poly.plot()
base = precip_sites_geo.plot(ax=base, marker='o', color='red', markersize=4)
base = swl_points.plot(ax=base, marker='o', color='black', markersize=4)




















































































