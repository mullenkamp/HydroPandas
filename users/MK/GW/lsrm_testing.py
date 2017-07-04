# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 10:42:42 2017

@author: MichaelEK
"""

from core.ecan_io import rd_niwa_vcsn, rd_sql
from core.ts.met.interp import sel_interp_agg
from pandas import merge, concat, to_datetime
from geopandas import read_file, GeoDataFrame, overlay
from core.spatial.vector import xy_to_gpd, points_grid_to_poly
from shapely.geometry import Point, Polygon


#############################################
### Parameters

server1 = 'SQL2012PROD05'
db1 = 'GIS'
tab_irr = 'AQUALINC_NZTM_IRRIGATED_AREA_20160629'
tab_paw = 'vLANDCARE_NZTM_Smap_Consents'

irr_cols = ['type']
irr_cols_rename = {'type': 'irr_type'}
paw_cols = ['WeightAvgPAW']
paw_cols_rename = {'WeightAvgPAW': 'paw'}

grid_res = 1000
crs = 4326

bound_shp = r'S:\Surface Water\backups\MichaelE\Projects\requests\waimak\2017-06-12\waimak_area.shp'

output_precip = r'S:\Surface Water\shared\projects\lsrm\testing\precip_resample1.nc'
output_et = r'S:\Surface Water\shared\projects\lsrm\testing\et_resample1.nc'

###########################################
### Extract data

irr1 = rd_sql(server1, db1, tab_irr, irr_cols, geo_col=True)
irr1.rename(columns=irr_cols_rename, inplace=True)

paw1 = rd_sql(server1, db1, tab_paw, paw_cols, geo_col=True)
paw1.rename(columns=paw_cols_rename, inplace=True)

precip_et = rd_niwa_vcsn(['precip', 'PET'], bound_shp)

bound = read_file(bound_shp).unary_union

##########################################
### Process data
## Resample met data data

new_rain = sel_interp_agg(precip_et, crs, bound_shp, grid_res, 'rain', 'time', 'x', 'y', agg_ts_fun='sum', period='W', output_path=output_precip)

new_et = sel_interp_agg(precip_et, crs, bound_shp, grid_res, 'pe', 'time', 'x', 'y', agg_ts_fun='sum', period='W', output_path=output_et)

new_rain_et = concat([new_rain, new_et], axis=1)

#ds1 = new_rain_et.to_xarray()

## convert new point locations to geopandas
time1 = new_rain_et.index.levels[0][0]
grid1 = new_rain_et.loc[time1].reset_index()[['x', 'y']]
grid2 = xy_to_gpd(grid1.index, 'x', 'y', grid1, crs)
grid2.columns = ['site', 'geometry']

## Convert points to polygons

sites_poly = points_grid_to_poly(grid2, 'site')

## process polygon data
# Select polgons within boundary

irr2 = irr1[irr1.intersects(sites_poly.unary_union)]
irr3 = irr2[irr2.irr_type.notnull() & (irr2.irr_type != 'Unknown')]
paw2 = paw1[paw1.intersects(sites_poly.unary_union)]
paw3 = paw2[paw2.paw.notnull()]

# Overlay intersection

irr4 = overlay(irr3, sites_poly, how='intersection')
paw4 = overlay(paw3, sites_poly, how='intersection')

irr4['area'] = irr4.geometry.area.round()
irr5 = irr4[irr4.area >= 1]

paw4['area'] = paw4.geometry.area.round()
paw5 = paw4[paw4.area >= 1]


















##########################################
### Testing

precip_crs = 4326
grid_res = 1000
data_col = 'rain'
time_col = 'time'
x_col = 'x'
y_col = 'y'


precip = precip_et.copy()
poly = bound_shp
buffer_dis=10000
interp_fun='multiquadric'
agg_ts_fun='sum'
period='W'
digits=3
agg_xy=False
output_format=None


df = precip2.copy()
from_crs=sites.crs
to_crs=poly_crs1


grid1 = new_rain_et.loc[to_datetime('1972-01-02')].reset_index()[['x', 'y']]
grid2 = xy_to_gpd(grid1.index, 'x', 'y', grid1)








