# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 10:42:42 2017

@author: MichaelEK
"""

from core.ecan_io import rd_niwa_vcsn, rd_sql
from core.ts.met.interp import poly_interp_agg
from pandas import merge, concat, to_datetime, DataFrame
from geopandas import read_file, GeoDataFrame, overlay
from core.spatial.vector import xy_to_gpd, points_grid_to_poly
from shapely.geometry import Point, Polygon
from numpy import tile, nan, exp, full
from time import time

#############################################
### Parameters

start1 = time()
server1 = 'SQL2012PROD05'
db1 = 'GIS'
tab_irr = 'AQUALINC_NZTM_IRRIGATED_AREA_20160629'
#tab_paw = 'vLANDCARE_NZTM_Smap_Consents'
tab_paw = 'LAND_NZTM_NEWZEALANDFUNDAMENTALSOILS'

irr_cols = ['type']
irr_cols_rename = {'type': 'irr_type'}
#paw_cols = ['WeightAvgPAW']
#paw_cols_rename = {'WeightAvgPAW': 'paw'}

paw_cols = ['PAW_MID']
paw_cols_rename = {'PAW_MID': 'paw'}

rain_name = 'rain'
pet_name = 'pe'

time_agg = 'W'
agg_ts_fun = 'sum'
buffer_dis = 10000
grid_res = 1000
crs = 4326
min_irr_area_ratio = 0.01

irr_mons = [10, 11, 12, 1, 2, 3, 4]

irr_eff_dict = {'Drip/micro': 1, 'Unknown': 0.8, 'dryland': 0, 'Gun': 0.8, 'Pivot': 0.8, 'K-line/Long lateral': 0.8, 'Rotorainer': 0.8, 'Solid set': 0.8}
irr_trig_dict = {'Drip/micro': 0.7, 'Unknown': 0.5, 'dryland': 0, 'Gun': 0.5, 'Pivot': 0.5, 'K-line/Long lateral': 0.5, 'Rotorainer': 0.5, 'Solid set': 0.5}

#bound_shp = r'S:\Surface Water\backups\MichaelE\Projects\requests\waimak\2017-06-12\waimak_area.shp'
bound_shp = r'E:\ecan\shared\projects\lsrm\gis\waipara.shp'

output_precip = r'S:\Surface Water\shared\projects\lsrm\testing\precip_resample1.nc'
output_et = r'S:\Surface Water\shared\projects\lsrm\testing\et_resample1.nc'

output_shp = r'E:\ecan\shared\projects\lsrm\gis\waipara_sites.shp'

##########################################
### Functions

def AET(pet, A, paw_now, paw_max):
    """
    Minhas et al. (1974) function used by David Scott to estimate 'actual ET' from PET and PAW. All inputs must be as floats.
    """

    aet = pet * ((1 - exp(-A*paw_now/paw_max))/(1 - 2*exp(-A) + exp(-A*paw_now/paw_max)))
    return(aet)


###########################################
### Extract data

print('Read in the input data')

irr1 = rd_sql(server1, db1, tab_irr, irr_cols, geo_col=True)
irr1.rename(columns=irr_cols_rename, inplace=True)

paw1 = rd_sql(server1, db1, tab_paw, paw_cols, geo_col=True)
paw1.rename(columns=paw_cols_rename, inplace=True)

precip_et = rd_niwa_vcsn(['precip', 'PET'], bound_shp, buffer_dis=buffer_dis)

bound = read_file(bound_shp)

##########################################
### Process data
## Resample met data data

print('Process the input data for the LSR model')


new_rain = poly_interp_agg(precip_et, crs, bound_shp, rain_name, 'time', 'x', 'y', buffer_dis, grid_res, agg_ts_fun=agg_ts_fun, period=time_agg, output_path=output_precip)

new_et = poly_interp_agg(precip_et, crs, bound_shp, pet_name, 'time', 'x', 'y', buffer_dis, grid_res, agg_ts_fun=agg_ts_fun', period=time_agg, output_path=output_et)

new_rain_et = concat([new_rain, new_et], axis=1)

#ds1 = new_rain_et.to_xarray()

## convert new point locations to geopandas
time1 = new_rain_et.index.levels[0][0]
grid1 = new_rain_et.loc[time1].reset_index()[['x', 'y']]
grid2 = xy_to_gpd(grid1.index, 'x', 'y', grid1, bound.crs)
grid2.columns = ['site', 'geometry']

all_times = new_rain_et.index.levels[0]
new_rain_et.loc[:, 'site'] = tile(grid1.index, len(all_times))

## Convert points to polygons

sites_poly = points_grid_to_poly(grid2, 'site')

## process polygon data
# Select polgons within boundary

irr2 = irr1[irr1.intersects(sites_poly.unary_union)]
irr3 = irr2[irr2.irr_type.notnull()]
paw2 = paw1[paw1.intersects(sites_poly.unary_union)]
paw3 = paw2[paw2.paw.notnull()]

# Overlay intersection

irr4 = overlay(irr3, sites_poly, how='intersection')
paw4 = overlay(paw3, sites_poly, how='intersection')

irr4['area'] = irr4.geometry.area.round()
irr5 = irr4[irr4.area >= 1]

paw4['area'] = paw4.geometry.area.round()
paw5 = paw4[(paw4.area >= 1) & (paw4.paw > 0)]

# Aggregate by site weighted by area
grid_area = grid_res**2
paw_area1 = paw5[['paw', 'site', 'area']].copy()
paw_area1.loc[:, 'paw_area'] = paw_area1['paw'] * paw_area1['area']
paw6 = (paw_area1.groupby('site')['paw_area'].sum() / grid_area).round(1)

site_irr_area = irr5.groupby('site')['area'].sum()
irr_eff1 = irr5.replace({'irr_type': irr_eff_dict})
irr_eff1.loc[:, 'irr_area'] = irr_eff1['irr_type'] * irr_eff1['area']
irr_eff2 = (irr_eff1.groupby('site')['irr_area'].sum() / site_irr_area).round(3)

irr_trig1 = irr5.replace({'irr_type': irr_trig_dict})
irr_trig1.loc[:, 'irr_area'] = irr_trig1['irr_type'] * irr_trig1['area']
irr_trig2 = (irr_trig1.groupby('site')['irr_area'].sum() / site_irr_area).round(3)

irr_area_ratio1 = (site_irr_area/grid_area).round(3)

poly_data1 = concat([paw6, irr_eff2, irr_trig2, irr_area_ratio1], axis=1)
poly_data1.columns = ['paw', 'irr_eff', 'irr_trig', 'irr_area_ratio']
poly_data1.loc[poly_data1['irr_area_ratio'] < min_irr_area_ratio, ['irr_eff', 'irr_trig', 'irr_area_ratio']] = nan

## Combine time series with polygon data

input1 = merge(new_rain_et.reset_index(), poly_data1.reset_index(), on='site', how='left')

## Remove irrigation parameters during non-irrigation times

input1.loc[~input1.time.dt.month.isin(irr_mons), ['irr_eff', 'irr_trig', 'irr_area_ratio']] = nan

## Run checks on the input data

print('Running checks on the prepared input data')

null_time = input1.loc[input1.time.isnull(), 'time']
null_x = input1.loc[input1.x.isnull(), 'x']
null_y = input1.loc[input1.y.isnull(), 'y']
null_pet = input1.loc[input1[pet_name].isnull(), pet_name]
null_rain = input1.loc[input1[rain_name].isnull(), rain_name]
null_paw = input1.loc[input1.paw.isnull(), 'paw']
not_null_irr_eff = input1.loc[input1.irr_eff.notnull(), 'irr_eff']

if not null_time.empty:
    raise ValueError('Null values in the time variable')
if not null_x.empty:
    raise ValueError('Null values in the x variable')
if not null_y.empty:
    raise ValueError('Null values in the y variable')
if not null_pet.empty:
    raise ValueError('Null values in the pet variable')
if not null_rain.empty:
    raise ValueError('Null values in the rain variable')
if not null_paw.empty:
    raise ValueError('Null values in the paw variable')
if not_null_irr_eff.empty:
    raise ValueError('No values for irrigation variables')

if input1['time'].dtype.name != 'datetime64[ns]':
    raise ValueError('time variable must be a datetime64[ns] dtype')
if input1['x'].dtype != float:
    raise ValueError('x variable must be a float dtype')
if input1['y'].dtype != float:
    raise ValueError('y variable must be a float dtype')
if input1[pet_name].dtype != float:
    raise ValueError('pet variable must be a float dtype')
if input1[rain_name].dtype != float:
    raise ValueError('rain variable must be a float dtype')
if input1['paw'].dtype != float:
    raise ValueError('paw variable must be a float dtype')
if input1['irr_eff'].dtype != float:
    raise ValueError('irr_eff variable must be a float dtype')
if input1['irr_trig'].dtype != float:
    raise ValueError('irr_trig variable must be a float dtype')
if input1['irr_area_ratio'].dtype != float:
    raise ValueError('irr_area_ratio variable must be a float dtype')




end1 = time()
end1 - start1

#########################################################
### Run the model

print('Run the LSR model')

paw_val = poly_data1['paw'].values
irr_eff_val = poly_data1['irr_eff'].values
irr_trig_val = poly_data1['irr_trig'].values
irr_area_ratio_val = poly_data1['irr_area_ratio']
irr_area_ratio_val[irr_area_ratio_val.isnull()] = 0

irr_paw_val = paw_val * irr_area_ratio_val.values
non_irr_paw_val = paw_val - irr_paw_val

grp1 = input1[['time', rain_name, pet_name]].groupby('time')

for name, group in grp1:
    print(name)
    print(group)


non_irr_w = paw_val

group = grp1.get_group(time1)

rain1 = group[rain_name].values
pet1 = group[pet_name].values


non_irr_w1 = non_irr_w + rain1 - pet1 - 100


new_w1[new_w1 < 0] = 0
paw_ratio = new_w1/group['paw'].values
irr_trig_bool = paw_ratio <= group['irr_trig'].values
if any(irr_trig_bool):
    diff_paw = (group['paw'].values - new_w1) * group['irr_area_ratio'].values
    irr_drainage = diff_paw/group['irr_eff'].values - diff_paw








##########################################
### Testing

end1 = time()
end1 - start1


%timeit diff_paw * (1/group['irr_eff'].values - 1)
%timeit diff_paw/group['irr_eff'].values - diff_paw

precip_crs = 4326
grid_res = 1000
data_col = 'rain'
time_col = 'time'
x_col = 'x'
y_col = 'y'


precip = precip_et.copy()
poly = bound_shp
buffer_dis=10000
interp_fun='cubic'
agg_ts_fun='sum'
period='W'
digits=2
agg_xy=False
output_format=None

gpd = grid2.copy()
id_col = 'site'

df = precip2.copy()
from_crs=sites.crs
to_crs=poly_crs1


grid1 = new_rain_et.loc[to_datetime('1972-01-02')].reset_index()[['x', 'y']]
grid2 = xy_to_gpd(grid1.index, 'x', 'y', grid1)



grid2.to_file(output_shp)

output_shp1 = r'E:\ecan\shared\projects\lsrm\gis\sites1.shp'

sites.to_file(output_shp1)

output_shp2 = r'E:\ecan\shared\projects\lsrm\gis\sites_poly.shp'

sites_poly.to_file(output_shp2)






