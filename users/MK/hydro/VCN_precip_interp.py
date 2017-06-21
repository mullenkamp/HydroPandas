# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from pandas import read_table, to_datetime, DataFrame, read_csv, concat, Series
from scipy import interpolate
import matplotlib.pyplot as plt
from numpy import arange, meshgrid, insert
from import_fun import rd_vcn, rd_sql
from geopandas import read_file, GeoDataFrame
from geopandas.tools import sjoin
from time import time
from spatial_fun import grid_interp_iter, catch_net, pts_poly_join, catch_agg, grid_catch_agg

######################################
### Parameters

#vcn_grid_shp = 'C:/ecan/local/Projects/otop/GIS/vector/precip/vcsn_grid_otop.shp'
select = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'
catch_sites_csv = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/results/catch_sites.csv'

## Other data

buffer_dis = 10000
grid_res = 0.005
epsg = 4326
period = 'water year'

poly_id_col = 'GRIDCODE'


#precip_csv = 'C:/ecan/local/Projects/otop/GIS/tables/vcsn_otop.csv'
vcn_grid_cols = ['Data_VCN_s', 'geometry']
vcn_grid_names = ['site', 'geometry']

site_loc_csv = 'C:/ecan/local/Projects/otop/GIS/tables/vcsn_otop.csv'
site_loc_col = ['DATA_VCN_S', 'DATA_VCN_1', 'DATA_VCN_2']
site_loc_col2 = ['site', 'x', 'y']
save_path1 = 'C:/ecan/GIS_base/tables/otop_precip2.csv'

####################################
#### Query the VCSN data

df1, gpd = rd_vcn(select, buffer_dis=buffer_dis)

df2, gpd2 = grid_interp_iter(df1, gpd, grid_res, period, epsg)

poly = read_file(select)

#agg_precip, catch_areas = grid_catch_agg(df2, poly, gpd2, poly_id_col, catch_sites_csv)

pts_poly, poly2 = pts_poly_join(gpd2, poly, poly_id_col)
pts_poly2 = pts_poly['id']

df3 = df2[pts_poly2.index]
site_precip = df3.groupby(pts_poly2.values, axis=1).mean()
sites, singles = catch_net(catch_sites_csv)

agg_precip, catch_areas = catch_agg(sites, site_precip, poly2, agg_fun='mean')











#####################################
### Read in data

vcn_grid = read_file(vcn_grid_shp)[vcn_grid_cols]
vcn_grid.columns = vcn_grid_names

sites = read_csv(site_loc_csv, usecols=site_loc_col)
sites.columns = site_loc_col2
sites.index = sites.site.astype('int')
sites2 = sites[site_loc_col2[1:3]]

data = rd_vcn(select=sites.site.values, export=False)

### Reorganize data into spatial matrix

max_x = sites.x.max()
min_x = sites.x.min()

max_y = sites.y.max()
min_y = sites.y.min()

range_x = sites.x.sort_values().unique()
range_y = sites.y.sort_values().unique()

t1 = data.transpose()
t1.index = t1.index.astype('int')

t2 = concat([sites2, t1], axis=1)

x = t2.x.values
y = t2.y.values

new_x = arange(min_x, max_x + res, res)
new_y = arange(min_y, max_y + res, res)

z = t2.iloc[:, 4].values


###########################################
### Interpolate

interp1 = interpolate.Rbf(x, y, z, function='linear')

x_int, y_int = meshgrid(new_x, new_y)

z_int = interp1(x_int, y_int).round(2)

z_int[z_int < 0] = 0

plt.plot(x,y, 'ko')
#plt.pcolor(x, y, z)
plt.imshow(z_int[::-1], extent=[min_x, max_x, min_y, max_y], cmap='gist_earth')
plt.colorbar()


##########################################
### Export data

x_int2 = x_int.flatten()
y_int2 = y_int.flatten()
z_int2 = z_int.flatten()

df1 = DataFrame({'x': x_int2, 'y': y_int2[::-1], 'z': z_int2})

df1.to_csv(save_path1, index=False)





df1[df1.z > 0]
df2[df2.z > 0]



import numpy as np
import scipy.interpolate
import matplotlib.pyplot as plt
np.random.seed(1977)

x, y, z = np.random.random((3, 10))

interp = scipy.interpolate.Rbf(x, y, z, function='linear')

yi, xi = np.mgrid[0:1:100j, 0:1:100j]

zi = interp(xi, yi)

plt.plot(x, y, 'ko')
plt.imshow(zi, extent=[0, 1, 1, 0], cmap='gist_earth')
plt.colorbar()

plt.show()

t2.iloc[:, 4]

shp1 = 'C:/ecan/local/Projects/otop/temp/test1.shp'


x1 = vcn_grid3.apply(lambda p: p.x)
y1 = vcn_grid3.apply(lambda p: p.y)
gdf1 = concat([vcn_grid3, t1], axis=1)

start = time()
x1 = vcn_grid3.apply(lambda p: p.x)
y1 = vcn_grid3.apply(lambda p: p.y)
end = time()
print(end - start)

out1 = GeoDataFrame(concat([df1.ix[5, :], gpd], axis=1), geometry='geometry')
out1.columns = ['1972-01-07', 'geometry']
out1.to_file('C:/ecan/local/Projects/otop/GIS/vector/min_flow/test1.shp')

gs.to_file('C:/ecan/local/Projects/otop/GIS/vector/min_flow/test2.shp')
gpd2.to_file('C:/ecan/local/Projects/otop/GIS/vector/min_flow/test3.shp')
gpd = vcn_grid3


def grid_interp_iter(df1, gpd, grid_res, epsg=None, period, n_periods):
    """

    """
    from numpy import arange, meshgrid
    from shapely.geometry import Point
    from geopandas import GeoDataFrame, GeoSeries
    from ts_stats_fun import w_resample

    #### Create the grids
    if epsg is None:
        x = gpd.apply(lambda p: p.x).values
        y = gpd.apply(lambda p: p.y).values
    else:
        gpd1 = gpd.to_crs(epsg=epsg)
        x = gpd1.apply(lambda p: p.x).round(3).values
        y = gpd1.apply(lambda p: p.y).round(3).values

    max_x = x.max()
    min_x = x.min()

    max_y = y.max()
    min_y = y.min()

    new_x = arange(min_x, max_x, grid_res)
    new_y = arange(min_y, max_y, grid_res)
    x_int, y_int = meshgrid(new_x, new_y)

    ## Convert new x and y to geopandas
    x_int2 = x_int.flatten()
    y_int2 = y_int.flatten()
    geometry = [Point(xy) for xy in zip(x_int2, y_int2)]
    gpd_new = GeoDataFrame(geometry=geometry)

    #### Resample the time series data
    df2 = w_resample(df1, period, n_periods, fun, digits=2)



    z = df1.ix[5, :].values


def grid_resample(x, y, z, x_int, y_int, method='multiquadric'):
    from scipy.interpolate import Rbf

    interp1 = Rbf(x, y, z, function=method)
    z_int = interp1(x_int, y_int).round(2)
    z_int[z_int < 0] = 0

    z_int2 = z_int.flatten()
    return(z_int2)




x2 = gs.geometry.apply(lambda p: p.x).round(3).values
y2 = gs.geometry.apply(lambda p: p.y).round(3).values
#z2 = gs.geometry.apply(lambda p: p.z).values


xi = linspace(min(x2), max(x2))
yi = linspace(min(y2), max(y2))

z3 = griddata(x2,y2,z_int2, xi, yi, interp='linear')

plt.plot(x,y, 'ko')
plt.contour(xi,yi,z3)
plt.colorbar()


plt.contour





