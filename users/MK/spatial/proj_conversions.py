# -*- coding: utf-8 -*-
"""
This script provides functions and example for imprting geographic data and projections manipulations in python.
"""

from geopandas import read_file, GeoDataFrame
from shapely.wkt import loads
from shapely.geometry import Point
from pyproj import Proj, transform
from spatial_fun import xy_to_gpd
from import_fun import rd_sql
from pycrs.parser import from_epsg_code
from pandas import read_csv, read_sql
from pymssql import connect

###################################
#### Parameters

from_epsg = 2193
to_epsg = 4326
shp_in = 'C:/ecan/shared/GIS_base/vector/hydro_sites/all_rec_loc.shp'
shp_out = 'C:/ecan/shared/GIS_base/vector/testing/test1.shp'

db_code = 'hydrotel_rain_gauges_gis'
data_source_csv = 'S:/Surface Water/shared/base_data/database/ecan_sql_data_sources.csv'

##################################
#### Create an object with geometry

### Read in external data

## Load in a shapefile
gpd1 = read_file(shp_in)

##Load in from MSSQL database
gpd2 = rd_sql(code=db_code)

### From x and y points

## Convert geometry to x and y values
x = gpd2.geometry.apply(lambda p: p.x).values
y = gpd2.geometry.apply(lambda p: p.y).values

## Ready-made function from dataframe
gpd3 = xy_to_gpd(gpd2, 'site', x, y, epsg=from_epsg)

## Lower-level way by first creating the x and y to shapely geometry
geometry = [Point(xy) for xy in zip(x, y)]
gpd4 = GeoDataFrame(gpd2['site'], geometry=geometry, crs=from_epsg_code(from_epsg).to_proj4())

### From WKT geometry format (for lines and polygons)

## Load in example data from database
dbs = read_csv(data_source_csv)
dbs_code = dbs[dbs.Code == 'cwms_gis']
server = dbs_code.Server.values[0]
database = dbs_code.Database.values[0]
table = dbs_code.Table.values[0]
col_names = dbs_code.db_fields.values[0].split(', ')
rename_cols = dbs_code.rename_fields.values[0].split(', ')
geo_col = dbs_code.geo_col.values[0]
conn = connect(server, database=database)

geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table

df1 = read_sql(stmt2, conn)
df1.columns = ['geometry']

index1 = df1.index.values

## Convert the really long WKT string to a shapely geometry
geometry = [loads(i) for i in df1.geometry]
gpd5 = GeoDataFrame(index1, geometry=geometry, crs=from_epsg_code(from_epsg).to_proj4())

########################################
#### Convert between projections

### Convert between projection format names

## epsg to proj4 (needed for many packages and formats)
from_proj4 = from_epsg_code(from_epsg).to_proj4()
to_proj4 = from_epsg_code(to_epsg).to_proj4()

### Convert x and y values directly from one projection to another using pyproj
from_proj = Proj(from_proj4)
to_proj = Proj(to_proj4)

to_x, to_y = transform(from_proj, to_proj, x, y)

### Using geopandas to reproject the entire object
gpd6 = gpd2.to_crs(to_proj4)


########################################
#### Export objects

### Shapefiles
gpd2.to_file(shp_out)






















