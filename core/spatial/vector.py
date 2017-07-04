# -*- coding: utf-8 -*-
"""
Vector processing functions.
"""

#########################################
### Dictionaries

proj4_netcdf_var = {'x_0': 'false_easting', 'y_0': 'false_northing', 'f': 'inverse_flattening', 'lat_0': 'latitude_of_projection_origin', 'lon_0': ('longitude_of_central_meridian', 'longitude_of_projection_origin'), 'pm': 'longitude_of_prime_meridian', 'k_0': ('scale_factor_at_central_meridian', 'scale_factor_at_projection_origin'), 'a': 'semi_major_axis', 'b': 'semi_minor_axis', 'lat_1': 'standard_parallel', 'proj': 'transform_name'}

proj4_netcdf_name = {'aea': 'albers_conical_equal_area', 'tmerc': 'transverse_mercator', 'aeqd': 'azimuthal_equidistant', 'laea': 'lambert_azimuthal_equal_area', 'lcc': 'lambert_conformal_conic', 'cea': 'lambert_cylindrical_equal_area', 'longlat': 'latitude_longitude', 'merc': 'mercator', 'ortho': 'orthographic', 'ups': 'polar_stereographic', 'stere': 'stereographic', 'geos': 'vertical_perspective'}


#########################################
### Functions


def sel_sites_poly(pts, poly, buffer_dis=0):
    """
    Simple function to select points within a single polygon. Optional buffer.

    Aguments:\n
    pts -- A GeoDataFrame of pounts with the site names as the index. Or a shapefile with the first column as the site names.\n
    poly -- A GeoDataFrame of polygons with the site names as the index. Or a shapefile with the first column as the site names.\n
    buffer_dis -- Distance in coordinate system units for a buffer around the polygon.
    """
    from geopandas import read_file, GeoDataFrame

    #### Read in data
    if isinstance(pts, GeoDataFrame):
        gdf_pts = pts.copy()
    elif isinstance(pts, str):
        if pts.endswith('.shp'):
            gdf_pts = read_file(pts).copy()
            gdf_pts.set_index(gdf_pts.columns[0], inplace=True)
    if isinstance(poly, GeoDataFrame):
        gdf_poly = poly.copy()
    elif isinstance(poly, str):
        if poly.endswith('.shp'):
            gdf_poly = read_file(poly).copy()
            gdf_poly.set_index(gdf_poly.columns[0], inplace=True)

    #### Perform vector operations for initial processing
    ## Dissolve polygons by id
    poly2 = gdf_poly.unary_union

    ## Create buffer
    poly_buff = poly2.buffer(buffer_dis)

    ## Select only the vcn sites within the buffer
    points2 = gdf_pts[gdf_pts.within(poly_buff)]

    return(points2)


def pts_poly_join(pts, poly, poly_id_col):
    """
    Simple function to join the attributes of the polygon to the points. Specifically for an ID field in the polygon.
    """
    from geopandas.tools import sjoin

    poly2 = poly[[poly_id_col, 'geometry']]
    poly3 = poly2.dissolve(by=poly_id_col)

    join1 = sjoin(pts, poly3, how='inner', op='within')
    join1.rename(columns={join1.columns[-1]: poly_id_col}, inplace=True)

    return([join1, poly3])


def pts_sql_join(pts, sql_codes):
    """
    Function to perform spatial joins on sql tables that are polygon layers.
    """
    from core.ecan_io import rd_sql
    from geopandas.tools import sjoin

    for i in sql_codes:
        poly = rd_sql(code=i)
        pts = sjoin(pts, poly, how='left', op='within').drop('index_right', axis=1)

    return(pts)


def precip_catch_agg(sites, site_precip, id_area, agg_fun='mean'):
    """
    Function to aggregate time series of catchments into their all of their upstream catchments.
    """
    from numpy import insert, in1d
    from pandas import concat

#    n_sites = len(sites) + len(singles)
#    if n_sites != len(site_precip.columns):
#        print("Site numbers between data sets are not the same!")
    output = site_precip.copy()

    id_area2 = id_area.area
    area_out = concat([id_area2, id_area2], axis=1)
    area_out.columns = ['id_area', 'tot_area']
    site_precip2 = site_precip.mul(id_area2.values.flatten(), axis=1)

    for i in sites.index:
        set1 = insert(sites.loc[i,:].dropna().values, 0, i).astype(int)
        tot_area = int(id_area2[in1d(id_area2.index, set1)].sum())
        output.loc[:, i] = (site_precip2[set1].sum(axis=1)/tot_area).values
        area_out.loc[i, 'tot_area'] = tot_area

    return([output.round(2), area_out.round()])


def xy_to_gpd(id_col, x_col, y_col, df=None, crs=2193):
    """
    Function to convert a DataFrame with x and y coordinates to a GeoDataFrame.

    Arguments:\n
    df -- Dataframe
    id_col -- the column(s) from the dataframe to be returned. Either a one name string or a list of column names.\n
    xcol -- Either the column name that has the x values within the df or an array of x values.\n
    ycol -- Same as xcol.\n
    crs -- The projection of the data.
    """
    from shapely.geometry import Point
    from geopandas import GeoDataFrame
    from core.misc import select_sites
    from pandas import Series, Index
    from numpy import ndarray
    from core.spatial import convert_crs

    if type(x_col) is str:
        geometry = [Point(xy) for xy in zip(df[x_col], df[y_col])]
    else:
        x1 = select_sites(x_col)
        y1 = select_sites(y_col)
        geometry = [Point(xy) for xy in zip(x1, y1)]
    if isinstance(id_col, str):
        id_data = df[id_col]
    elif isinstance(id_col, (list, ndarray, Series, Index)):
        id_data = id_col
    gpd = GeoDataFrame(id_data, geometry=geometry, crs=convert_crs(crs))
    return(gpd)


def flow_sites_to_shp(sites='All', min_flow_only=False, export=False, export_path='sites.shp'):
    """
    Function to create a geopandas/shapefile from flow sites.
    """
    from core.spatial import sel_sites_poly
    from geopandas import read_file
    from numpy import in1d
    from core.misc import select_sites
    from core.ecan_io import rd_sql

    ### Import from databases
    if min_flow_only:
        min_flow_sites = rd_sql('SQL2012PROD05', 'Wells', '"vMinimumFlowSites+Consent+Well_classes"', col_names=['RefDbase', 'RefDbaseKey', 'restrictionType', 'RecordNo', 'WellNo'], where_col='RefDbase', where_val=['Gauging', 'Hydrotel'])
        min_flow_sites.columns = ['type', 'site', 'restr', 'crc', 'wap']
        min_flow_sites['site'] = min_flow_sites['site'].astype(int)
        min_flow_sites = min_flow_sites[min_flow_sites.restr == 'LowFlow']

    site_geo = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'], geo_col=True)
    site_geo.columns = ['site', 'river', 'site_name', 'geometry']
    site_geo['river'] = site_geo.river.apply(lambda x: x.title())
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.title())
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace(' (Recorder)', ''))
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Sh', 'SH'))
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Ecs', 'ECS'))

    ### Select sites
    if type(sites) is str:
        if sites is 'All':
            sites_sel_geo = site_geo
        elif sites.endswith('.shp'):
            poly = read_file(sites)
            sites_sel_geo = sel_sites_poly(poly, site_geo)
    else:
        sites_sel = select_sites(sites).astype('int32')
        sites_sel_geo = site_geo[in1d(site_geo.site, sites_sel)]
    if min_flow_only:
        sites_sel_geo = sites_sel_geo[in1d(sites_sel_geo.site, min_flow_sites.site.values)]

    ### Export and return
    if export:
        sites_sel_geo.to_file(export_path)
    return(sites_sel_geo)


def convert_crs(from_crs, crs_type='proj4', pass_str=False):
    """
    Function to convert one crs format to another.

    from_crs -- The crs as either an epsg number or a str in a common crs format (e.g. proj4 or wkt).\n
    crs_type -- Output format type of the crs ('proj4', 'wkt', 'proj4_dict', or 'netcdf_dict').\n
    pass_str -- If input is a str, should it be passed though without conversion?
    """
    from pycrs import parser
    from pandas import to_numeric

    ### Load in crs
    if all([pass_str, isinstance(from_crs, str)]):
        crs2 = from_crs
    else:
        if isinstance(from_crs, int):
            crs1 = parser.from_epsg_code(from_crs)
        elif isinstance(from_crs, str):
            crs1 = parser.from_unknown_text(from_crs)

        ### Convert to standard formats
        if crs_type == 'proj4':
            crs2 = crs1.to_proj4()
        elif crs_type == 'wkt':
            crs2 = crs1.to_ogc_wkt()
        elif crs_type in ['proj4_dict', 'netcdf_dict']:
            crs1a = crs1.to_proj4()
            crs1b = crs1a.replace('+', '').split()[:-1]
            crs1c = dict(i.split('=') for i in crs1b)
            crs2 = dict((i, to_numeric(crs1c[i], 'ignore')) for i in crs1c)
        else:
            raise ValueError('Select one of "proj4", "wkt", "proj4_dict", or "netcdf_dict"')
        if crs_type == 'netcdf_dict':
            crs3 = {}
            for i in crs2:
                if i in proj4_netcdf_var.keys():
                    t1 = proj4_netcdf_var[i]
                    if isinstance(t1, tuple):
                        crs3.update({j: crs2[i] for j in t1})
                    else:
                        crs3.update({proj4_netcdf_var[i]: crs2[i]})
    #        crs2 = {proj4_netcdf_var[i]: crs2[i] for i in crs2 if i in proj4_netcdf_var.keys()}
            if crs3['transform_name'] in proj4_netcdf_name.keys():
                gmn = proj4_netcdf_name[crs3['transform_name']]
                crs3.update({'transform_name': gmn})
            else:
                raise ValueError('No appropriate netcdf projection.')
            crs2 = crs3

    return(crs2)


def point_to_poly_apply(geo, side_len):
    """
    Function to convert a point to a square polygon. Input is a shapely point. Output is a shapely polygon.

    geo -- A shapely point.\n
    side_len -- The side length of the square (in the units of geo).
    """
    from shapely.geometry import Polygon

    half_side = side_len * 0.5
    l1 = Polygon([[geo.x + half_side, geo.y + half_side], [geo.x + half_side, geo.y - half_side], [geo.x - half_side, geo.y - half_side], [geo.x - half_side, geo.y + half_side]])
    return(l1)


def points_grid_to_poly(gpd, id_col):
    """
    Function to convert a GeoDataFrame of evenly spaced gridded points to square polygons. Output is a GeoDataFrame of the same length as input.

    gpd -- GeoDataFrame of gridded points with an id column.\n
    id_col -- The id column name.
    """
    from geopandas import GeoDataFrame
    from core.spatial.vector import point_to_poly_apply
    from pandas import Series

    geo1a = Series(gpd.geometry.apply(lambda j: j.x))
    geo1b = geo1a.shift()

    side_len = (geo1b - geo1a).abs().min()
    gpd1 = gpd.apply(lambda j: point_to_poly_apply(j.geometry, side_len=side_len), axis=1)
    gpd2 = GeoDataFrame(gpd[id_col], geometry=gpd1, crs=gpd.crs)
    return(gpd2)







