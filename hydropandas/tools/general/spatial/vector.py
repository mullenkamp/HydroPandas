# -*- coding: utf-8 -*-
"""
Vector processing functions.
"""
import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin
from pycrs import parser
from shapely.geometry import Point, Polygon
from hydropandas.util.misc import select_sites
from hydropandas.io.tools.mssql import rd_sql


#########################################
### Dictionaries

proj4_netcdf_var = {'x_0': 'false_easting', 'y_0': 'false_northing', 'f': 'inverse_flattening',
                    'lat_0': 'latitude_of_projection_origin',
                    'lon_0': ('longitude_of_central_meridian', 'longitude_of_projection_origin'),
                    'pm': 'longitude_of_prime_meridian',
                    'k_0': ('scale_factor_at_central_meridian', 'scale_factor_at_projection_origin'),
                    'a': 'semi_major_axis', 'b': 'semi_minor_axis', 'lat_1': 'standard_parallel',
                    'proj': 'transform_name'}

proj4_netcdf_name = {'aea': 'albers_conical_equal_area', 'tmerc': 'transverse_mercator',
                     'aeqd': 'azimuthal_equidistant', 'laea': 'lambert_azimuthal_equal_area',
                     'lcc': 'lambert_conformal_conic', 'cea': 'lambert_cylindrical_equal_area',
                     'longlat': 'latitude_longitude', 'merc': 'mercator', 'ortho': 'orthographic',
                     'ups': 'polar_stereographic', 'stere': 'stereographic', 'geos': 'vertical_perspective'}


#########################################
### Functions


def sel_sites_poly(pts, poly, buffer_dis=0):
    """
    Simple function to select points within a single polygon. Optional buffer.

    Parameters
    ----------
    pts: GeoDataFrame or str
        A GeoDataFrame of points with the site names as the index. Or a shapefile with the first column as the site names.
    poly: GeoDataFrame or str
        A GeoDataFrame of polygons with the site names as the index. Or a shapefile with the first column as the site names.
    buffer_dis: int
        Distance in coordinate system units for a buffer around the polygon.

    Returns
    -------
    GeoDataFrame
        Of points.
    """

    #### Read in data
    if isinstance(pts, (gpd.GeoDataFrame, gpd.GeoSeries)):
        gdf_pts = pts.copy()
    elif isinstance(pts, str):
        if pts.endswith('.shp'):
            gdf_pts = gpd.read_file(pts).copy()
            col1 = gdf_pts.columns.drop('geometry')[0]
            gdf_pts.set_index(col1, inplace=True)
        else:
            raise ValueError('pts must be a GeoDataFrame, GeoSeries, or a str path to a shapefile')
    else:
        raise ValueError('pts must be a GeoDataFrame, GeoSeries, or a str path to a shapefile')
    if isinstance(poly, (gpd.GeoDataFrame, gpd.GeoSeries)):
        gdf_poly = poly.copy()
    elif isinstance(poly, str):
        if poly.endswith('.shp'):
            gdf_poly = gpd.read_file(poly).copy()
            col2 = gdf_poly.columns.drop('geometry')[0]
            gdf_poly.set_index(col2, inplace=True)
        else:
            raise ValueError('poly must be a GeoDataFrame, GeoSeries, or a str path to a shapefile')
    else:
        raise ValueError('poly must be a GeoDataFrame, GeoSeries, or a str path to a shapefile')

    #### Perform vector operations for initial processing
    ## Dissolve polygons by id
    poly2 = gdf_poly.unary_union

    ## Create buffer
    poly_buff = poly2.buffer(buffer_dis)

    ## Select only the vcn sites within the buffer
    points2 = gdf_pts[gdf_pts.within(poly_buff)]

    return points2


def pts_poly_join(pts, poly, poly_id_col):
    """
    Simple function to join the attributes of the polygon to the points. Specifically for an ID field in the polygon.

    Parameters
    ----------
    pts: GeoDataFrame
        A GeoDataFrame of points with the site names as the index.
    poly: GeoDataFrame
        A GeoDataFrame of polygons with the site names as the index.
    poly_id_col: str or list of str
        The names of the columns to join.

    Returns
    -------
    GeoDataFrame
    """

    poly2 = poly[[poly_id_col, 'geometry']]
    poly3 = poly2.dissolve(by=poly_id_col)

    join1 = sjoin(pts, poly3, how='inner', op='within')
    join1.rename(columns={join1.columns[-1]: poly_id_col}, inplace=True)

    return join1, poly3


# def pts_sql_join(pts, sql_codes):
#     """
#     Function to perform spatial joins on sql tables that are polygon layers.
#     """
#
#     sql1 = sql_arg()
#
#     for i in sql_codes:
#         poly = rd_sql(**sql1.get_dict(i))
#         pts = sjoin(pts, poly, how='left', op='within').drop('index_right', axis=1)
#
#     return pts


def precip_catch_agg(sites, site_precip, id_area):
    """
    Function to aggregate time series of catchments into their all of their upstream catchments.
    """

    #    n_sites = len(sites) + len(singles)
    #    if n_sites != len(site_precip.columns):
    #        print("Site numbers between data sets are not the same!")
    output = site_precip.copy()

    id_area2 = id_area.area
    area_out = pd.concat([id_area2, id_area2], axis=1)
    area_out.columns = ['id_area', 'tot_area']
    site_precip2 = site_precip.mul(id_area2.values.flatten(), axis=1)

    for i in sites.index:
        set1 = np.insert(sites.loc[i, :].dropna().values, 0, i).astype(int)
        tot_area = int(id_area2[np.in1d(id_area2.index, set1)].sum())
        output.loc[:, i] = (site_precip2[set1].sum(axis=1) / tot_area).values
        area_out.loc[i, 'tot_area'] = tot_area

    return output.round(2), area_out.round()


def xy_to_gpd(id_col, x_col, y_col, df=None, crs=2193):
    """
    Function to convert a DataFrame with x and y coordinates to a GeoDataFrame.

    Parameters
    ----------
    df: Dataframe
        The DataFrame with the location data.
    id_col: str or list of str
        The column(s) from the dataframe to be returned. Either a one name string or a list of column names.
    xcol: str or ndarray
        Either the column name that has the x values within the df or an array of x values.
    ycol: str or ndarray
        Same as xcol except for y.
    crs: int
        The projection of the data.

    Returns
    -------
    GeoDataFrame
        Of points.
    """

    if type(x_col) is str:
        geometry = [Point(xy) for xy in zip(df[x_col], df[y_col])]
    else:
        x1 = select_sites(x_col)
        y1 = select_sites(y_col)
        geometry = [Point(xy) for xy in zip(x1, y1)]
    if isinstance(id_col, str) & (df is not None):
        id_data = df[id_col]
    elif isinstance(id_col, list):
        if df is not None:
            id_data = df[id_col]
        else:
            id_data = id_col
    elif isinstance(id_col, (np.ndarray, pd.Series, pd.Index)):
        id_data = id_col
    else:
        raise ValueError('id_data could not be determined')
    if isinstance(crs, int):
        crs1 = convert_crs(crs)
    elif isinstance(crs, (str, dict)):
        crs1 = crs
    else:
        raise ValueError('crs must be an int, str, or dict')
    gpd1 = gpd.GeoDataFrame(id_data, geometry=geometry, crs=crs1)
    return gpd1


# def flow_sites_to_shp(sites='All', min_flow_only=False, export=False, export_path='sites.shp'):
#     """
#     Function to create a geopandas/shapefile from flow sites.
#     """
#
#     ### Import from databases
#     if min_flow_only:
#         min_flow_sites = rd_sql('SQL2012PROD05', 'Wells', '"vMinimumFlowSites+Consent+Well_classes"',
#                                 col_names=['RefDbase', 'RefDbaseKey', 'restrictionType', 'RecordNo', 'WellNo'],
#                                 where_col='RefDbase', where_val=['Gauging', 'Hydrotel'])
#         min_flow_sites.columns = ['type', 'site', 'restr', 'crc', 'wap']
#         min_flow_sites['site'] = min_flow_sites['site'].astype(int)
#         min_flow_sites = min_flow_sites[min_flow_sites.restr == 'LowFlow']
#
#     site_geo = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'],
#                       geo_col=True)
#     site_geo.columns = ['site', 'river', 'site_name', 'geometry']
#     site_geo['river'] = site_geo.river.apply(lambda x: x.title())
#     site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.title())
#     site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace(' (Recorder)', ''))
#     site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Sh', 'SH'))
#     site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Ecs', 'ECS'))
#
#     ### Select sites
#     if type(sites) is str:
#         if sites is 'All':
#             sites_sel_geo = site_geo
#         elif sites.endswith('.shp'):
#             poly = gpd.read_file(sites)
#             sites_sel_geo = sel_sites_poly(poly, site_geo)
#         else:
#             raise ValueError('If sites is a str, then it must be a shapefile.')
#     else:
#         sites_sel = select_sites(sites).astype('int32')
#         sites_sel_geo = site_geo[np.in1d(site_geo.site, sites_sel)]
#     if min_flow_only:
#         sites_sel_geo = sites_sel_geo[np.in1d(sites_sel_geo.site, min_flow_sites.site.values)]
#
#     ### Export and return
#     if export:
#         sites_sel_geo.to_file(export_path)
#     return sites_sel_geo


def convert_crs(from_crs, crs_type='proj4', pass_str=False):
    """
    Function to convert one crs format to another.

    Parameters
    ----------
    from_crs: int or str
        The crs as either an epsg number or a str in a common crs format (e.g. proj4 or wkt).
    crs_type: str
        Output format type of the crs ('proj4', 'wkt', 'proj4_dict', or 'netcdf_dict').
    pass_str: str
        If input is a str, should it be passed though without conversion?

    Returns
    -------
    str or dict
    """

    ### Load in crs
    if all([pass_str, isinstance(from_crs, str)]):
        crs2 = from_crs
    else:
        if isinstance(from_crs, int):
            crs1 = parser.from_epsg_code(from_crs)
        elif isinstance(from_crs, str):
            crs1 = parser.from_unknown_text(from_crs)
        else:
            raise  ValueError('from_crs must be an int or str')

        ### Convert to standard formats
        if crs_type == 'proj4':
            crs2 = crs1.to_proj4()
        elif crs_type == 'wkt':
            crs2 = crs1.to_ogc_wkt()
        elif crs_type in ['proj4_dict', 'netcdf_dict']:
            crs1a = crs1.to_proj4()
            crs1b = crs1a.replace('+', '').split()[:-1]
            crs1c = dict(i.split('=') for i in crs1b)
            crs2 = dict((i, pd.to_numeric(crs1c[i], 'ignore')) for i in crs1c)
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

    return crs2


def point_to_poly_apply(geo, side_len):
    """
    Apply function for a GeoDataFrame to convert a point to a square polygon. Input is a shapely point. Output is a shapely polygon.

    Parameters
    ----------
    geo: Point
        A shapely point.
    side_len: int
        The side length of the square (in the units of geo).

    Returns
    -------
    Shpaely Polygon
    """

    half_side = side_len * 0.5
    l1 = Polygon([[geo.x + half_side, geo.y + half_side], [geo.x + half_side, geo.y - half_side],
                  [geo.x - half_side, geo.y - half_side], [geo.x - half_side, geo.y + half_side]])
    return l1


def points_grid_to_poly(geodataframe, id_col):
    """
    Function to convert a GeoDataFrame of evenly spaced gridded points to square polygons. Output is a GeoDataFrame of the same length as input.

    geodataframe: GeoDataFrame
        GeoDataFrame of gridded points with an id column.
    id_col: str or list of str
        The id column(s) name(s).

    Returns
    -------
    GeoDataFrame
    """

    geo1a = pd.Series(geodataframe.geometry.apply(lambda j: j.x))
    geo1b = geo1a.shift()

    side_len1 = (geo1b - geo1a).abs()
    side_len = side_len1[side_len1 > 0].min()
    gpd1 = geodataframe.apply(lambda j: point_to_poly_apply(j.geometry, side_len=side_len), axis=1)
    gpd2 = gpd.GeoDataFrame(gpd1[id_col], geometry=gpd1, crs=gpd1.crs)
    return gpd2


def spatial_overlays(gpd1, gpd2, how='intersection'):
    """
    Much faster overlay operation as compared to the stock GeoPandas overlay.

    gpd1: GeoDataFrame
        First GeoDataFrame
    df2: GeoDataFrame
        Second GeoDataFrame
    how: str
        The associated operation. Either 'intersection' or 'difference'.

    Returns
    -------
    GeoDataFrame
    """

    df1 = gpd1.copy()
    df2 = gpd2.copy()

    if how == 'intersection':
        # Spatial Index to create intersections
        spatial_index = df2.sindex
        df1['bbox'] = df1.geometry.apply(lambda x: x.bounds)
        df1['histreg'] = df1.bbox.apply(lambda x: list(spatial_index.intersection(x)))
        pairs = df1['histreg'].to_dict()
        nei = []
        for i, j in pairs.items():
            for k in j:
                nei.append([i, k])

        pairs = gpd.GeoDataFrame(nei, columns=['idx1', 'idx2'], crs=df1.crs)
        pairs = pairs.merge(df1, left_on='idx1', right_index=True)
        pairs = pairs.merge(df2, left_on='idx2', right_index=True, suffixes=['_1', '_2'])
        pairs['Intersection'] = pairs.apply(lambda x: (x['geometry_1'].intersection(x['geometry_2'])).buffer(0), axis=1)
        pairs = gpd.GeoDataFrame(pairs, columns=pairs.columns, crs=df1.crs)
        cols = pairs.columns.tolist()
        cols.remove('geometry_1')
        cols.remove('geometry_2')
        cols.remove('histreg')
        cols.remove('bbox')
        cols.remove('Intersection')
        dfinter = pairs[cols + ['Intersection']].copy()
        dfinter.rename(columns={'Intersection': 'geometry'}, inplace=True)
        dfinter = gpd.GeoDataFrame(dfinter, columns=dfinter.columns, crs=pairs.crs)
        dfinter = dfinter.loc[dfinter.geometry.is_empty == False]
        return dfinter
    elif how == 'difference':
        spatial_index = df2.sindex
        df1['bbox'] = df1.geometry.apply(lambda x: x.bounds)
        df1['histreg'] = df1.bbox.apply(lambda x: list(spatial_index.intersection(x)))
        df1['new_g'] = df1.apply(
            lambda x: reduce(lambda x, y: x.difference(y).buffer(0), [x.geometry] + list(df2.iloc[x.histreg].geometry)),
            axis=1)
        df1.geometry = df1.new_g
        df1 = df1.loc[df1.geometry.is_empty == False].copy()
        df1.drop(['bbox', 'histreg', 'new_g'], axis=1, inplace=True)
        return df1


def closest_line_to_pts(pts, lines, line_site_col, buffer_dis=None):
    """
    Function to determine the line closest to each point. Inputs must be GeoDataframes.

    Parameters
    ----------
    pts: GeoDataFrame
        The points input.
    lines: GeoDataFrame
        The lines input.
    line_site_col: str
        The site column from the 'lines' that should be retained at the output.
    buffer_dis: int
        The max distance from each point to search for a line. Try to use the shortest buffer_dis that will cover all of your points as a larger buffer_dis will significantly slow down the operation.

    Returns
    -------
    GeoDataFrame
    """

    pts_line_seg = gpd.GeoDataFrame()
    for i in pts.index:
        pts_seg = pts.loc[[i]]
        if isinstance(buffer_dis, int):
            bound = pts_seg.buffer(buffer_dis).unary_union
            lines1 = lines[lines.intersects(bound)]
        else:
            lines1 = lines.copy()
        if lines1.empty:
            continue
        near1 = lines1.distance(pts.geometry[i]).idxmin()
        line_seg1 = lines1.loc[near1, line_site_col]
        pts_seg[line_site_col] = line_seg1
        pts_line_seg = pd.concat([pts_line_seg, pts_seg])
    #        print(i)

    ### Determine points that did not find a line
    mis_pts = pts.loc[~pts.index.isin(pts_line_seg.index)]
    if not mis_pts.empty:
        print(mis_pts)
        print('Did not find a line segment for these sites')

    return pts_line_seg


def multipoly_to_poly(geodataframe):
    """
    Function to convert a GeoDataFrame with some MultiPolygons to only polygons. Creates additional rows in the GeoDataFrame.

    Parameters
    ----------
    geodataframe: GeoDataFrame

    Returns
    -------
    GeoDataFrame
    """

    gpd2 = gpd.GeoDataFrame()
    for i in geodataframe.index:
        geom1 = geodataframe.loc[[i]]
        geom2 = geom1.loc[i, 'geometry']
        if geom2.type == 'MultiPolygon':
            polys = [j for j in geom2]
            new1 = geom1.loc[[i] * len(polys)]
            new1.loc[:, 'geometry'] = polys
        else:
            new1 = geom1.copy()
        gpd2 = pd.concat([gpd2, new1])
    return gpd2.reset_index(drop=True)
