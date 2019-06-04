# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 11:16:40 2017

@author: MichaelEK

Interpolation functions for Met data.
"""
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from hydropandas.tools.general.spatial.vector import sel_sites_poly, xy_to_gpd
from hydropandas.tools.general.spatial.raster import grid_interp_ts, save_geotiff


def poly_interp_agg(precip, precip_crs, poly, data_col, time_col, x_col, y_col, interp_buffer_dis=10000, poly_buffer_dis=0, grid_res=None, interp_fun='cubic', agg_ts_fun=None, period=None, digits=2, agg_xy=False, nfiles='many', output_path=None):
    """
    Function to select the precip sites within a polygon with a certain buffer distance, then interpolate/resample the data at a specific resolution, then output the results.

    Parameters
    ----------
    precip: DataFrame
        Dataframe of time, x, y, and precip.
    precip_crs: int
        The crs of the x and y coordinates of the precip dataframe.
    poly: GeoDataFrame or str
        str path of a shapefile polygon or a polygon GeoDataFrame.
    interp_buffer_dis: int
        Buffer distance of the polygon selection when performing the interpolation.
    poly_buffer_dis: int
        Buffer distance of the polygon selection when outputting the results.
    grid_res: int
        The resulting grid resolution in meters (or the unit of the final projection).
    interp_fun: str
        The scipy griddata interpolation function to be applied (see https://docs.scipy.org/doc/scipy-0.19.0/reference/generated/scipy.interpolate.griddata.html).
    agg_ts_fun: str or None
        The pandas time series resampling function to resample the data in time (either 'mean' or 'sum'). If None, then no time resampling.
    period: str or None
        The pandas time series code to resample the data in time (i.e. '2H' for two hours).
    digits: int
        the number of digits to round to.
    agg_xy: bool
        Should all of the interpolated points within the polygon area be aggregated (mean) to a single time series?
    nfiles: str
        If output_path is a geotiff, then 'one' or 'many' geotiffs to be created.
    output_path: str or None
        Full path string where the output should be stored. The file extension should be one of '.tif' for geotiff, '.nc' for netcdf, or '.csv' for csv.

    Returns
    -------
    DataFrame
    """

    ### Convert x and y of precip to geodataframe
    sites0 = precip[[x_col, y_col]].drop_duplicates().reset_index(drop=True)
    sites = xy_to_gpd(sites0.index, sites0[x_col], sites0[y_col], crs=precip_crs)
    sites.columns = ['site', 'geometry']

    ### Select the locations within the polygon
    if isinstance(poly, (gpd.GeoDataFrame, gpd.GeoSeries)):
        poly1 = poly.copy()
    elif isinstance(poly, str):
        poly1 = gpd.read_file(poly)
    sites1 = sites.to_crs(poly1.crs)
    sites_sel = sel_sites_poly(sites1, poly, interp_buffer_dis)
    sites2 = sites0.loc[sites_sel['site']]

    ### Determine the grid resolution if not set
    if not isinstance(grid_res, (int, float)):
        bounds = poly1.unary_union.bounds
        x_range = bounds[2] - bounds[0]
        y_range = bounds[3] - bounds[1]
        min1 = min([x_range, y_range])
        grid_res = int(np.ceil(min1/20))

    ### Select the precip data from the sites
    precip2 = pd.merge(precip, sites2, on=['x', 'y']).dropna()

    ### Interpolate grid
    poly_crs = ['+' + str(i) + '=' + str(poly1.crs[i]) for i in poly1.crs]
    poly_crs1 = ' '.join(poly_crs)
    new_precip = grid_interp_ts(precip2, time_col, x_col, y_col, data_col, grid_res, sites.crs, poly_crs1, interp_fun=interp_fun, agg_ts_fun=agg_ts_fun, period=period, digits=digits)

    ### Create new sites list
    time = new_precip[time_col].sort_values().unique()
    sites_new_df = new_precip.loc[new_precip[time_col] == time[0], [x_col, y_col, data_col]]
    sites_new = xy_to_gpd(sites_new_df.index.values, x_col, y_col, sites_new_df, poly_crs1)
    sites_new.columns = ['site', 'geometry']
    new_precip['site'] = np.tile(sites_new_df.index.values, len(time))

    ### Select sites from polygon
    sites_sel2 = sel_sites_poly(sites_new, poly, poly_buffer_dis)
    new_precip2 = new_precip.loc[new_precip.site.isin(sites_sel2.site), [time_col, x_col, y_col, data_col]]

    ### Agg to polygon if required
    if agg_xy:
        new_precip3 = new_precip2.groupby(time_col)[data_col].mean().round(digits)
        time_col = None
    else:
        new_precip3 = new_precip2.set_index([time_col, x_col, y_col])[data_col]

    ### Save results
    if isinstance(output_path, str):
        path1 = os.path.splitext(output_path)[0]
        if '.csv' in output_path:
            new_precip3.to_csv(path1 + '.csv', header=True)

        if '.tif' in output_path:
            df = new_precip3.reset_index()
            save_geotiff(df=df, data_col=data_col, crs=poly_crs1, x_col=x_col, y_col=y_col, time_col=time_col, nfiles=nfiles, export_path=path1 + '.tif')

        if '.nc' in output_path:
            ds1 = new_precip3.to_xarray().to_dataset()
            ds1.attrs['spatial_ref'] = poly_crs1
            ds1.to_netcdf(path1 + '.nc')

    return new_precip3


