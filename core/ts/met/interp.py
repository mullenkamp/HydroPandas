# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 11:16:40 2017

@author: MichaelEK

Interpolation functions for Met data.
"""


def sel_interp_agg(precip, sites, poly, grid_res, data_col, time_col, x_col, y_col, buffer_dis=10000, interp_fun='multiquadric', agg_ts_fun=None, period=None, digits=3, agg_xy=False, output_format='csv', nfiles='many', output_path='precip_interp.csv'):
    """
    Function to select the precip sites within a polygon with a certain buffer distance, then interpolate/resample the data at a specific resolution, then output the results.
    precip -- dataframe of time, x, y, and precip.\n
    sites -- GeoDataFrame of site locations.\n
    poly -- String path of a shapefile polygon.\n
    res -- Resolution in meters of the resampling.\n
    buffer_dis -- Buffer distance of the polygon selection.\n
    interp_fun -- The scipy Rbf interpolation function to be applied (see https://docs.scipy.org/doc/scipy-0.16.1/reference/generated/scipy.interpolate.Rbf.html).\n
    agg_ts_fun -- The pandas time series resampling function to resample the data in time (either 'mean' or 'sum'). If None, then no time resampling.\n
    agg_ts_fun -- The pandas time series code to resample the data in time (i.e. '2H' for two hours).\n
    digits -- the number of digits to round to (int).\n
    agg_xy -- Should all of the interpolated points within the polygon area be aggregated (mean) to a single time series?\n
    output_format -- Either a str or list of 'csv', 'geotiff', and/or 'netcdf'.\n
    nfiles -- If 'geotiff' is in the output_format, then 'one' or 'many' geotiffs to be created.\n
    output_path -- Full path string where the output should be stored.
    """

    from core.spatial import sel_sites_poly, grid_interp_ts, xy_to_gpd, save_geotiff
    from geopandas import read_file
    from numpy import tile
    from os import path

    ### Select the locations within the polygon
    poly1 = read_file(poly)
    sites1 = sites.to_crs(poly1.crs)
    sites_sel = sel_sites_poly(sites1, poly, buffer_dis)
    sites2 = sites[sites.site.isin(sites_sel.site)]

    ### Select the precip data from the sites
    precip2 = precip[precip.site.isin(sites2.site)]

    ### Interpolate grid
    poly_crs = ['+' + str(i) + '=' + str(poly1.crs[i]) for i in poly1.crs]
    poly_crs1 = ' '.join(poly_crs)
    new_precip = grid_interp_ts(precip2, time_col, x_col, y_col, data_col, grid_res, sites.crs, poly_crs1, interp_fun=interp_fun, agg_ts_fun=agg_ts_fun, period=period, digits=digits)

    ### Create new sites list
    time = new_precip[time_col].sort_values().unique()
    sites_new_df = new_precip.loc[new_precip[time_col] == time[0], [x_col, y_col, data_col]]
    sites_new = xy_to_gpd(sites_new_df.index.values, x_col, y_col, sites_new_df, poly_crs1)
    sites_new.columns = ['site', 'geometry']
    new_precip['site'] = tile(sites_new_df.index.values, len(time))

    ### Select sites from polygon
    sites_sel2 = sel_sites_poly(sites_new, poly)
    new_precip2 = new_precip.loc[new_precip.site.isin(sites_sel2.site), [time_col, x_col, y_col, data_col]]

    ### Agg to polygon if required
    if agg_xy:
        new_precip3 = new_precip2.groupby(time_col)[data_col].mean().round(digits)
        time_col = None
    else:
        new_precip3 = new_precip2.set_index([time_col, x_col, y_col])[data_col]

    ### Save results
    path1 = path.splitext(output_path)[0]
    if 'csv' in output_format:
        new_precip3.to_csv(path1 + '.csv', header=True)

    if 'geotiff' in output_format:
        df = new_precip3.reset_index()
        save_geotiff(df=df, data_col=data_col, crs=poly_crs1, x_col=x_col, y_col=y_col, time_col=time_col, nfiles=nfiles, export_path=path1 + '.tif')

    if 'netcdf' in output_format:
        ds1 = new_precip3.to_xarray().to_dataset()
        ds1.attrs['spatial_ref'] = poly_crs1
        ds1.to_netcdf(path1 + '.nc')

    return(new_precip3)

