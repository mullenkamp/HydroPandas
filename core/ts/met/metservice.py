# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 11:17:26 2017

@author: MichaelEK

Functions for processing MetService data.
"""


def proc_metservice_nc(nc, lat_coord='south_north', lon_coord='west_east', time_coord='Time', time_var='Times'):
    """
    Function to process MetService netcdf files so that it is actually complete. The function adds in the appropriate coordinate arrays for the data and resaves the file with '_corr" added to the end of the name.

    nc -- Full path to the MetService nc file (str).\n
    lat_coord -- The name of the lat coordinate that should be added (str).\n
    lon_coord -- Same as lat_coord except for the lon.\n
    time_coord -- Ditto for the time.\n
    time_var -- The existing name of the time variable (that should be converted and removed).
    """
    from xarray import open_dataset
    from os import path
    from numpy import arange
    from pandas import to_datetime
    from core.ecan_io.met import ACPR_to_rate
    from core.spatial.vector import convert_crs

    ### Parameters
    proj1 = '+proj=lcc +lat_1=-60 +lat_2=-30 +lat_0=-60 +lon_0=167.5 +x_0=211921 +y_0=-1221320 +a=6367470 +b=6367470 +no_defs'

    ### Read in the nc file
    x1 = open_dataset(nc)

    ### Extract parameters and convert to numpy arrays
    time1 = to_datetime(x1[time_var].data, format='%Y-%m-%d_%H:%M:%S')

    nlat = x1.dims[lat_coord]
    nlon = x1.dims[lon_coord]
    x_res = int(x1.attrs['DX'])
    y_res = int(x1.attrs['DY'])

    lat = arange(nlat, dtype='int32') * y_res
    lon = arange(nlon, dtype='int32') * x_res

    ### Remove the old time variable and add in the coordinates
    x2 = x1.drop(time_var)
    x2.coords[time_coord] = ((time_coord), time1)
    x2.coords[lat_coord] = ((lat_coord), lat)
    x2.coords[lon_coord] = ((lon_coord), lon)

    ### rename coordinates
    x3 = x2.rename({time_coord: 'time', lat_coord: 'y', lon_coord: 'x'})

    ### Calc hourly precip rate
    df = x3['ACPR'].to_dataframe().reset_index()
    precip = ACPR_to_rate(df, 'y', 'x')

    ### Remove the first time step (as there is no data for it)
    x4 = x3.sel(time=precip.time.unique())
#
#    ### Put in the hourly rate
    precip_ds = precip.set_index(['time', 'y', 'x']).to_xarray()
    x5 = x4.merge(precip_ds)

    ### Add in attributes
    ## x
    x_attrs = {'standard_name': 'projection_x_coordinate', 'units': 'm', 'axis': 'X'}
    x5.coords['x'].attrs = x_attrs

    ## y
    y_attrs = {'standard_name': 'projection_y_coordinate', 'units': 'm', 'axis': 'Y'}
    x5.coords['y'].attrs = y_attrs

    ## variables
    ACPR_attrs = {'standard_name': 'precipitation_amount', 'units': 'mm', 'description': 'accumulated total grid precipitation'}
    precip_attrs = {'standard_name': 'precipitation_amount', 'units': 'mm', 'description': 'hourly precipitation'}

    x5.variables['ACPR'].attrs = ACPR_attrs
    x5.variables['precip'].attrs = precip_attrs

    ## Overall attributes
    x5.attrs['spatial_ref'] =  proj1

    ### Save the new file and close them
    new_path = path.splitext(nc)[0] + '_corr.nc'
    x5.to_netcdf(new_path)
    x1.close()
    x5.close()


def ACPR_to_rate(df, lat_coord='y', lon_coord='x', time_coord='time'):
    """
    Function to convert cummulative precip to hourly rate.

    df -- DataFrame of the cummulative precip.\n
    lat_coord -- The name of the lat coordinate that should be added (str).\n
    lon_coord -- Same as lat_coord except for the lon.\n
    time_coord -- Ditto for the time.
    """
    from pandas import merge

    ### Extract data into dataframe
    df1 = df.copy().set_index(time_coord)
    df1a = df1.shift(1, freq='H')
    df0 = merge(df1.reset_index(), df1a.reset_index(), on=[time_coord, lon_coord, lat_coord], how='inner')
    df0['precip'] = (df0['ACPR_x'] - df0['ACPR_y']).round(3)
    df2 = df0[[time_coord, lon_coord, lat_coord, 'precip']]

    return(df2)


def MetS_nc_to_df(nc, lat_coord='y', lon_coord='x', time_coord='time', precip_var='precip', proj4='spatial_ref'):
    """
    Function to convert a MetService nc file to the components of precip and sites with x y locations.

    nc -- The path to the corrected MetService netcdf file.\n
    lat_coord -- The name of the lat coordinate that should be added (str).\n
    lon_coord -- Same as lat_coord except for the lon.\n
    time_coord -- Ditto for the time.\n
    precip_var -- The precip variable name.\n
    proj4 -- The proj4 coordinate system attribute name.
    """
    from xarray import open_dataset
    from numpy import tile
    from shapely.geometry import Point
    from geopandas import GeoDataFrame

    ### Extract all data to dataframes
    ds = open_dataset(nc)
    precip = ds[precip_var].to_dataframe().reset_index()
    proj1 = str(ds.attrs[proj4])

    ### Create geodataframe
    time = precip[time_coord].unique()
    sites0 = precip.loc[precip[time_coord] == time[0], [lon_coord, lat_coord]]
    precip.loc[:, 'site'] = tile(sites0.index, len(time))
    sites0.index.name = 'site'

    geometry = [Point(xy) for xy in zip(sites0[lon_coord], sites0[lat_coord])]
    sites = GeoDataFrame(sites0.index, geometry=geometry, crs=proj1)

    ### Return
    ds.close()
    return(precip, sites)


