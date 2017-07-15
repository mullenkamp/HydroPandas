# -*- coding: utf-8 -*-
"""
Functions for importing meteorological data.
"""


def rd_niwa_rcp(base_path, mtypes, poly, vcsn_sites_csv=r'Z:\Data\VirtualClimate\GIS\niwa_vcsn_wgs84.csv', id_col='Network', x_col='deg_x', y_col='deg_y', output_fun=None, export_path='output'):
    """
    Function to read in the NIWA RCP netcdf files and output the data in a specified format.
    """
    from pandas import read_csv
    from core.spatial import xy_to_gpd, sel_sites_poly
    from geopandas import read_file
    from os import path, walk, makedirs
    from core.ecan_io.met import rd_niwa_rcp_dir

    mtype_name = {'precip': 'TotalPrecipCorr', 'T_max': 'MaxTempCorr', 'T_min': 'MinTempCorr', 'P_atmos': 'MSLP', 'PET': 'PE', 'RH_mean': 'RelHum', 'R_s': 'SurfRad', 'U_z': 'WindSpeed'}

    ### Import and reorganize data
    vcsn_sites = read_csv(vcsn_sites_csv)[[id_col, x_col, y_col]]

    sites_gpd = xy_to_gpd(id_col, x_col, y_col, vcsn_sites, 4326)
    poly1 = read_file(poly)

    sites_gpd2 = sites_gpd.to_crs(poly1.crs)

    mtypes1 = [mtype_name[i] for i in mtypes]

    ### Select sites
    sites_gpd3 = sel_sites_poly(sites_gpd2, poly1)[id_col]
    site_loc1 = vcsn_sites[vcsn_sites[id_col].isin(sites_gpd3)]
    site_loc1.columns = ['id', 'x', 'y']

    ### Read and extract data from netcdf files

    for root, dirs, files in walk(base_path):
        files2 = [i for i in files if i.endswith('.nc')]
        files3 = [j for j in files2 if any(j.startswith(i) for i in mtypes1)]
        file_paths1 = [path.join(root, i) for i in files3]
        if len(file_paths1) > 0:
            ds = rd_niwa_rcp_dir(file_paths1, site_loc1, mtypes)
            if callable(output_fun):
                new_base_path = root.replace(base_path, export_path)
                base_file_name = file_paths1[0].split('VCSN_')[1]
                if not path.exists(new_base_path):
                    makedirs(new_base_path)
                output_fun(ds, new_base_path, base_file_name)
                print(base_file_name)
            else:
                raise ValueError('Must have a output function.')

    ### What should I return?


def rd_niwa_rcp_dir(file_paths, site_loc, mtypes):
    """
    Function to read in one or more nc files with the same time, x, and y but different mtypes.

    file_paths -- A string of a file path or a list of string paths.\n
    site_loc -- A dataframe with id and x and y in decimal degrees WGS84.\n
    mtypes -- The measurement types to extract.
    """
    from xarray import open_dataset, Dataset, DataArray
    from numpy import in1d
    from os.path import basename

    ### Parameters
    mtype_param = {'precip': 'rain', 'T_max': 'tmax', 'T_min': 'tmin', 'P_atmos': 'mslp', 'PET': 'pe', 'RH_mean': 'rh', 'R_s': 'srad', 'U_z': 'wind'}
    mtype_param1 = {v: k for k, v in mtype_param.iteritems()}
    prob_mtypes = ['P_atmos', 'RH_mean', 'R_s', 'U_z', 'T_min']
    bad_names = {'mslp2': 'mslp'}
    data_attr = {'grid_mapping': 'crs'}
    nc_crs = {'inverse_flattening': 298.257223563, 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'grid_mapping_name': 'latitude_longitude'}
    time_attr = {'bounds': 'time_bounds', 'standard_name': 'time', 'axis': 'T', 'long_name': 'time (end of reporting period)'}

    ### Extract the proper time coordinate to fix the problem parameters if needed
    if any(in1d(prob_mtypes, mtypes)):
        tmax_file = [j for j in file_paths if 'MaxTempCorr' in basename(j)][0]
        tmax_ds = open_dataset(tmax_file)
        time_da = tmax_ds['time'].copy()
        tmax_ds.close()

    ### Open data files and Extract the data
    ds10 = Dataset()
    for i in file_paths:
        ds5 = open_dataset(i)
        if any(in1d(bad_names.keys(), ds5.data_vars.keys())):
            ds5 = ds5.rename(bad_names)
        mtype2 = [j for j in ds5.data_vars.keys() if j in mtype_param.values()][0]
        mtype0 = mtype_param1[mtype2]

        ## Prepare the selection from the x and y
        if 'bool_lat' not in locals():
            lat1 = (ds5.latitude.data * 1000).astype('int32')
            lon1 = (ds5.longitude.data * 1000).astype('int32')
            site_lat  = (site_loc['y'] * 1000).astype('int32').unique()
            site_lon  = (site_loc['x'] * 1000).astype('int32').unique()

            bool_lat = in1d(lat1, site_lat)
            bool_lon = in1d(lon1, site_lon)

        ## Extract the data based on criteria from earlier
        ds6 = ds5.sel(latitude=bool_lat, longitude=bool_lon)
        da1 = ds6[[mtype2]]
        attr1 = da1[mtype2].attrs
        attr1.update(data_attr)
        da1[mtype2].attrs = attr1
        da1['time'].attrs = time_attr

        ## Imbed the correct time coordinates if necessary
        if mtype0 in prob_mtypes:
            da1['time'] = time_da

        ## Merge datasets
        ds10 = ds10.merge(da1)
#        print([mtype2, len(ds10.time)])
        print(mtype0)

    ### Add in dummy GIS variable
    ds_crs = DataArray(4326, attrs=nc_crs, name='crs').to_dataset()
    ds11 = ds10.merge(ds_crs).copy()
    ds11.attrs = da1.attrs

    return(ds11)


#def export_rcp_lst(ds, export_path):
#    """
#    Function to take the output of rd_niwa_rcp_dir and save the data as standard lst files.
#    """
#    from os import path
#
#    ### Reorganize
#    df3 = df[['id', 'y', 'x', 'time', 'precip', 'PET']]
#    time1 = df3.time.dt.strftime('%Y%m%d')
#    df3.loc[:, 'time'] = time1
#
#    ### Save to many files (by id)
#    id1 = df3.id.unique()
#    for i in id1:
#        out1 = df3[df3.id == i]
#        out1.to_csv(path.join(export_path, i + '.lst'), header=False, index=False)


def export_rcp_nc(ds, export_path, file_name):
    """
    Function to take the output of rd_niwa_rcp_dir and save the data as a standard nc file.
    """
    from os.path import join

    ### Save to nc file based on directory names
    ds.to_netcdf(join(export_path, 'VCSN_' + file_name))
    ds.close()



def nc_add_gis(nc, x_coord, y_coord):
    """
    Function to add the appropriate attributes to a netcdf file to be able to load it into GIS if the netcdf file has x and y in WGS84 decimal degrees.

    nc -- A path str to the netcdf file (str).\n
    x_coord -- The x coordinate name (str).\n
    y_coord -- The y coordinate name (str).
    """
    from xarray import open_dataset, DataArray
    from os.path import splitext

    ### Attributes for the various datasets
    nc_crs = {'inverse_flattening': 298.257223563, 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'grid_mapping_name': 'latitude_longitude'}

    x_attr = {'long_name': 'longitude', 'units': 'degrees_east', 'standard_name': 'longitude', 'axis': 'X'}
    y_attr = {'long_name': 'latitude', 'units': 'degrees_north', 'standard_name': 'latitude', 'axis': 'Y'}
    data_attr = {'grid_mapping': 'crs'}

    ### Read in the nc
    ds1 = open_dataset(nc)

    ### Determine the variables with x and y coordinates
    vars1 = ds1.data_vars
    vars2 = [i for i in vars1 if ((x_coord in ds1[i]) & (y_coord in ds1[i]))]

    ### Put in the additional attribute into the variables
    ds1[x_coord].attrs = x_attr
    ds1[y_coord].attrs = y_attr

    for i in vars2:
        attr1 = ds1[i].attrs
        attr1.update(data_attr)
        ds1[i].attrs = attr1

    ### Add crs dummy dataset
    ds_crs = DataArray(4326, attrs=nc_crs, name='crs').to_dataset()
    ds2 = ds1.merge(ds_crs)

    ### Resave nc file
    new_path = splitext(nc)[0] + '_gis.nc'
    ds2.to_netcdf(new_path)
    ds1.close()
    ds2.close()


def rd_niwa_vcsn(mtypes, sites, nc_path='Y:/VirtualClimate/vcsn_precip_et_2016-06-06.nc', vcsn_sites_csv=r'Y:\VirtualClimate\GIS\niwa_vcsn_wgs84.csv', id_col='Network', x_col='deg_x', y_col='deg_y', buffer_dis=0, include_sites=False, out_crs=None, netcdf_out=None):
    """
    Function to read in the NIWA vcsn netcdf file and output the data as a dataframe.

    mtypes -- A string or list of the measurement types (either 'precip', or 'PET').\n
    sites -- Either a list of vcsn site names or a polygon of the area of interest.\n
    nc_path -- The path to the vcsn nc file.\n
    vcsn_sites_csv -- The csv file that relates the site name to coordinates.\n
    id_col -- The site name column in vcsn_sites_csv.\n
    x_col - The x column name in vcsn_sites_csv.\n
    y_col -- The y column name in vcsn_sites_csv.\n
    include_sites -- Should the site names be added to the output?\n
    out_crs -- The crs epsg number for the output coordinates if different than the default WGS85 (e.g. 2193 for NZTM).
    """
    from pandas import read_csv, Series, merge
    from core.spatial import xy_to_gpd, sel_sites_poly, convert_crs
    from geopandas import read_file
    from numpy import ndarray
    from xarray import open_dataset

    mtype_name = {'precip': 'rain', 'PET': 'pe'}

    ### Import and reorganize data
    vcsn_sites = read_csv(vcsn_sites_csv)[[id_col, x_col, y_col]]

    if isinstance(sites, str):
        if sites.endswith('.shp'):
            sites_gpd = xy_to_gpd(id_col, x_col, y_col, vcsn_sites, 4326)
            poly1 = read_file(sites)

            sites_gpd2 = sites_gpd.to_crs(poly1.crs)

            ### Select sites
            sites2 = sel_sites_poly(sites_gpd2, poly1, buffer_dis)[id_col]
    elif isinstance(sites, (list, Series, ndarray)):
        sites2 = sites

    ### Select locations
    site_loc1 = vcsn_sites[vcsn_sites[id_col].isin(sites2)]
    site_loc1.columns = ['id', 'x', 'y']

    ### Select mtypes
    if isinstance(mtypes, str):
        mtypes1 = [mtype_name[mtypes]]
    else:
        mtypes1 = [mtype_name[i] for i in mtypes]

    if include_sites:
        mtypes1.extend(['site'])

    ### Read and extract data from netcdf files
    ds1 = open_dataset(nc_path)
    ds2 = ds1.sel(longitude=site_loc1.x.unique(), latitude=site_loc1.y.unique())
    ds3 = ds2[mtypes1]

    ### Convert to DataFrame
    df1 = ds3.to_dataframe().reset_index()
    df1.rename(columns={'latitude': 'y', 'longitude': 'x'}, inplace=True)

    ### Convert to different crs if needed
    if out_crs is not None:
        crs1 = convert_crs(out_crs)
        new_gpd1 = xy_to_gpd('id', 'x', 'y', site_loc1, 4326)
        new_gpd2 = new_gpd1.to_crs(crs1)
        site_loc2 = site_loc1.copy()
        site_loc2['x_new'] = new_gpd2.geometry.apply(lambda j: j.x)
        site_loc2['y_new'] = new_gpd2.geometry.apply(lambda j: j.y)

        df2 = merge(df1, site_loc2[['x', 'y', 'x_new', 'y_new']], on=['x', 'y'])
        df3 = df2.drop(['x', 'y'], axis=1).rename(columns={'x_new': 'x', 'y_new': 'y'})
        col_order = ['y', 'x', 'time']
        col_order.extend(mtypes1)
        df4 = df3[col_order]
    else:
        df4 = df1

    ds1.close()
    ds3.close()

    ### Return
    if isinstance(netcdf_out, str):
        ds3.to_netcdf(netcdf_out)
    return(df4)















