# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 11:32:45 2017

@author: MichaelEK

Functions for processing TopNet data.
"""


def proc_topnet_nc(base_path, out_path, start_str):
    """
    Function to process TopNet output data that come as multiple nc files of different time periods.

    base_path -- The base path where many nc files and subfolders lay.\n
    out_path -- The output directory for the processed data.\n
    start_str -- The name that the files should be given at the beginning of the file name.
    """
    from xarray import open_dataset, concat, Dataset
    from os import path, walk, makedirs
    from pandas import to_datetime

    #### Walk through the files and folders
    for root, dirs, files in walk(base_path):
        files2 = [i for i in files if i.endswith('.nc')]
        file_paths1 = [path.join(root, i) for i in files2]
        if len(file_paths1) > 0:
            new_base_path = path.split(root.replace(base_path, out_path))[0]
            if not path.exists(new_base_path):
                makedirs(new_base_path)
            ### Load in each file and process each
            for i in file_paths1:
                tds1 = open_dataset(i)

                ## Reorganise variables and coordinates
                tds1['nrch'] = tds1['rchid']
                if 'river_flow_rate_mod' in tds1.data_vars.keys():
                    tds1.rename({'river_flow_rate_mod': 'flow_rate'}, inplace=True)
                if 'mod_flow' in tds1.data_vars.keys():
                    tds1.rename({'mod_flow': 'flow_rate'}, inplace=True)
                tds2 = tds1[['flow_rate', 'drainge', 'ovstn_q', 'soilevp', 'soilh2o', 'aprecip']]
                tds2a = tds2.drop(['basin_lat', 'basin_lon', 'end_lat', 'end_lon'])
                tds3 = tds2a.squeeze('nens')
                time1 = to_datetime(to_datetime(tds3['time'].data).date)
                tds3['time'] = time1

                ## Data integrety checks
                freq1 = time1.inferred_freq
                if freq1 != 'D':
                    raise ValueError('Time dimension is not regular!')
                if 'topds1' in locals():
                    force1 = topds1.attrs['forcing']
                    force2 = tds3.attrs['forcing']
                    if force1 != force2:
                        print(path.split(i)[1] + ' has a different forcing compared to previous files for this model')

#                print(path.split(i)[1], tds1.data_vars)

                ## Combine data
                if 'topds1' in locals():
                    topds1 = concat([topds1, tds3], 'time')
                else:
                    topds1 = tds3

            topds2 = topds1.copy()
            del(topds1)

            ### Extract necessary attributes
            paths = path.split(path.split(i)[0])
            model_name = paths[1]
            rcp_name = path.split(paths[0])[1]
            base_file_name = '_'.join([start_str, rcp_name, model_name]) + '.nc'

            ### Save the new data
            topds2.to_netcdf(path.join(new_base_path, base_file_name))
            tds1.close()
            topds2.close()
            print(base_file_name)









