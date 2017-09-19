# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 15:02:29 2017

@author: MichaelEK
"""


def proc_waimak_nc(base_path, out_path):
    """
    Function to process TopNet output data that come as multiple nc files of different time periods.

    base_path -- The base path where many nc files and subfolders lay.\n
    out_path -- The output directory for the processed data.\n
    """
    from xarray import open_dataset, concat, Dataset
    from os import path, walk, makedirs
    from pandas import to_datetime

    #### Walk through the files and folders
    for root, dirs, files in walk(base_path):
        files2 = [i for i in files if i.endswith('.nc')]
        file_paths1 = [path.join(root, i) for i in files2]
        if len(file_paths1) > 0:
            new_base_path = root.replace(base_path, out_path)
            if not path.exists(new_base_path):
                makedirs(new_base_path)
            ### Load in each file and process each
            mtypes = ['flow', 'drainge', 'ovstn_q', 'soilevp', 'soilh2o', 'aprecip']
            for i in file_paths1:
                tds1 = open_dataset(i)

                ## Reorganise variables and coordinates
                tds1['nrch'] = tds1['rchid']
                if 'river_flow_rate_mod' in tds1.data_vars.keys():
                    tds1.rename({'river_flow_rate_mod': 'flow'}, inplace=True)
                if 'mod_flow' in tds1.data_vars.keys():
                    tds1.rename({'mod_flow': 'flow'}, inplace=True)
                tds2 = tds1[mtypes]
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
            model_name = path.split(new_base_path)[1]
            rcp_name = path.split(path.split(new_base_path)[0])[1]
            dates = to_datetime(topds2.time.data)
            start_date = str(dates[0].year)
            end_date = str(dates[-1].year)
#            base_file_name = '_'.join([start_str, rcp_name, model_name]) + '.nc'

            ### Save the new data
            for j in mtypes:
                file_name = '_'.join([j, model_name, rcp_name, start_date, end_date]) + '.nc'
                topds2[[j]].to_netcdf(path.join(new_base_path, file_name))
                print(file_name)

            tds1.close()
            topds2.close()


def proc_other_nc(base_path, out_path):
    """
    Function to process TopNet output data that come as multiple nc files of different time periods.

    base_path -- The base path where many nc files and subfolders lay.\n
    out_path -- The output directory for the processed data.\n
    """
    from xarray import open_dataset, concat, Dataset
    from os import path, walk, makedirs, rename
    from pandas import to_datetime

    #### Walk through the files and folders
    for root, dirs, files in walk(base_path):
        files2 = [i for i in files if i.endswith('.nc')]
        file_paths1 = [path.join(root, i) for i in files2]
        if len(file_paths1) > 0:

            ### Check to see how the files are organised


            new_base_path = root.replace(base_path, out_path)
            if not path.exists(new_base_path):
                makedirs(new_base_path)
            ### Load in each file and process each
            mtypes = ['flow', 'drainge', 'ovstn_q', 'soilevp', 'soilh2o', 'aprecip']
            for i in file_paths1:
                tds1 = open_dataset(i)

                ## Reorganise variables and coordinates
                tds1['nrch'] = tds1['rchid']
                if 'river_flow_rate_mod' in tds1.data_vars.keys():
                    tds1.rename({'river_flow_rate_mod': 'flow'}, inplace=True)
                if 'mod_flow' in tds1.data_vars.keys():
                    tds1.rename({'mod_flow': 'flow'}, inplace=True)
                tds2 = tds1[mtypes]
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
            model_name = path.split(new_base_path)[1]
            rcp_name = path.split(path.split(new_base_path)[0])[1]
            dates = to_datetime(topds2.time.data)
            start_date = str(dates[0].year)
            end_date = str(dates[-1].year)
#            base_file_name = '_'.join([start_str, rcp_name, model_name]) + '.nc'

            ### Save the new data
            for j in mtypes:
                file_name = '_'.join([j, model_name, rcp_name, start_date, end_date]) + '.nc'
                topds2[[j]].to_netcdf(path.join(new_base_path, file_name))
                print(file_name)

            tds1.close()
            topds2.close()


















