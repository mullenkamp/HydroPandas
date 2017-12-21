# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 6/12/2017 1:37 PM
"""

from __future__ import division
from core import env
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.modpath_sims.setup_forward_modpath import part_group_cell_mapper
import netCDF4 as nc
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import itertools
import pickle
import numpy as np


# particle id moves to 0 indexed
# particle id links the endpoint file and path file
# make a unique id from a string of k,i,j
# store as a netcdf with 2 variables cellid and percentage
# dimensions of (layer, row, col, ids) +- realisation
# a vectorised dictionary call to extract 'variable' numbers and their corresponding values...
# extraction could take some time, but this is fine as it will be a rare occurance
# the particle
# it looks like every cell occurance that the particle passes through is in teh pathline file maybe confirm with brioch/mike

def open_path_file_as_df(path):
    names = ['Particle_ID',
             'Particle_Group',
             'Time_Point_Index',
             'Cumulative_Time_Step',
             'Tracking_Time',
             'Global_X',
             'Global_Y',
             'Global_Z',
             'Layer',
             'Row',
             'Column',
             'Grid',
             'Local_X',
             'Local_Y',
             'Local_Z',
             'Line_Segment_Index',
             ]
    data = pd.read_table(path, skiprows=3, names=names, delim_whitespace=True)
    return data


def _get_group_num(x):
    return x.iloc[0]


def extract_forward_data(path):
    """
    extract the data and export as pd.DataFrame with index of cell_ref_id, Particle_Group and column of Particle_ID count
    :param path: path to the pathline file
    :return:
    """
    # for now assume that I can hold the full thing in memory, but watch
    drop_names = [
     'Time_Point_Index',
     'Cumulative_Time_Step',
     'Tracking_Time',
     'Global_X',
     'Global_Y',
     'Global_Z',
     'Grid',
     'Local_X',
     'Local_Y',
     'Local_Z',
     'Line_Segment_Index',
     ]
    print('reading data')
    data = open_path_file_as_df(path)
    print('simplifying data')
    data.drop(drop_names, 1, inplace=True)
    # make a ref cell id and make sure it is zero indexed
    data['ref_cell_id'] = ['{:02d}_{:03d}_{:03d}'.format(k - 1, i - 1, j - 1) for k, i, j in
                           data.loc[:, ['Layer', 'Row', 'Column']].itertuples(False, None)]
    data.drop(['Layer', 'Row', 'Column'],axis=1,inplace=True)
    # now for some fancy groupby operations
    print('calculating percentages')
    outdata = data.groupby(['ref_cell_id', 'Particle_ID']).aggregate({'Particle_Group': _get_group_num}).reset_index()
    outdata = outdata.groupby(['ref_cell_id', 'Particle_Group']).count().astype(float)

    # make this output a fraction
    outdata = outdata.rename(columns={'Particle_ID':'fraction'})
    sums = outdata.groupby('ref_cell_id').sum()
    outdata = outdata/sums
    outdata = outdata.reset_index().set_index('ref_cell_id')

    return outdata


def save_forward_data(path, outpath):
    # save the data extracted above to an emulator netcdf
    # keep the group id to locate cells, but make a linker (e.g. pass the dictionary to the dataframe)
    data = extract_forward_data(path)
    data.to_hdf(outpath, 'emulator', mode='w')

def extract_back_data(path_path, group_mapper_path):
    """

    :param path_path: the pathline file
    :param group_mapper_path: the file to the group mapper produced in set_up_reverse_modpath
    :return:
    """
    # for now assume that I can hold the full thing in memory, but watch
    drop_names = [
     'Time_Point_Index',
     'Cumulative_Time_Step',
     'Tracking_Time',
     'Global_X',
     'Global_Y',
     'Global_Z',
     'Grid',
     'Local_X',
     'Local_Y',
     'Local_Z',
     'Line_Segment_Index',
     ]
    print('reading data')


    data = open_path_file_as_df(path_path)
    print('simplifying data')
    data.drop(drop_names, 1, inplace=True)
    # make a ref cell id and make sure it is zero indexed
    data = data.loc[data.Layer == 1]  # just keep all data in top layer
    outdata = {}
    group_mapper = pd.read_csv(group_mapper_path, index_col=0, names=['key','val'])['val'].to_dict()
    for g in set(data.Particle_Group):
        temp = data.loc[data.Particle_Group==g,['Row','Column']]
        temp = temp.reset_index().groupby(['Row','Column']).count().reset_index().values
        temp_out = smt.get_empty_model_grid().astype(int)
        temp_out[temp[:,0],temp[:,1]] = temp[:,2]
        outdata[group_mapper[g]] = temp_out

    return outdata

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    test=extract_back_data(r"C:\Users\MattH\Desktop\test_reverse_modpath_strong\test_reverse.mppth",
                      r"C:\Users\MattH\Desktop\test_reverse_modpath_strong\test_reverse_group_mapper.csv")
    smt.plt_matrix(test[1]>0,base_map=True)
    plt.show()
    print('done')