# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 6/12/2017 1:37 PM
"""

from __future__ import division
from core import env
import pandas as pd
from set_up_run_per_modpath import part_group_cell_mapper
import netCDF4 as nc
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt

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

def extract_data(path):
    """
    extract the data and export as pd.DataFrame with index of cell_ref_id, Particle_Group and column of Particle_ID count
    :param path:
    :return:
    """
    # for now assume that I can hold the full thing in memory, but watch
    data = open_path_file_as_df(path).loc[:,['Particle_ID', 'Particle_Group', 'Time_Point_Index',
                                             'Layer', 'Row', 'Column']]
    data['ref_cell_id'] = ['{:02d}_{:03d}_{:03d}'.format(k,i,j) for k,i,j in data.loc[:,['Layer', 'Row', 'Column']].itertuples(False,None)]
    # now for some fancy groupby operations
    outdata = data.groupby(['ref_cell_id','Particle_ID']).aggregate({'Particle_Group': _get_group_num}).reset_index()
    outdata = outdata.groupby(['ref_cell_id','Particle_Group']).count() #todo check this too...

    return outdata



def save_emulator(path, outpath): #todo
    # save the data extracted above to an emulator netcdf
    # keep the group id to locate cells, but make a linker (e.g. pass the dictionary to the dataframe)
    data = extract_data(path)
    mapper = part_group_cell_mapper()

    # initalise the dataset
    nc_data = nc.Dataset(outpath, 'w')

    # dimensions
    nc_data.createDimension('layer', smt.layers)
    nc_data.createDimension('row', smt.rows)
    nc_data.createDimension('col',smt.cols)
    nc_data.createDimension('components') # unbounded
    nc_data.createDimension('comp_id', len(mapper)) # to store the cell to cell mapper

    # variables (metadata)
    # layer
    nc_data.createVariable('layer',)
    # row
    # col
    # orgid for mapper
    # org layer
    # org row
    # org col

    # actual data #todo how does netcdf handle expanding dimensions when filling data in parts
    # component id
    # component percentage






if __name__ == '__main__':
    data = extract_data(r"C:\Users\MattH\Desktop\NsmcBase_modpath_tester\NsmcBase_modpath_tester_mp.mppth")
    data.to_csv(r"C:\Users\MattH\Desktop\NsmcBase_modpath_tester\easy_read_pathline.csv")
