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
import itertools


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
    data = open_path_file_as_df(path).loc[:, ['Particle_ID', 'Particle_Group', 'Time_Point_Index',
                                              'Layer', 'Row', 'Column']] #todo fix this so it drops in place
    # make a ref cell id and make sure it is zero indexed
    data['ref_cell_id'] = ['{:02d}_{:03d}_{:03d}'.format(k - 1, i - 1, j - 1) for k, i, j in
                           data.loc[:, ['Layer', 'Row', 'Column']].itertuples(False, None)] #todo drop all of the other components in place
    # now for some fancy groupby operations
    outdata = data.groupby(['ref_cell_id', 'Particle_ID']).aggregate({'Particle_Group': _get_group_num}).reset_index()
    outdata = outdata.groupby(['ref_cell_id', 'Particle_Group']).count().astype(float)  # todo check this too...

    return outdata


def save_emulator(path, outpath):  # todo check
    # save the data extracted above to an emulator netcdf
    # keep the group id to locate cells, but make a linker (e.g. pass the dictionary to the dataframe)
    mapper = part_group_cell_mapper()
    data = extract_data(path)
    temp = data.reset_index()
    comps_dim = temp.groupby('ref_cell_id').count()['Particle_Group'].max()
    # turn data to percentages
    data['Particle_ID'] *= 1/data['Particle_ID'].sum()


    # initalise the dataset
    nc_file = nc.Dataset(outpath, 'w')

    # dimensions
    nc_file.createDimension('layer', smt.layers)
    nc_file.createDimension('row', smt.rows)
    nc_file.createDimension('col', smt.cols)
    nc_file.createDimension('component', comps_dim)
    nc_file.createDimension('comp_id', len(mapper))  # to store the cell to cell mapper

    # variables (metadata)
    layer = nc_file.createVariable('layer', 'i4', ('layer',), fill_value=-9)
    layer.setncatts({'units': 'none',
                     'long_name': 'model layer',
                     'comments': '0 indexed',
                     'missing_value': -9})
    layer[:] = range(smt.layers)

    row = nc_file.createVariable('row', 'i4', ('row',), fill_value=-9)
    row.setncatts({'units': 'none',
                   'long_name': 'model row',
                   'comments': '0 indexed',
                   'missing_value': -9})
    row[:] = range(smt.rows)

    col = nc_file.createVariable('col', 'i4', ('col',), fill_value=-9)
    col.setncatts({'units': 'none',
                   'long_name': 'model column',
                   'comments': '0 indexed',
                   'missing_value': -9})
    col[:] = range(smt.cols)

    # orgid for mapper
    keys, values = mapper.keys(), mapper.values()
    orgid = nc_file.createVariable('orgid', 'i4', ('comp_id',), fill_value=-9)
    orgid.setncatts({'units': 'none',
                     'long_name': 'origin ID',
                     'missing_value': -9})
    orgid[:] = keys
    # org layer
    # org row
    # org col
    temp = mapper.values()
    orglayer = nc_file.createVariable('org_layer', 'i1', ('comp_id',), fill_value=-9)
    orglayer.setncatts({'units': 'none',
                        'long_name': 'origin model layer',
                        'comments': '0 indexed',
                        'missing_value': -9})
    orglayer[:] = 0  # all from top layer

    orgrow = nc_file.createVariable('org_row', 'i1', ('comp_id',), fill_value=-9)
    orgrow.setncatts({'units': 'none',
                      'long_name': 'origin model row',
                      'comments': '0 indexed',
                      'missing_value': -9})
    orgrow[:] = [e[0] for e in values]

    orgcol = nc_file.createVariable('org_col', 'i1', ('comp_id',), fill_value=-9)
    orgcol.setncatts({'units': 'none',
                      'long_name': 'origin model column',
                      'comments': '0 indexed',
                      'missing_value': -9})
    t = [e[1] for e in values]
    orgcol[:] = t

    # actual data
    # component id
    component = nc_file.createVariable('component', 'i4', ('layer', 'row', 'col', 'component'), fill_value=-1)
    component.setncatts({'units': 'none',
                         'long_name': 'component ids for that cell',
                         'comments': 'mapped to cell location from orgid,orglayer, etc.',
                         'missing_value': -1})
    fraction = nc_file.createVariable('fraction', 'i4', ('layer', 'row', 'col', 'component'), fill_value=-1)
    fraction.setncatts({'units': 'none',
                        'long_name': 'component fraction for that cell',
                        'comments': 'mapped to cell location from component id and orgid,orglayer, etc.',
                        'missing_value': -1})

    ibnd = smt.get_no_flow()
    # add data #todo speed up? ask mike
    all_keys = data.index.levels[0]
    for layer, row, col in itertools.product(range(smt.layers), range(smt.rows), range(smt.cols)):
        key = '{:02d}_{:03d}_{:03d}'.format(layer, row, col)
        if key not in all_keys:  # no data here
            continue
        temp_data = data.loc[key]
        end = len(temp_data.index)
        # the 0:end is necessary as this forces the netcdf to expand the unbounded dimension
        component[layer,row,col,0:end] = temp_data.index.values
        fraction[layer,row,col,0:end] = temp_data[u'Particle_ID'].values


if __name__ == '__main__':
    save_emulator(r"C:\Users\MattH\Desktop\NsmcBase_modpath_tester\NsmcBase_modpath_tester_mp.mppth",
                  r"C:\Users\MattH\Desktop\NsmcBase_modpath_tester\test_emulator.csv")
