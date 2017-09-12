# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/09/2017 5:33 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
import flopy
import os
from warnings import warn

hds_no_data = -999.99 #todo check with BRIOCH this has changed

def get_well_positions(well_list, nan_handeling='raise'): #todo
    """

    :param well_list: list of well numbers or a well number
    :param nan_handeling: one of: 'raise' : raise an exception for any nan values with well numbers
                                  'forgive': silently remove nan values
                                  'warn': remove nan values, but issue a warning with well numbers
    :return: dataframe of well: k,i,j,x,y,z
    """
    well_list = np.atleast_1d(well_list)
    #todo how to handle NAN values? flag either raise an exception, warning, or silently remove
    #todo make sure to only return values in active cells (no CHB)
    raise NotImplementedError

def get_hds_file_path(name_file_path=None, hds_path=None, m=None):
    loc_inputs = 0
    if hds_path is not None:
        loc_inputs += 1
    if name_file_path is not None:
        loc_inputs += 1
        with open(name_file_path) as f:
            name_file_data = pd.Series(f.readlines())
        temp2 = name_file_data.loc[name_file_data.str.contains('.hds')]
        if len(temp2) != 1:
            raise ValueError('could not identify heads file from name_file')
        temp2 = pd.Series(temp2.iloc[0].split(' '))
        temp2 = temp2.loc[temp2.str.contains('.hds')]
        if len(temp2) != 1:
            raise ValueError('could not identify heads file from name_file')
        hds_path = '{}/{}'.format(os.path.dirname(name_file_path),temp2.iloc[0])
    if m is not None:
        loc_inputs += 1
        temp = np.array(m.output_fnames)
        temp2 = temp.loc[temp.str.contains('.hds')]
        if len(temp2) != 1:
            raise ValueError('could not identify heads file from model')
        hds_path = '{}/{}'.format(m.model_ws,temp2.iloc[0])

    if loc_inputs == 0:
        raise ValueError('must define one of: name_file_path, hds_path, m')
    elif loc_inputs > 1:
        raise ValueError('must define only one of: name_file_path, hds_path, m')
    return hds_path

def get_hds_at_wells(well_list, kstpkpers=None, rel_kstpkpers=None, name_file_path=None, hds_path=None, m=None, add_loc=False): #todo
    """
    return dataframe of heads at wells in well list
    :param well_list: list of well numbers to export data at
    :param kstpkpers: a list or tuple of A tuple containing the time step and stress period (kstp, kper).
                      These are zero-based kstp and kper values. kstpkpers or relative kstpkpers must be assigned
    :param rel_kstpkpers: a list or integer of the relative kstpkpers to be used (as list indexing) or 'all'
    :param name_file_path: None or the path to the name file, must define one of name_file_path, hds_path, m
    :param hds_path:  None or the path to the heads file, must define one of name_file_path, hds_path, m
    :param m: None or a flopy model object, must define one of name_file_path, hds_path, m
    :param add_loc: boolean if true add k,i,j, x,y,z to outdata
    :return: df of heads (rows: wells  columns:kstpkpers)
    """
    # set up inputs and outdata
    hds_path = get_hds_file_path(name_file_path=name_file_path, hds_path=hds_path, m=m)
    hds_file = flopy.utils.HeadFile(hds_path)

    kstpkpers = _get_kstkpers(hds_file, kstpkpers, rel_kstpkpers)

    kstpkper_names = ['{}_{}'.format(e[0],e[1]) for e in kstpkpers]
    well_locs = get_well_positions(well_list)
    outdata = pd.DataFrame(index=well_list,columns=kstpkper_names)
    if add_loc:
        outdata = pd.merge(outdata,well_locs,right_index=True,left_index=True)

    hds = hds_file.get_alldata(nodata=hds_no_data)
    #TODO how to handle the ntimes component of hds for the indexing... check some stuff with old models
    #todo how to efficiently get data out


    raise NotImplementedError
    return outdata

def get_con_at_wells(well_list, unc_file_path, kstpkpers=None, rel_kstpkpers=None, add_loc=False): #todo
    """

    :param well_list: list of well numbers to export data at
    :param unc_file_path:
    :param kstpkpers: a list or tuple of A tuple containing the time step and stress period (kstp, kper).
                      These are zero-based kstp and kper values. must define only one of kstpkpers or rel_kstpkpers
    :param rel_kstpkpers: a list or integer of the relative kstpkpers to be used (as list indexing) or 'all'
    :param add_loc: boolean if true add k,i,j, x,y,z to outdata
    :return: df of heads (rows: wells  columns:kstpkpers)
    """
    unc_file = flopy.utils.UcnFile(unc_file_path)

    kstpkpers = _get_kstkpers(unc_file, kstpkpers, rel_kstpkpers)
    kstpkper_names = ['{}_{}'.format(e[0],e[1]) for e in kstpkpers]
    well_locs = get_well_positions(well_list)
    outdata = pd.DataFrame(index=well_list,columns=kstpkper_names)
    if add_loc:
        outdata = pd.merge(outdata,well_locs,right_index=True,left_index=True)

    con_data = unc_file.get_alldata()
    #todo this will be the same for the hds file and unc file



    raise NotImplementedError

def _get_kstkpers(bud_file, kstpkpers, rel_kstpkpers):
    if kstpkpers is not None and rel_kstpkpers is not None:
        raise ValueError('must define only one of kstpkpers or rel_kstpkpers')
    elif kstpkpers is not None:
        kstpkpers = np.atleast_2d(kstpkpers)
        if kstpkpers.shape[1] != 2:
            raise ValueError('must define both kstp and kper in kstpkpers')
    elif rel_kstpkpers is not None:
        if rel_kstpkpers == 'all':
            kstpkpers = np.atleast_2d(bud_file.get_kstpkper())
        else:
            rel_kstpkpers = np.atleast_1d(rel_kstpkpers)
            if rel_kstpkpers.ndim != 1:
                raise ValueError ('too many dimensions for rel_kstpkpers')
            temp = bud_file.get_kstpkper()
            kstpkpers = np.atleast_2d([temp[e] for e in rel_kstpkpers])
    elif kstpkpers is None and rel_kstpkpers is None:
        raise ValueError('must define one of kstpkpers or rel_kstpkpers')

    return kstpkpers