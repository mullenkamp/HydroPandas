# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 9/10/2017 11:45 AM
"""

from __future__ import division
from core import env
import numpy as np
from copy import deepcopy
import flopy
import os
import sys
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools import mod_gns_model, get_max_rate, \
    get_full_consent, get_race_data, zip_non_essential_files
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.convergance_check import converged
from traceback import format_exc

def setup_and_run_ss_grid_stream_dep_multip(kwargs):
    """
    quick wrapper to make it easier to feed the below function to a multiprocessing script
    :param kwargs:
    :return:
    """
    try:
        name, success = setup_and_run_ss_grid_stream_dep(**kwargs)
        print(name, success)
        return name, success
    except Exception as val:
        name = kwargs['name']
        success = format_exc().replace('\n', '')
    return name, success

#todo this was just a play and is not finished

def setup_and_run_ss_grid_stream_dep(model_id, name, base_dir, wells_to_turn_on,
                                     silent=True, start_heads=None,grid=False, grid_rate=None):
    """
    set up and run the stream depletion modflow models will write log to the base_dir
    :param model_id: the model id for the NSMC realisation
    :param name: name of the model
    :param base_dir: the directory to place the folder containing the model
    :param wells_to_turn_on: list well names to turn on e.g. if want no wells to be turned on pass an empty list
    :param silent: passed to m.run_model.  if true Mirror modflow information to consol
    :param start_heads: starting heads to pass to the model
    :param grid: boolean if false pull well data from CAV, if True pull from the grid and use grid pumping
    :param grid_rate: the pumping rate for the grid wells

    :return:
    """
    if grid:
        raise NotImplementedError('grid not yet implemented')

    # check inputs are dictionaries
    for input_arg in ['wells_to_turn_on']:
        if not isinstance(eval(input_arg), list):
            raise ValueError('incorrect input type for {} expected list'.format(input_arg))

    wells = {}

    base_well = get_race_data(model_id)
    full_consent = get_full_consent(model_id)
    # set up wells
    input_wells = deepcopy(base_well)
    for well in wells_to_turn_on:
        add_well = full_consent.loc[well] #todo should this be the full consent over 150 days or over 365 etc.
        input_wells.loc[well] = add_well

    wells[0] = smt.convert_well_data_to_stresspd(input_wells)

    m = mod_gns_model(model_id, name, '{}/{}'.format(base_dir, name),
                      safe_mode=False,
                      well=wells,
                      mt3d_link=False,
                      start_heads=start_heads)

    # below included for easy manipulation
    flopy.modflow.mfnwt.ModflowNwt(m,
                                   headtol=1e-5,
                                   fluxtol=500,
                                   maxiterout=100,
                                   thickfact=1e-05,
                                   linmeth=1,
                                   iprnwt=0,
                                   ibotav=0,
                                   options='COMPLEX',
                                   Continue=False,
                                   dbdtheta=0.4,  # only when options is specified
                                   dbdkappa=1e-05,  # only when options is specified
                                   dbdgamma=0.0,  # only when options is specified
                                   momfact=0.1,  # only when options is specified
                                   backflag=1,  # only when options is specified
                                   maxbackiter=50,  # only when options is specified
                                   backtol=1.1,  # only when options is specified
                                   backreduce=0.7,  # only when options is specified
                                   maxitinner=50,  # only when options is specified
                                   ilumethod=2,  # only when options is specified
                                   levfill=5,  # only when options is specified
                                   stoptol=1e-10,  # only when options is specified
                                   msdr=15,  # only when options is specified
                                   iacl=2,  # only when options is specified
                                   norder=1,  # only when options is specified
                                   level=5,  # only when options is specified
                                   north=7,  # only when options is specified
                                   iredsys=0,  # only when options is specified
                                   rrctols=0.0,  # only when options is specified
                                   idroptol=1,  # only when options is specified
                                   epsrn=0.0001,  # only when options is specified
                                   hclosexmd=0.0001,  # only when options is specified
                                   mxiterxmd=50,  # only when options is specified
                                   unitnumber=714)

    # write inputs and run the model and write output to a log
    m.write_input()
    m.write_name_file()
    if silent:
        print('starting to run model {}'.format(name))
        sys.stdout.flush()
    success, buff = m.run_model(silent=silent, report=True)

    # write a log of the buffer
    log_dir = '{}/logging'.format(base_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log = '{}/{}_log.txt'.format(log_dir, name)
    buff = [e + '\n' for e in buff]
    with open(log, 'w') as f:
        f.writelines(buff)

    # get success and zip files I don't need for this analysis
    con = None
    if success:
        con = converged(os.path.join(m.model_ws,m.namefile.replace('.nam', '.list')))
        zip_non_essential_files(m.model_ws, include_list=False, other_files=['.sfo','.ddn']) #todo  are there others I can incorporate? .ddn? .hds?
    if con is None:
        success = 'convergence unknown'
    elif con:
        success = 'converged'
    else:
        success = 'did not converge'
    return name, success
    # todo this needs debugging


def grid_wells (): #set up a grid
    raise NotImplementedError

if __name__ == '__main__':
    print('done')