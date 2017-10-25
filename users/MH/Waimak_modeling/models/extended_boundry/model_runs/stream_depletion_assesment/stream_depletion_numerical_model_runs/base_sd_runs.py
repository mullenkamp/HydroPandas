# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 6/10/2017 8:58 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.stream_depletion_assesment.stream_depletion_numerical_model_runs.stream_depletion_model_setup import setup_and_run_stream_dep
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.stream_depletion_assesment.stream_depletion_numerical_model_runs.starting_hds_ss_sy import get_ss_sy, get_sd_starting_hds
import os
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.realisation_id import temp_pickle_dir

def get_str_dep_base_path(model_id, sd_version, recalc=False):
    """
    function to return the model path for the fully naturalized fully transient run and ensure that the model has been
    run this run has a ss and then full year run from july to end of june  ## for SD 7
    :param model_id: which NSMC realisation to use
    :param sd_version: one of ["sd150", "sd30", "sd7"]
    :param recalc: if True recreate and run the base model
    :return: path to the model (without an extension)
    """
    if sd_version not in ["sd150", "sd30", "sd7"]:
        raise ValueError('unexpected argument for version {} expected one of ["sd150", "sd30", "sd7"]'.format(sd_version))

    base_path = os.path.join(temp_pickle_dir,'base_sd_runs')
    name = '{}_base_run'.format(sd_version)
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    cbc_path = os.path.join(base_path,'{}_{}'.format(model_id,name),'{}_{}.cbc'.format(model_id,name))
    if os.path.exists(cbc_path) and not recalc:
        return cbc_path.replace('.cbc', '')


    ss,sy = get_ss_sy()
    start_heads = get_sd_starting_hds(model_id,sd_version)

    spv = get_sd_spv(sd_version)


    base_kwargs = {
        'model_id': model_id,
        'base_dir': base_path, # this is the directory to put the model folder
        'stress_vals': spv,
        'ss': ss,
        'sy': sy,
        'silent': True,
        'start_heads': start_heads,
        'sd_7_150': sd_version,
        'wells_to_turn_on': {0:[]},
        'name': name} # model id is added internally

    name, success = setup_and_run_stream_dep(**base_kwargs)

    if not os.path.exists(cbc_path):
        raise ValueError('for some reason the cbc path did not write')

    if success != 'converged':
        os.remove(cbc_path) # to prevent it from returing the path on a future run
        raise ValueError('base model did not converge')

    return cbc_path.replace('.cbc', '')

def get_sd_spv(sd_version):
    """
    get the stress period values for the sd assesment
    :param sd_version: one of ["sd150", "sd30", "sd7"]
    :return:
    """

    if sd_version == 'sd150':
        spv = {'nper': 5,
           'perlen': 30,
           'nstp': 2,
           'steady': [False, False, False, False, False],
           'tsmult': 1.}
    elif sd_version == 'sd7':
        spv = {'nper': 7,
               'perlen': 1,
               'nstp': 1,
               'steady': [False, False, False, False, False, False, False],
               'tsmult': 1.}
    elif sd_version == 'sd30':
        spv = {'nper': 10,
               'perlen': 3,
               'nstp': 1,
               'steady': [False, False, False, False, False, False, False, False, False, False],
               'tsmult': 1.}
    else:
        raise ValueError('unexpected argument for version {} expected one of ["sd150", "sd30", "sd7"]'.format(sd_version))

    return spv
