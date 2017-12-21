# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 19/10/2017 4:48 PM
"""

from __future__ import division
from core import env
import os
import time
from well_by_well_str_dep_sd7 import get_sd_well_list, well_by_well_depletion_sd7
from well_by_well_str_dep_sd30 import well_by_well_depletion_sd30
from well_by_well_str_dep_sd150 import well_by_well_depletion_sd150
from extract_stream_depletion import calc_str_dep_all_wells
import shutil
from sd_metadata import save_sd_metadata
from starting_hds_ss_sy import get_ss_sy

def run_full_non_grid_sd(model_id, model_base_path, data_outdir, ss_sy_version, notes=''):
    """
    a conveninance function to run all of the non-well stream depletion assesments and data extraction Note that this
    does not keep the model file, those are deleted after data extraction
    :param model_id: which nsmc realisation to use
    :param model_base_path: the base path to put the models
    :param data_outdir: the base path to put the data
    :param ss_sy_version: which version of ss_sy to use see get_ss_sy()
    :param notes: any notes to write to a readme file
    :return:
    """

    description = get_ss_sy(ss_sy_version=ss_sy_version, return_description=True)
    sd7_notes = sd30_notes = sd150_notes = notes + '\nss_sy version: {}\n ss_sy_description:{}'.format(ss_sy_version,
                                                                                                       description)
    # below should not change
    if not os.path.exists(model_base_path):
        os.makedirs(model_base_path)
    if not os.path.exists(data_outdir):
        os.makedirs(data_outdir)

    with open(os.path.join(data_outdir,'READ_ME.txt'),'w') as f:
        f.write(sd7_notes)

    well_list = get_sd_well_list(model_id)[0:10] #todo DADB

    #### run the models ####
    sd7_base_path = os.path.join(model_base_path, 'sd7')
    sd30_base_path = os.path.join(model_base_path, 'sd30')
    sd150_base_path = os.path.join(model_base_path, 'sd150')

    t = time.time()
    # sd 7
    well_by_well_depletion_sd7(model_id, well_list, sd7_base_path, sd7_notes, ss_sy_version=ss_sy_version)
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd7.csv"), sd7_base_path, 'sd7')
    save_sd_metadata(os.path.join(data_outdir, "sd7_metadata.csv"), sd7_base_path)
    shutil.rmtree(sd7_base_path)

    # sd 30
    well_by_well_depletion_sd30(model_id, well_list, sd30_base_path, sd30_notes, ss_sy_version=ss_sy_version)
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd30.csv"), sd30_base_path, 'sd30')
    save_sd_metadata(os.path.join(data_outdir, "sd30_metadata.csv"), sd30_base_path)
    shutil.rmtree(sd30_base_path)

    # sd 150
    well_by_well_depletion_sd150(model_id, well_list, sd150_base_path, sd150_notes, ss_sy_version=ss_sy_version)
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd150.csv"), sd150_base_path, 'sd150')
    save_sd_metadata(os.path.join(data_outdir, "sd150_metadata.csv"), sd150_base_path)
    shutil.rmtree(sd150_base_path)

    print('done after {} minutes for {} model runs'.format((time.time() - t) / 60, len(well_list*3)))



if __name__ == '__main__':

    model_id = 'NsmcBase'
    model_base_path = r"D:\mattH\python_wm_runs\sd_runs\{}_2017_12_21_low".format(model_id)
    data_outdir = r"D:\mattH\python_wm_runs\sd_runs\data_{}_2017_12_21_low".format(model_id)
    run_full_non_grid_sd(model_id,model_base_path,data_outdir,ss_sy_version=2, notes='a test, not all wells are present, no carpet drains,')
    #todo set up a high, medium, low storage coeffients versions and a running directory
    # #todo the differention in file names is only at the directory level
