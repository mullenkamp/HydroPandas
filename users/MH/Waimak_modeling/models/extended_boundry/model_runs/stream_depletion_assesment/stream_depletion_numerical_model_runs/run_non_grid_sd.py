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

if __name__ == '__main__':
    # a conveninance function to run all of the non-well stream depletion assesments and data extraction

    #### todo update below parameters ####
    # on DHI-runs02
    model_id = 'StrOpt'

    sd7_notes = """ """
    sd30_notes = """ """
    sd150_notes = """ """


    model_base_path = r"D:\mattH\python_wm_runs\sd_runs\{}_2017_10_20".format(model_id)
    data_outdir = r"D:\mattH\python_wm_runs\sd_runs\data_{}_2017_10_20".format(model_id)
    run_models = True

    # below should not change
    well_list = get_sd_well_list(model_id)

    #### run the models ####
    sd7_base_path = os.path.join(model_base_path, 'sd7')
    sd30_base_path = os.path.join(model_base_path, 'sd30')
    sd150_base_path = os.path.join(model_base_path, 'sd150')

    if run_models:
        t = time.time()

        well_by_well_depletion_sd7(model_id, well_list, sd7_base_path, sd7_notes)
        well_by_well_depletion_sd30(model_id, well_list, sd30_base_path, sd30_notes)
        well_by_well_depletion_sd150(model_id, well_list, sd150_base_path, sd150_notes)

        print('done after {} minutes for {} model runs'.format((time.time() - t) / 60, len(well_list) * 3))

    #### extract data ####
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd150.csv"), sd150_base_path, 'sd150')
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd30.csv"), sd30_base_path, 'sd30')
    calc_str_dep_all_wells(os.path.join(data_outdir, "extract_sd7.csv"), sd7_base_path, 'sd7')
