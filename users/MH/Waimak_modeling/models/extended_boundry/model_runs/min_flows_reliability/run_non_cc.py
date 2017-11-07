# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 19/10/2017 4:29 PM
"""

from __future__ import division
from core import env
import os
import time
from future.builtins import input

from forward_runs import setup_run_args, run_forward_runs
from extract_data_for_forward_runs import gen_all_outdata_forward_runs, extract_and_save_all_cc_mult_missing_w
from visualise_data_from_fruns import plot_and_save_forward_vis
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.transfer_readme import trans_readme


if __name__ == '__main__':
    # a convinence set to run everything for the forward runs
    # note that the well reliablity scripts where not completed at the time that this was run so are not included here
    # and must be run separately, if I need to re-run the scripts at a later date include this item
    # this is meant to easily run the forward runs for the non cc senarios

    # done on rdsprod03
    #### todo inputs to define for each run####
    safemode = True
    run_modelses = [True]
    model_ids = ['NsmcBase', 'StrOpt']  # todo update
    date = '2017_11_07'

    #### run the models ####
    for model_id, run_models in zip(model_ids, run_modelses):
        model_dir_path = r"D:\mh_model_runs\{}_non_cc_forward_runs_{}".format(model_id,date)  # path on rdsprod03
        results_dir = r"D:\mh_model_runs\{}_non_cc_forward_runs_{}_results".format(model_id,date)
        cc_to_waimak_only = False
        notes = """ 
        Naturalisation changes applied to full domain, No climate change senarios run carpet drains removed 
        if not explicit in the model name (_w_ncar)
        pumping changes only applied to Waimakariri, run in {}
        """.format(model_dir_path)
        if run_models:
            if safemode:
                if os.path.exists(model_dir_path):
                    cont = input(
                        'run all forward runs, this could overwrite item in :\n {} \n continue y/n\n'.format(
                            model_dir_path)).lower()
                    if cont != 'y':
                        raise ValueError('script aborted so as not to potentially overwrite {}'.format(model_dir_path))

            if not os.path.exists(model_dir_path):
                os.makedirs(model_dir_path)
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            runs = setup_run_args(model_id, model_dir_path, cc_to_waimak_only=cc_to_waimak_only, cc_runs=False)

            t = time.time()
            run_forward_runs(runs, model_dir_path, notes)
            print('{} runs in __ min'.format(len(runs)))
            print((time.time() - t) / 60)

        gen_all_outdata_forward_runs(
            model_dir_path,
            results_dir,
            False)
        extract_and_save_all_cc_mult_missing_w(model_dir_path, os.path.join(results_dir, "ccmult_extract.csv"))
        print('done')
        trans_readme(model_dir_path,results_dir)
        plot_and_save_forward_vis(
            os.path.join(results_dir, "overview_plots"),
            os.path.join(results_dir, "{}_relative_data.csv".format(model_id)),
            os.path.join(results_dir, "{}_meta_data.csv".format(model_id)),
            cc_runs=False, pc5_comp=True
        )
