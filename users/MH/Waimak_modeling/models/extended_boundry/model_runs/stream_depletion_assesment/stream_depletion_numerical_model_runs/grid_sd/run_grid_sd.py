# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 19/10/2017 5:04 PM
"""

from __future__ import division
from core import env
import os
import time
from well_by_well_str_dep_ss import well_by_well_depletion_grid
from extract_grid_sd import calc_str_dep_all_wells_grid
from visualise_grid_sd import krig_plot_sd_grid
import socket
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.stream_depletion_assesment.stream_depletion_numerical_model_runs.sd_metadata import save_sd_metadata

if __name__ == '__main__':
    # a convenience function to run all of the grid sd, extract, and visualise the data
    # size requirements: one run (one well) is ~ 47 MB there are one full grid run is ~85 GB
    # run time requirments:

    #### todo update the below parameters ####
    # run on GWruns02
    model_id = 'StrOpt'  # todo re-define
    if socket.gethostname() == 'RDSProd03':
        base_dir = "D:\mh_model_runs\grid_sd_runs\{}_models_2017_10_21".format(model_id)
        data_out_dir = "D:\mh_model_runs\grid_sd_runs\{}_data_2017_10_21".format(model_id)
        run_models = False
        amalg_results = False

        # below should not change
        fluxes = [-100] # todo add -25 when run again
        fluxes = [e * 86.4 for e in fluxes]

    else: # runs on gw02
        run_models = False
        amalg_results = False
        base_dir = "D:\mh_waimak_models\grid_sd_runs\{}_models_2017_10_21".format(model_id)
        data_out_dir = "D:\mh_waimak_models\grid_sd_runs\{}_data_2017_10_21".format(model_id)

        # below should not change
        fluxes = [-5, - 25]
        fluxes = [e * 86.4 for e in fluxes]

    #### run the models ####
    if run_models:
        t = time.time()
        base_paths = []
        for flux in fluxes:
            path = os.path.join(base_dir, 'flux_{}'.format(flux))
            base_paths.append(path)
            well_by_well_depletion_grid(model_id, flux, path, 'one flux of 3, using flux of {} m3/d'.format(flux))

        print('done running models took {} minutes'.format((t-time.time())/60))

    else:
        base_paths = []
        for flux in fluxes:
            path = os.path.join(base_dir, 'flux_{}'.format(flux))
            base_paths.append(path)

    #### extract the data ####
    outpaths = []
    for path in base_paths:
        out_path = os.path.join(data_out_dir,'sd_grid_data_{}.csv'.format(os.path.basename(path)))
        outpaths.append(out_path)
        if amalg_results:
            calc_str_dep_all_wells_grid(out_path, path)

    print('finished extracting data')
    # add metadata
    for path in base_paths: #todo uncomment after debug #todo add to amalg results
        save_sd_metadata(os.path.join(data_out_dir,'sd_grid_metadata_{}.csv'.format(os.path.basename(path))),path)
    # krig and plot all data
    for path in outpaths:
        rd_path = os.path.join(os.path.dirname(path),'{}_{}'.format(model_id,os.path.basename(path)))
        krig_plot_sd_grid(rd_path,data_out_dir)


