# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 28/09/2017 2:00 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction import \
    get_hds_at_wells, get_flow_at_points, get_samp_points_df
from glob import glob
import os
import datetime
import itertools


def extract_forward_run(name_file_path):
    wells = []
    streams = get_samp_points_df()
    streams = streams.loc[streams.m_type == 'min_flow'].index

    hd_data = get_hds_at_wells(wells, rel_kstpkpers=0, name_file_path=name_file_path,
                               add_loc=True)
    hd_data = hd_data.rename(columns={'hd_m3d_kstp0_kper0':'kstpkper0'})
    str_data = get_flow_at_points(streams, rel_kstpkpers=0, base_path=name_file_path)
    str_data = str_data.rename(columns={'flow_m3d_kstp0_kper0':'kstpkper0'})
    outdata = pd.concat((hd_data, str_data))
    return outdata


def extract_and_save_all_forward_runs(forward_run_dir, outpath):
    paths = glob(os.path.join(forward_run_dir, '*/*.nam'))
    model_id = os.path.basename(paths[0]).split('_')[0]
    outpath = os.path.join(os.path.dirname(outpath),'{}_{}'.format(model_id,os.path.basename(outpath)))
    for i, path in enumerate(paths):
        temp_name = os.path.basename(path).replace('.nam', '').replace(model_id,'')
        temp = extract_forward_run(path)
        temp = temp.rename({'kstpkper0':temp_name})
        if i == 0:
            outdata = temp
        else:
            temp = temp.drop(['k', 'i', 'j', 'x', 'y', 'z'], 1)
            outdata = pd.merge(outdata, temp, right_index=True, left_index=True)
    with open(outpath,'w') as f:
        f.write('flow and flux valuse from model {}. all flow values in m3/day, all hd, z in m, x,y in nztm, '
                'i,j,k are unit less, made {}\n'.format(model_id, datetime.datetime.now().isoformat()))
    outdata.to_csv(outpath, mode='a')

def make_rel_data(data_path, out_path):
    org_data = pd.read_csv(data_path,skiprows=1) #todo check
    model_id = os.path.basename(data_path).split('_')[0]
    out_path = os.path.join(os.path.dirname(out_path),'{}_{}'.format(model_id,os.path.basename(out_path)))
    # go from extract and save to relative
    # cc relative to RCP past (consistant amalg type, rcm) really perhaps not constant amalg type
    # leave RCP past in units of m and days

    # all others are relavive to current,
    # leave current in units of m and days

    #do not modify x,y,z
    outdata = None #todo
    # write doc string
    with open(out_path,'w') as f:
        f.write('relative flow and flux valuse from model {}. all cc relative to the appropriate amalg type and rcm of RCP past'
                'others are relative to current, for current and RCP past all flow values in m3/day, all hd, z in m, x,y in nztm, '
                'i,j,k are unit less, made: {}\n'.format(model_id,datetime.datetime.now().isoformat()))
    # write data
    outdata.to_csv(out_path, mode='a')

    raise NotImplementedError

print 'done'
