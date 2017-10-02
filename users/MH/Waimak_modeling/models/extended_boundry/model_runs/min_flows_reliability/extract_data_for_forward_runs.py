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
from copy import deepcopy
import flopy
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.convergance_check import \
    zipped_converged
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt


def extract_forward_run(name_file_path):
    wells = [
        'M34/0306',
        'L35/0062',
        'M35/0538',
        'M35/5445',
        'L35/0686',
        'M35/9154',
        'M35/11283',
        'M35/0058',
        'M35/4873',
        'BW23/0133',
        'BW23/0134',
        'M35/6295'
    ]
    streams = get_samp_points_df()
    streams = streams.loc[streams.m_type == 'min_flow'].index

    hd_data = get_hds_at_wells(wells, rel_kstpkpers=0, name_file_path=name_file_path,
                               add_loc=True)
    hd_data = hd_data.rename(columns={'hd_m3d_kstp0_kper0': 'kstpkper0'})
    str_data = get_flow_at_points(streams, rel_kstpkpers=0, base_path=name_file_path)
    str_data = str_data.rename(columns={'flow_m3d_kstp0_kper0': 'kstpkper0'})
    outdata = pd.concat((hd_data, str_data))
    return outdata


def extract_forward_metadata(forward_run_dir, outpath):
    paths = glob(os.path.join(forward_run_dir, '*/*.nam'))
    model_id = os.path.basename(paths[0]).split('_')[0]
    outpath = os.path.join(os.path.dirname(outpath), '{}_{}'.format(model_id, os.path.basename(outpath)))
    model_names = [os.path.basename(path).replace('.nam', '').replace(model_id + '_', '') for path in paths]
    converged = [zipped_converged(path, return_nans=True) for path in paths]
    outdata = pd.DataFrame(index=model_names, data={'converged': converged, 'path': paths})
    outdata.to_csv(outpath)
    return outpath


def extract_and_save_all_forward_runs(forward_run_dir, outpath):
    paths = glob(os.path.join(forward_run_dir, '*/*.nam'))
    paths = sorted(paths)
    model_id = os.path.basename(paths[0]).split('_')[0]
    outpath = os.path.join(os.path.dirname(outpath), '{}_{}'.format(model_id, os.path.basename(outpath)))
    for i, path in enumerate(paths):
        temp_name = os.path.basename(path).replace('.nam', '').replace(model_id + '_', '')
        temp = extract_forward_run(path)
        temp = temp.rename(columns={'kstpkper0': temp_name})
        if i == 0:
            outdata = temp.loc[:,['depth', 'i', 'j', 'k', 'mid_screen_elv', 'nztmx', 'nztmy',temp_name]]
        else:
            temp = temp.drop(['depth', 'i', 'j', 'k', 'mid_screen_elv', 'nztmx', 'nztmy'], 1)
            outdata = pd.merge(outdata, temp, right_index=True, left_index=True)
    with open(outpath, 'w') as f:
        f.write('flow and flux values from model {}. all flow values in m3/day; all hd; z in m; x; y in nztm, '
                'i;j;k are unit less; made {}\n'.format(model_id, datetime.datetime.now().isoformat()))
    outdata.index.name = 'site'
    outdata.to_csv(outpath, mode='a')
    return outpath


def make_rel_data(data_path, out_path):
    org_data = pd.read_csv(data_path, skiprows=1, index_col=0)  # todo check
    model_id = os.path.basename(data_path).split('_')[0]
    out_path = os.path.join(os.path.dirname(out_path), '{}_{}'.format(model_id, os.path.basename(out_path)))
    current_key = 'current'
    remove_keys = [current_key, 'depth', 'i', 'j', 'k', 'mid_screen_elv', 'nztmx', 'nztmy']
    divide_keys = list(set(org_data.keys()) - set(remove_keys))
    # todo how to handle wells for now the same
    outdata = deepcopy(org_data)
    for key in divide_keys:
        outdata.loc[:, key] *= 1 / org_data.loc[:, current_key]
    with open(out_path, 'w') as f:
        f.write(
            'relative flow and flux values from model {mid}. all values relative to the model_period {cid}'
            ' for {cid} all flow values in m3/day; all hd; z in m; x; y in nztm'
            ' i;j;k are unit less, made: {dt}\n'.format(mid=model_id, cid=current_key,
                                                        dt=datetime.datetime.now().isoformat()))
    # write data
    outdata.to_csv(out_path, mode='a')

def get_baseline_path(meta_data, name): #todo compare CC LSR to it's baseline period
    # keep senario and rcm constant
    raise NotImplementedError

def plt_drawdown(meta_data_path, outdir):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    meta_data = pd.read_csv(meta_data_path, index_col=0)
    mod_per_hds = flopy.utils.HeadFile(meta_data.loc['current', 'path'].replace('.nam', '.hds')).get_data((0, 0))
    for name in meta_data.index:
        converged = meta_data.loc[name, 'converged']
        if pd.isnull(converged):
            continue
        plt_out_dir = os.path.join(outdir, name)
        if not os.path.exists(plt_out_dir):
            os.makedirs(plt_out_dir)

        hd_file_path = meta_data.loc[name, 'path'].replace('.nam', '.hds')
        hds = flopy.utils.HeadFile(hd_file_path).get_data((0, 0))
        # set no flow and dry cells to nan, in plotting, the drycells with appear green, the others will appear black
        hds[hds > 1e20] = np.nan
        hds[hds < -666] = np.nan
        hds = hds - mod_per_hds
        for layer in range(smt.layers):
            fig, ax = smt.plt_matrix(hds[layer], vmin=-5, vmax=5,
                                     title='draw_down for {}, converged: {}, layer {:02d}'.format(name, converged,
                                                                                                  layer),
                                     no_flow_layer=layer, cmap='RdBu')
            ax.set_facecolor('g')
            fig.savefig(os.path.join(plt_out_dir, 'drawdown_layer_{:02d}.png'.format(layer)))
            plt.close(fig)


def gen_all_outdata_forward_runs(forward_run_dir, outdir, plt_dd=False):
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    absolute_outpath = 'absolute_data.csv'
    meta_data_path = 'meta_data.csv'
    relative_outpath = 'relative_data.csv'
    absolute_outpath = extract_and_save_all_forward_runs(forward_run_dir, os.path.join(outdir, absolute_outpath))
    meta_data_path = extract_forward_metadata(forward_run_dir, os.path.join(outdir, meta_data_path))
    make_rel_data(absolute_outpath, os.path.join(outdir, relative_outpath))

    if plt_dd:
        plt_drawdown(meta_data_path, os.path.join(outdir, 'plots'))


if __name__ == '__main__':
    gen_all_outdata_forward_runs(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\forward_sw_gw\runs\forward_runs_2017_09_30",
                                 r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\forward_sw_gw\results\cc_only_to_waimak",
                                 True)
    print 'done'  # todo debug this shit
