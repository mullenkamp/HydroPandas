"""
Author: matth
Date Created: 1/06/2017 11:58 AM
"""
from __future__ import division
import flopy
import pandas as pd
from base_sd_runs import get_str_dep_base_path, get_sd_spv
import glob
import itertools
import numpy as np
import os
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction.data_from_streams import \
    get_samp_points_df, get_flux_at_points
from users.MH.Waimak_modeling.models.extended_boundry.supporting_data_analysis.all_well_layer_col_row import \
    get_all_well_row_col
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_bc_data.wells import \
    get_full_consent, get_max_rate

def calc_stream_dep(model_path, sd_version='sd150'):
    """
    calculate a dataframe for the stream depletion
    :param model_path: path to the modflow model namefile with or without extension
    :param sd_version: either sd150 sd30 sd7,  this defines which base mod path to use.
    :return: series with index of sites defined below
    """
    model_path = model_path.replace('.nam', '')
    model_id = os.path.basename(model_path).split('_')[0]

    if sd_version not in ["sd150", "sd30", "sd7"]:
        raise ValueError(
            'unexpected argument for version {} expected one of ["sd150", "sd30", "sd7"]'.format(sd_version))
    baseline_path = get_str_dep_base_path(model_id, sd_version)

    # id the kstpkpers and the integrater value
    spv = get_sd_spv(sd_version)
    kstpkpers = list(itertools.product(range(spv['nstp']), range(0, spv['nper'])))
    integrater = spv['perlen'] / spv['nstp']

    # define sites
    samp_points_df = get_samp_points_df()
    sites = list(samp_points_df[samp_points_df.m_type == 'swaz'].index)

    str_base = get_flux_at_points(sites=sites, base_path=baseline_path, kstpkpers=kstpkpers)
    str_sd = get_flux_at_points(sites=sites, base_path=model_path, kstpkpers=kstpkpers)

    outdata = (str_base.sum(axis=1) - str_sd.sum(axis=1)) * integrater  # todo check sign

    # cumulative well abstraction budget #todo check but I think this is ok
    budget = flopy.utils.MfListBudget('{}.list'.format(model_path))
    temp = pd.DataFrame(budget.get_cumulative('WELLS_OUT'))
    abs_vol = temp[(temp['stress_period'] == temp['stress_period'].max()) &
                   (temp['time_step'] == temp['time_step'].max())]['WELLS_OUT'].iloc[0]

    if abs_vol == 0:
        outdata *= np.nan
    else:
        outdata *= 1 / abs_vol
    return outdata, abs_vol/(spv['perlen'] * spv['nspt'])  # todo check/debug

def calc_str_dep_all_wells(out_path, base_path, sd_version='sd150'):
    """

    :param out_path: the path to save the csv to
    :param base_path: path to the well by well stream depletion path
    :param sd_version: either sd150 sd30 sd7,  this defines which base mod path to use.
    :return: dataframe with index fo wells (defined from file names) and columns of sites defined in calc_stream_dep
    """
    all_paths = glob.glob('{}/*/*.nam'.format(base_path))
    all_paths = [e.replace('.nam', '') for e in all_paths]
    wells = ['{}/{}'.format(e.split('_')[-2], e.split('_')[-1]) for e in all_paths]  # todo check this is correct name
    model_id = os.path.basename(all_paths[0]).split('_')[0]  # todo check this returns the right model id
    outdata = {}
    out_per_abs_vol = {}
    for well, path in zip(wells, all_paths):
        sd, per_abs_vol = calc_stream_dep(path, sd_version=sd_version)
        outdata[well] = sd
        out_per_abs_vol[well] = per_abs_vol
    outdata = pd.DataFrame(outdata).transpose()
    out_per_abs_vol = pd.DataFrame(out_per_abs_vol, index=['model_abs_rate']).transpose()

    outdata = pd.merge(outdata, out_per_abs_vol)

    # add additional information
    # add flux
    if sd_version == 'sd150':
        flux = get_full_consent(model_id).loc['flux']  # todo should this be scaled
    else:
        flux = get_max_rate(model_id).loc['flux']
    outdata = pd.merge(outdata, pd.DataFrame(flux), how='left', left_index=True, right_index=True)

    all_wells = get_all_well_row_col()
    outdata = pd.merge(outdata,
                       all_wells.loc[:, ['nztmx', 'nztmy', 'depth', 'mid_screen_elv', 'mx', 'my', 'mz']],
                       how='left', left_index=True, right_index=True)
    outdata['hor_misloc'] = ((outdata['mx'] - outdata['nztmx']) ** 2 + (outdata['my'] - outdata['nxtmy']) ** 2) ** 0.5
    outdata['ver_misloc'] = outdata['mz'] - outdata['mid_screen_elv']

    # save with header
    header = (
    'all sd values are relative to the flux; though nwt sometimes reduces a wells pumping rate if it goes dry;'
    'the average abstracted rate is noted in model_abs_rate(m3/day).  m(x;y) (e.g. modelx) and nztm(z;y) are in nztm. '
    'mid_screen_elv; mz are in m lyttleton; hor_misloc and vert_misloc are in m and vert_misloc is mz - mid_screen_elv')
    'flux is in m3/day'
    with open(out_path, 'w') as f:
        f.write(header)
    outdata.to_csv(out_path, mode='a') # todo check/debug


if __name__ == '__main__':
    print('done')