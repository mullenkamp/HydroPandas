# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 27/10/2017 10:25 AM
"""

from __future__ import division
from core import env
import netCDF4 as nc
from rrfextract import extractrrf, extractphisummary, extractoptphi
import numpy as np
import pandas as pd
import sys
import datetime
from read_pst import rd_pst_parameters, param_from_rec
from users.MH.Waimak_modeling.models.extended_boundry.supporting_data_analysis.all_well_layer_col_row import \
    get_all_well_row_col

# rap up the NSMC parameters adn observations into a netcdf file
nsmc_dim = 7890 + 2
rch_dim = 46
layer_dim = 11
sfr_dim = 47
khv_dim = 178


def _add_simple_params(param, pst_param, nc_file):
    simple_parameters = {'pump_c': {'units': 'none',
                                    'long_name': 'christchurch west melton pumping multiplier'},
                         'pump_s': {'units': 'none',
                                    'long_name': 'selwyn pumping multiplier'},
                         'pump_w': {'units': 'none',
                                    'long_name': 'waimakariri pumping multiplier'},
                         'sriv': {'units': 'none',
                                  'long_name': 'selwyn river influx multiplier'},
                         'n_race': {'units': 'none',
                                    'long_name': 'waimakariri race multiplier'},
                         's_race': {'units': 'none',
                                    'long_name': 'selwyn race multiplier'},
                         'nbndf': {'units': 'none',
                                   'long_name': 'northern boundary flux multiplier'},
                         'top_e_flo': {'units': 'm3/s',
                                       'long_name': 'top of the eyre flow'},
                         'mid_c_flo': {'units': 'm3/s',
                                       'long_name': 'mid cust (biwash) flow'},
                         'top_c_flo': {'units': 'm3/s',
                                       'long_name': 'top of the cust flow'},
                         'ulrzf': {'units': 'm3/s',
                                   'long_name': 'inland southwestern boundary flux'},
                         'llrzf': {'units': 'm3/s',
                                   'long_name': 'coastal southwestern boundary flux'},
                         'fkh_mult': {'units': 'none',
                                      'long_name': 'fault kh multiplier'},
                         'fkv_mult': {'units': 'none',
                                      'long_name': 'fault kv multiplier'}}
    for key in simple_parameters.keys():
        temp = nc_file.createVariable(key, 'f8', ('nsmc_num',), fill_value=np.nan)
        temp.setncatts({'units': simple_parameters[key]['units'],
                        'long_name': simple_parameters[key]['long_name'],
                        'missing_value': np.nan,
                        'initial': pst_param.loc[key, 'initial'],
                        'lower': pst_param.loc[key, 'lower'],
                        'upper': pst_param.loc[key, 'upper'],
                        })
        temp[:] = param.loc[key].values


def _add_rch_params(param, rch_ppt_tpl, pst_param, nc_file):
    # rch ppts
    rch_meta = pd.read_table(rch_ppt_tpl, skiprows=1, names=['x', 'y', 'group', 'ignore'], delim_whitespace=True)
    rch_ppt_ids = ['rch_ppt_{:02d}'.format(e) for e in range(46)]
    rch_pts = nc_file.createVariable('rch_ppt', str, ('rch_ppt',), fill_value='none')
    rch_pts.setncatts({'units': 'none',
                       'long_name': 'recharge pilot point identifier',
                       'comments': 'this is a unique identifier',
                       'missing_value': 'none'})
    rch_pts[:] = rch_ppt_ids

    rch_x = nc_file.createVariable('rch_ppt_x', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_x.setncatts({'units': 'nztmx',
                     'long_name': 'recharge pilot point longitude',
                     'missing_value': np.nan})
    rch_x[:] = rch_meta.loc[:, 'x'].values

    rch_y = nc_file.createVariable('rch_ppt_y', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_y.setncatts({'units': 'nztmy',
                     'long_name': 'recharge pilot point latitude',
                     'missing_value': np.nan})
    rch_y[:] = rch_meta.loc[:, 'y'].values

    # initials uppers lowers
    rch_lower = nc_file.createVariable('rch_ppt_lower', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_lower.setncatts({'units': 'none',
                         'long_name': 'recharge pilot point lower bound',
                         'missing_value': np.nan})
    rch_lower[:] = pst_param.loc[rch_ppt_ids, 'lower'].values

    rch_upper = nc_file.createVariable('rch_ppt_upper', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_upper.setncatts({'units': 'none',
                         'long_name': 'recharge pilot point upper bound',
                         'missing_value': np.nan})
    rch_upper[:] = pst_param.loc[rch_ppt_ids, 'upper'].values

    rch_initial = nc_file.createVariable('rch_ppt_initial', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_initial.setncatts({'units': 'none',
                           'long_name': 'recharge pilot point initial value',
                           'missing_value': np.nan})
    rch_initial[:] = pst_param.loc[rch_ppt_ids, 'initial'].values

    rch_group = nc_file.createVariable('rch_ppt_group', 'i4', ('rch_ppt',), fill_value=-9)
    rch_group.setncatts({'flag_values': [1, 2, 3, 4],
                         'flag_meanings': 'dryland confined selwyn_irr waimak_irr',
                         'long_name': 'recharge pilot point groups',
                         'missing_value': -9})
    rch_group[:] = rch_meta.loc[:, 'group']

    rch_mult = nc_file.createVariable('rch_mult', 'f8', ('nsmc_num', 'rch_ppt'), fill_value=np.nan)
    rch_mult.setncatts({'units': 'none',
                        'long_name': 'recharge multipliers',
                        'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, rch_dim)) * np.nan
    for i, key in enumerate(rch_ppt_ids):
        temp_data[:, i] = param.loc[key].values

    rch_mult[:] = temp_data


def _add_sfr_cond(param, pst_param, nc_file):
    # stream hconds
    hcond_sites = ['hcond1', 'hcond2', 'hcond3', 'hcond4', 'hcond5', 'hcond6', 'hcond7', 'hcond8', 'hcond9', 'hcond10',
                   'hcond11', 'hcond12', 'hcond13', 'hcond14', 'hcond15', 'hcond16', 'hcond17', 'hcond18', 'hcond19',
                   'hcond20', 'hcond21', 'hcond22', 'hcond23', 'hcond24', 'hcond25', 'hcond26', 'hcond27', 'hcond28',
                   'hcond29', 'hcond30', 'hcond31', 'hcond32', 'hcond33', 'hcond34', 'hcond34x', 'hcond35', 'hcond35x',
                   'hcond36', 'hcond37', 'hcond38', 'hcond39', 'hcond40', 'hcond41', 'hcond42', 'hcond43', 'hcond44',
                   'hcond44x']

    sfr_cond = nc_file.createVariable('sfr_cond', str, ('sfr_cond',), fill_value='none')
    sfr_cond.setncatts({'units': 'none',
                        'long_name': 'sfr conductance identifier',
                        'missing_value': 'none'})
    sfr_cond[:] = hcond_sites

    # I could add river that the segment relates to if I have time (I wont)

    sfr_cond_val = nc_file.createVariable('sfr_cond_val', 'f8', ('nsmc_num', 'sfr_cond'), fill_value=np.nan)
    sfr_cond_val.setncatts({'units': 'm/day',
                            'long_name': 'sfr conductance at points',
                            'missing_value': np.nan})

    temp_data = np.zeros((nsmc_dim, sfr_dim)) * np.nan
    for i, key in enumerate(hcond_sites):
        temp_data[:, i] = param.loc[key].values


def _add_drain_cond(param, pst_param, nc_file):
    drns = ['d_salt_fct', 'd_salt_top', 'd_kuku_leg', 'd_ash_s', 'd_tar_stok', 'd_cam_mrsh', 'd_tar_gre', 'd_nbk_mrsh',
            'd_sbk_mrsh', 'd_cam_yng', 'd_cam_revl', 'd_kairaki', 'd_oho_mlbk', 'd_oho_whit', 'd_oho_jefs',
            'd_oho_kpoi', 'd_sil_ilnd', 'd_oho_misc', 'd_oho_btch', 'd_cour_nrd', 'd_dwaimak', 'd_emd_gard',
            'd_bul_styx', 'd_sil_heyw', 'd_smiths', 'd_sil_harp', 'd_bul_avon', 'd_ash_c', 'd_cust_c', 'd_chch_c',
            'd_waihora', 'd_ulin_c', 'd_dlin_c', 'd_usel_c', 'd_dsel_c', 'd_uwaimak', 'd_ash_est']

    drn_pts = nc_file.createVariable('drns', str, ('drns',), fill_value='none')
    drn_pts.setncatts({'units': 'none',
                       'long_name': 'drain_segment names',
                       'missing_value': 'none'})
    drn_pts[:] = drns

    drn_lower = nc_file.createVariable('drn_lower', str, ('drns',), fill_value=np.nan)
    drn_lower.setncatts({'units': 'none',
                         'long_name': 'drain cond lower bound',
                         'missing_value': np.nan})
    drn_lower[:] = pst_param.loc[drns, 'lower']

    drn_upper = nc_file.createVariable('drn_upper', str, ('drns',), fill_value=np.nan)
    drn_upper.setncatts({'units': 'none',
                         'long_name': 'drain cond upper bound',
                         'missing_value': np.nan})
    drn_upper[:] = pst_param.loc[drns, 'upper']

    drn_initial = nc_file.createVariable('drn_initial', str, ('drns',), fill_value=np.nan)
    drn_initial.setncatts({'units': 'none',
                           'long_name': 'drain cond initial value',
                           'missing_value': np.nan})
    drn_initial[:] = pst_param.loc[drns, 'initial']

    drn_cond = nc_file.createVariable('drn_cond', 'f8', ('nsmc_num', 'drns'), fill_value=np.nan)
    drn_cond.setncatts({'units': 'm3/day',
                        'long_name': 'drain conductance',
                        'missing_value': np.nan})
    drn_cond[:] = param.loc[drns].transpose().values


def _add_kv_kh(param, kh_kv_ppt_file, pst_param, nc_file):
    # kh kv pilot points dimensions (nsmc_num, ppt, layer)
    ppt_ids = ['pp005602', 'pp005630', 'pp005798', 'pp010726', 'pp010754', 'pp010810', 'pp010894', 'pp010922',
               'pp015738', 'pp015794', 'pp015822', 'pp015850', 'pp015878', 'pp015906', 'pp015934', 'pp015962',
               'pp015990', 'pp016018', 'pp020834', 'pp020862', 'pp020890', 'pp020918', 'pp020946', 'pp020974',
               'pp021002', 'pp021030', 'pp021058', 'pp021086', 'pp021114', 'pp021142', 'pp025930', 'pp025958',
               'pp025986', 'pp026014', 'pp026042', 'pp026070', 'pp026098', 'pp026126', 'pp026154', 'pp026182',
               'pp026210', 'pp026238', 'pp031054', 'pp031082', 'pp031110', 'pp031138', 'pp031166', 'pp031194',
               'pp031222', 'pp031250', 'pp031278', 'pp031306', 'pp031334', 'pp031362', 'pp036150', 'pp036178',
               'pp036206', 'pp036234', 'pp036262', 'pp036290', 'pp036318', 'pp036346', 'pp036374', 'pp036402',
               'pp036430', 'pp036458', 'pp041302', 'pp041330', 'pp041358', 'pp041386', 'pp041414', 'pp041442',
               'pp041470', 'pp041498', 'pp041526', 'pp041554', 'pp041582', 'pp046398', 'pp046426', 'pp046454',
               'pp046482', 'pp046510', 'pp046538', 'pp046566', 'pp046594', 'pp046622', 'pp046650', 'pp046678',
               'pp051522', 'pp051550', 'pp051578', 'pp051606', 'pp051634', 'pp051662', 'pp051690', 'pp051718',
               'pp051746', 'pp051774', 'pp051802', 'pp056618', 'pp056646', 'pp056674', 'pp056702', 'pp056730',
               'pp056758', 'pp056786', 'pp056814', 'pp056842', 'pp056870', 'pp056898', 'pp061714', 'pp061742',
               'pp061770', 'pp061798', 'pp061826', 'pp061854', 'pp061882', 'pp061910', 'pp061938', 'pp061966',
               'pp061994', 'pp062022', 'pp066838', 'pp066866', 'pp066894', 'pp066922', 'pp066950', 'pp066978',
               'pp067006', 'pp067034', 'pp067062', 'pp067090', 'pp067118', 'pp071962', 'pp071990', 'pp072018',
               'pp072046', 'pp072074', 'pp072102', 'pp072130', 'pp072158', 'pp072186', 'pp079628', 'pp079668',
               'pp079708', 'pp079748', 'pp079788', 'pp079828', 'pp086948', 'pp086988', 'pp087028', 'pp087068',
               'pp087108', 'pp094268', 'pp094308', 'pp094348', 'pp094388', 'pp094428', 'pp101588', 'pp101628',
               'pp101668', 'pp101708', 'pp108908', 'pp108948', 'pp108988', 'pp109028', 'pp116228', 'pp116268',
               'pp116308', 'pp116348', 'pp123548', 'pp123588', 'pp123628', 'pp123668', 'pp130868', 'pp130908',
               'pp130948', 'pp130988']  # from layer 1 I assume this is all of them

    ppt_meta = pd.read_table(kh_kv_ppt_file, skiprows=1, names=['x', 'y', 'group', 'ignore'], delim_whitespace=True)

    ppts = nc_file.createVariable('khv_ppt', str, ('khv_ppt',), fill_value='none')
    ppts.setncatts({'units': 'none',
                    'long_name': 'kv and kh pilot point id',
                    'missing_value': 'none'})
    ppts[:] = ppt_ids

    # X
    pptsx = nc_file.createVariable('khv_ppt_x', 'f8', ('khv_ppt',), fill_value=np.nan)
    pptsx.setncatts({'units': 'nztm',
                     'long_name': 'kv and kh pilot point longitude',
                     'missing_value': np.nan})
    pptsx[:] = ppt_meta.loc[ppt_ids, 'x']

    # Y
    pptsy = nc_file.createVariable('khv_ppt_y', 'f8', ('khv_ppt',), fill_value=np.nan)
    pptsy.setncatts({'units': 'nztm',
                     'long_name': 'kv and kh pilot point latitude',
                     'missing_value': np.nan})
    pptsy[:] = ppt_meta.loc[ppt_ids, 'y']

    # kv upper lower initial
    temp_upper = np.zeros((layer_dim, khv_dim)) * np.nan
    temp_lower = np.zeros((layer_dim, khv_dim)) * np.nan
    temp_initial = np.zeros((layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_upper[i, k] = pst_param.loc['{}_v{}'.format(key, i + 1), 'upper']
                temp_lower[i, k] = pst_param.loc['{}_v{}'.format(key, i + 1), 'lower']
                temp_initial[i, k] = pst_param.loc['{}_v{}'.format(key, i + 1), 'initial']
            except KeyError:
                pass

    pptlower = nc_file.createVariable('kv_lower', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptlower.setncatts({'units': 'm/day',
                        'long_name': 'kv pilot point lower bound',
                        'missing_value': np.nan})
    pptlower[:] = temp_lower

    pptupper = nc_file.createVariable('kv_upper', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptupper.setncatts({'units': 'm/day',
                        'long_name': 'kv pilot point upper bound',
                        'missing_value': np.nan})
    pptupper[:] = temp_upper

    pptinitial = nc_file.createVariable('kv_initial', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptinitial.setncatts({'units': 'm/day',
                          'long_name': 'kv pilot point initial value',
                          'missing_value': np.nan})
    pptinitial[:] = temp_initial

    # kh upper lower initial
    temp_upper = np.zeros((layer_dim, khv_dim)) * np.nan
    temp_lower = np.zeros((layer_dim, khv_dim)) * np.nan
    temp_initial = np.zeros((layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_upper[i, k] = pst_param.loc['{}_h{}'.format(key, i + 1), 'upper']
                temp_lower[i, k] = pst_param.loc['{}_h{}'.format(key, i + 1), 'lower']
                temp_initial[i, k] = pst_param.loc['{}_h{}'.format(key, i + 1), 'initial']
            except KeyError:
                pass

    pptlower = nc_file.createVariable('kh_lower', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptlower.setncatts({'units': 'm/day',
                        'long_name': 'kh pilot point lower bound',
                        'missing_value': np.nan})
    pptlower[:] = temp_lower

    pptupper = nc_file.createVariable('kh_upper', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptupper.setncatts({'units': 'm/day',
                        'long_name': 'kh pilot point upper bound',
                        'missing_value': np.nan})
    pptupper[:] = temp_upper

    pptinitial = nc_file.createVariable('kh_initial', 'f8', ('layer', 'khv_ppt'), fill_value=np.nan)
    pptinitial.setncatts({'units': 'm/day',
                          'long_name': 'kh pilot point initial value',
                          'missing_value': np.nan})
    pptinitial[:] = temp_initial

    # kv
    kv = nc_file.createVariable('kv', 'f8', ('nsmc_num', 'layer', 'khv_ppt'), fill_value=np.nan)
    kv.setncatts({'units': 'm/day',
                  'long_name': 'vertical conductivity',
                  'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_data[:, i, k] = param.loc['{}_v{}'.format(key, i + 1)]
            except KeyError:
                pass
    kv[:] = temp_data

    # kh
    kh = nc_file.createVariable('kh', 'f8', ('nsmc_num', 'layer', 'khv_ppt'), fill_value=np.nan)
    kh.setncatts({'units': 'm/day',
                  'long_name': 'horizontal conductivity',
                  'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_data[:, i, k] = param.loc['{}_h{}'.format(key, i + 1)]
            except KeyError:
                pass
    kh[:] = temp_data


def _add_well_obs(obs_file, rei_file, nc_file):
    other_obs = ['brnthl_4_1', 'eyrftn_2_1', 'eyrftn_6_2', 'eyrftn_6_1', 'wdnd_4_2', 'wdnd_8_4', 'wdnd_8_2', 'peg_8_7',
                 'peg_9_8', 'peg_10_9', 'peg_10_7', 'eyrftm_2_1', 'eyrftl_7_2', 'oxfds_6_2', 'oxfdnr_3_1', 'oxfdnr_4_3',
                 'oxfdnr_4_1', 'chch_4_2', 'chb_ash', 'chb_chch', 'chb_chchz', 'chb_cust', 'chb_sely', 'sel_off',
                 'chch_str', 'd_ash_c', 'd_ash_est', 'd_ash_s', 'd_bul_avon', 'd_bul_styx', 'd_cam_mrsh', 'd_cam_revl',
                 'd_cam_revlz', 'd_cam_s', 'd_cam_yng', 'd_chch_c', 'd_cour_nrd', 'd_court_s', 'd_cust_c', 'sel_str',
                 'd_dlin_c', 'd_dsel_c', 'd_dwaimak', 'd_ect', 'd_emd_gard', 'd_kaiapo_s', 'd_kairaki', 'd_kuku_leg',
                 'd_nbk_mrsh', 'd_oho_btch', 'd_oho_jefs', 'd_oho_kpoi', 'd_oho_misc', 'd_oho_miscz', 'd_oho_mlbk',
                 'd_oho_whit', 'd_salt_fct', 'd_salt_s', 'd_salt_top', 'd_sbk_mrsh', 'd_sil_harp', 'd_sil_heyw',
                 'd_sil_ilnd', 'd_smiths', 'd_tar_gre', 'd_tar_stok', 'd_taran_s', 'd_ulin_c', 'd_usel_c', 'd_uwaimak',
                 'd_waihora', 'd_waikuk_s', 'sfx_2adrn', 'sfx_3drn', 'sfx_4drn', 'sfx_5drn', 'sfx_6drn', 'sfx_7drn',
                 'mid_ash_g', 'sfx_a1_con', 'sfx_a2_gol', 'sfx_a3_tul', 'sfx_a4_sh1', 'sfx_c1_swa', 'sfx_c2_mil',
                 'sfx_cd_all', 'sfx_e1_stf', 'sfx_e_all', 'sfx_w1_cou', 'sfx_w2_tom', 'sfx_w3_ros', 'sfx_w4_mcl',
                 'sfx_w5_wat', 'sfx_w6_sh1', 'sfx_w_all', 'sfo_1drn', 'sfo_2drn', 'sfo_3drn', 'sfo_4drn', 'sfo_5drn',
                 'sfo_7drn', 'sfo_c_benn', 'sfo_c_oxf', 'sfo_c_pat', 'sfo_c_skew', 'sfo_c_swan', 'sfo_c_tip',
                 'sfo_c_tlks', 'sfo_e_down', 'sfo_e_downz', 'sfo_e_poyn', 'sfo_e_poynz', 'sfo_e_seyr', 'sfo_e_seyrz',
                 'sfo_e_wolf', 'sfo_e_wolz']
    meta = pd.read_table(rei_file, skiprows=4, index_col=0, delim_whitespace=True)
    well_names = list(set(obs_file.index) - set(other_obs))
    well_names_cap = [e.upper().replace('_', '/') for e in well_names]

    # names
    well_nm = nc_file.createVariable('well_name', str, ('well_name',), fill_value='none')
    well_nm.setncatts({'units': 'none',
                       'long_name': 'identifier for head targets',
                       'missing_value': np.nan})
    well_nm[:] = well_names_cap

    # obs
    obs = nc_file.createVariable('well_obs', 'f8', ('nsmc_num', 'well_name'), fill_value=np.nan)
    obs.setncatts({'units': 'm',
                   'long_name': 'model observation for head targets',
                   'missing_value': np.nan})
    obs[:] = obs_file.loc[well_names].transpose().values

    # target values
    tar = nc_file.createVariable('well_target', 'f8', ('well_name',), fill_value=np.nan)
    tar.setncatts({'units': 'm',
                   'long_name': 'head targets value',
                   'missing_value': np.nan})
    tar[:] = meta.loc[well_names, 'Measured'].values

    # weight
    weight = nc_file.createVariable('well_weight', 'f8', ('well_name',), fill_value=np.nan)
    weight.setncatts({'units': 'none',
                      'long_name': 'NSMC weight for head targets',
                      'missing_value': np.nan})
    weight[:] = meta.loc[well_names, 'Weights'].values

    all_wells = get_all_well_row_col()

    # x
    x = nc_file.createVariable('well_x', 'f8', ('well_name'), fill_value=np.nan)
    x.setncatts({'units': 'nztm',
                 'long_name': 'head targets longitude',
                 'missing_value': np.nan})
    x[:] = all_wells.loc[well_names_cap, 'nztmx']

    # y
    y = nc_file.createVariable('well_y', 'f8', ('well_name'), fill_value=np.nan)
    y.setncatts({'units': 'nztm',
                 'long_name': 'head targets latitude',
                 'missing_value': np.nan})
    y[:] = all_wells.loc[well_names_cap, 'nztmy']

    # depth
    depth = nc_file.createVariable('well_depth', 'f8', ('well_name'), fill_value=np.nan)
    depth.setncatts({'units': 'm',
                     'long_name': 'head targets depth',
                     'missing_value': np.nan})
    depth[:] = all_wells.loc[well_names_cap, 'depth']

    # midscreen_elv
    midscreen = nc_file.createVariable('well_midscreen', 'f8', ('well_name'), fill_value=np.nan)
    midscreen.setncatts({'units': 'm',
                         'long_name': 'head targets midscreen elevation',
                         'missing_value': np.nan})
    midscreen[:] = all_wells.loc[well_names_cap, 'mid_screen_elv']


def _add_other_obs(obs_file, rei_file, nc_file):
    other_obs = ['brnthl_4_1', 'eyrftn_2_1', 'eyrftn_6_2', 'eyrftn_6_1', 'wdnd_4_2', 'wdnd_8_4', 'wdnd_8_2', 'peg_8_7',
                 'peg_9_8', 'peg_10_9', 'peg_10_7', 'eyrftm_2_1', 'eyrftl_7_2', 'oxfds_6_2', 'oxfdnr_3_1', 'oxfdnr_4_3',
                 'oxfdnr_4_1', 'chch_4_2', 'chb_ash', 'chb_chch', 'chb_chchz', 'chb_cust', 'chb_sely', 'sel_off',
                 'chch_str', 'd_ash_c', 'd_ash_est', 'd_ash_s', 'd_bul_avon', 'd_bul_styx', 'd_cam_mrsh', 'd_cam_revl',
                 'd_cam_revlz', 'd_cam_s', 'd_cam_yng', 'd_chch_c', 'd_cour_nrd', 'd_court_s', 'd_cust_c', 'sel_str',
                 'd_dlin_c', 'd_dsel_c', 'd_dwaimak', 'd_ect', 'd_emd_gard', 'd_kaiapo_s', 'd_kairaki', 'd_kuku_leg',
                 'd_nbk_mrsh', 'd_oho_btch', 'd_oho_jefs', 'd_oho_kpoi', 'd_oho_misc', 'd_oho_miscz', 'd_oho_mlbk',
                 'd_oho_whit', 'd_salt_fct', 'd_salt_s', 'd_salt_top', 'd_sbk_mrsh', 'd_sil_harp', 'd_sil_heyw',
                 'd_sil_ilnd', 'd_smiths', 'd_tar_gre', 'd_tar_stok', 'd_taran_s', 'd_ulin_c', 'd_usel_c', 'd_uwaimak',
                 'd_waihora', 'd_waikuk_s', 'sfx_2adrn', 'sfx_3drn', 'sfx_4drn', 'sfx_5drn', 'sfx_6drn', 'sfx_7drn',
                 'mid_ash_g', 'sfx_a1_con', 'sfx_a2_gol', 'sfx_a3_tul', 'sfx_a4_sh1', 'sfx_c1_swa', 'sfx_c2_mil',
                 'sfx_cd_all', 'sfx_e1_stf', 'sfx_e_all', 'sfx_w1_cou', 'sfx_w2_tom', 'sfx_w3_ros', 'sfx_w4_mcl',
                 'sfx_w5_wat', 'sfx_w6_sh1', 'sfx_w_all', 'sfo_1drn', 'sfo_2drn', 'sfo_3drn', 'sfo_4drn', 'sfo_5drn',
                 'sfo_7drn', 'sfo_c_benn', 'sfo_c_oxf', 'sfo_c_pat', 'sfo_c_skew', 'sfo_c_swan', 'sfo_c_tip',
                 'sfo_c_tlks', 'sfo_e_down', 'sfo_e_downz', 'sfo_e_poyn', 'sfo_e_poynz', 'sfo_e_seyr', 'sfo_e_seyrz',
                 'sfo_e_wolf', 'sfo_e_wolz']

    verts = ['brnthl_4_1', 'eyrftn_2_1', 'eyrftn_6_2', 'eyrftn_6_1', 'wdnd_4_2', 'wdnd_8_4', 'wdnd_8_2', 'peg_8_7',
             'peg_9_8', 'peg_10_9', 'peg_10_7', 'eyrftm_2_1', 'eyrftl_7_2', 'oxfds_6_2', 'oxfdnr_3_1', 'oxfdnr_4_3',
             'oxfdnr_4_1', 'chch_4_2']

    meta = pd.read_table(rei_file, skiprows=4, index_col=0, delim_whitespace=True)

    for obs in other_obs:
        units = 'm3/day'
        if obs in verts:
            units = 'm'
            long_name = 'verticle gradient target {}'.format(obs)
        elif 'mid_ash_g' == obs:
            long_name = 'midashley stream-model flux group'
        elif 'sel_str' == obs:
            long_name = 'selwyn springfed stream flux'
        elif 'chch_str' == obs:
            long_name = 'chch springfed stream flux'
        elif 'sel_off' == obs:
            long_name = 'selwyn offshore flux'
        elif 'd_' in obs:
            long_name = 'drain flux {}'.format(obs)
        elif 'sfx_' in obs:
            long_name = 'sfr stream-model flux {}'.format(obs)
        elif 'chb_' in obs:
            long_name = 'constant head/offshore {}'.format(obs)
        elif 'sfo_' in obs:
            long_name = 'sfr in stream flow {}'.format(obs)
        else:
            raise ValueError('{} shouldnt get here'.format(obs))

        temp = nc_file.createVariable(obs, 'f8', ('nsmc_num',), fill_value=np.nan)
        temp.setncatts({'units': units,
                        'long_name': long_name,
                        'missing_value': np.nan,
                        'target': meta.loc[obs, 'Measured'],
                        'nsmc_weight': meta.loc[obs, 'Weight']})
        temp[:] = obs_file.loc[obs].values


def _add_convergence(obs_file, nc_file):
    converged = pd.isnull(obs_file.iloc[0]).values
    con = nc_file.createVariable('converged', 'i1', ('nsmc_num',), fill_value=-1)
    con.setncatts({'units': 'boolean',
                   'long_name': 'model_converged',
                   'missing_value': -1})
    con[:] = converged


def _add_phis(rec_file, opt_lower_rec, opt_upper_rec, nc_file):
    phis = extractphisummary(rec_file).transpose()
    basenames = ['head', 'vert', 'sfx', 'coast', 'sfo', 'drn', 'total']
    lower_phis = extractoptphi(opt_lower_rec)
    upper_phis = extractoptphi(opt_upper_rec)
    for bn in basenames:
        temp = nc_file.createVariable('phi_{}'.format(bn), 'f8', ('nsmc_num',), fill_value=np.nan)
        temp.setncatts({'units': 'none',
                        'long_name': 'phi for {}'.format(bn),
                        'missing_value': np.nan})
        temp_optlower = lower_phis.loc[bn].values
        temp_optupper = upper_phis.loc[bn].values
        temp[:] = np.concatenate((phis.loc[:, bn].values, temp_optlower, temp_optupper))


def _add_filter_1(f1txt, nc_file):
    nsmc_nums = np.array(nc_file.variables['nsmc_num'])
    with open(f1txt) as f:
        pass_nums = [int(e.strip) for e in f.readlines()] + [-1, -2]
    passed = np.in1d(nsmc_nums, pass_nums).astype(int)
    filter = nc_file.createVariable('filter1', 'i1', ('nsmc_num',), fill_value=-1)
    filter.setncatts({'units': 'boolean',
                      'long_name': 'filter 1 phi filter',
                      'missing_value': -1})
    filter[:] = passed


def _add_filter_2(f2txt, nc_file):
    nsmc_nums = np.array(nc_file.variables['nsmc_num'])
    filter1 = np.array(nc_file.variables['filter1'])
    with open(f2txt) as f:
        pass_nums = [int(e.strip) for e in f.readlines()] + [-1, -2]
    passed = np.in1d(nsmc_nums, pass_nums).astype(int)
    passed[nsmc_nums > 4000] = -1
    passed[filter1 < 1] = -1

    filter = nc_file.createVariable('filter2', 'i1', ('nsmc_num',), fill_value=-1)
    filter.setncatts({'units': 'boolean',
                      'long_name': 'filter 2 vert filter',
                      'comments': 'not run on models with number above 4000 (denoted -1)',
                      'missing_value': -1})
    filter[:] = passed


def _add_filter_3(f3txt, nc_file):
    nsmc_nums = np.array(nc_file.variables['nsmc_num'])
    filter1 = np.array(nc_file.variables['filter1'])
    with open(f3txt) as f:
        pass_nums = [int(e.strip) for e in f.readlines()] + [-1, -2]
    passed = np.in1d(nsmc_nums, pass_nums).astype(int)
    passed[nsmc_nums > 4000] = -1
    passed[filter1 < 1] = -1

    filter = nc_file.createVariable('filter3', 'i1', ('nsmc_num',), fill_value=-1)
    filter.setncatts({'units': 'boolean',
                      'long_name': 'filter 3 piezo filter',
                      'comments': 'not run on models with number above 4000 (denoted -1)',
                      'missing_value': -1})
    filter[:] = passed


def _add_filter_4(f4txt, nc_file):
    nsmc_nums = np.array(nc_file.variables['nsmc_num'])
    filter1 = np.array(nc_file.variables['filter1'])
    with open(f4txt) as f:
        pass_nums = [int(e.strip) for e in f.readlines()] + [-1, -2]
    passed = np.in1d(nsmc_nums, pass_nums).astype(int)
    passed[nsmc_nums > 4000] = -1
    passed[filter1 < 1] = -1

    filter = nc_file.createVariable('filter4', 'i1', ('nsmc_num',), fill_value=-1)
    filter.setncatts({'units': 'boolean',
                      'long_name': 'filter 4 intersect of piezo and vert filter',
                      'comments': 'not run on models with number above 4000 (denoted -1)',
                      'missing_value': -1})
    filter[:] = passed


def _add_filter_5(f5txt, nc_file):
    nsmc_nums = np.array(nc_file.variables['nsmc_num'])
    #filter4 = np.array(nc_file.variables['filter4']) #todo enable once we've run the filter
    #with open(f5txt) as f:
    #    pass_nums = [int(e.strip) for e in f.readlines()] + [-1, -2]
    #passed = np.in1d(nsmc_nums, pass_nums).astype(int)
    #passed[filter4 < 1] = -1

    filter = nc_file.createVariable('filter5', 'i1', ('nsmc_num',), fill_value=-1)
    filter.setncatts({'units': 'boolean',
                      'long_name': 'filter 5 end member mixing filter',
                      'comments': 'not run on models with number above 4000 (denoted -1)',
                      'missing_value': -1})
    filter[:] = np.zeros((nsmc_dim)) - 1  # todo


def make_netcdf_nsmc(nc_outfile, rrffile, rec_file, opt_lower_rec, opt_upper_rec, rch_ppt_tpl, kh_kv_ppt_file,
                     rei_file, pst_file, f1txt, f2txt, f3txt, f4txt, f5txt):
    """

    :param nc_outfile: path to write the netcdf file
    :param rrffile: the mc rrf file
    :param rec_file: the mc rec file
    :param opt_lower_rec: the lower optimised model rec file (NsmcBase)
    :param opt_upper_rec: the lower optimised model rec file (NsmcBaseB)
    :param rch_ppt_tpl: the recharge pilot point template file (path)
    :param kh_kv_ppt_file: kh or kv pilot point file for layer 1
    :param rei_file: rei file which holds the target values and weights
    :param pst_file: the pest contol file for the NSMC
    :param f1txt: the text file with a list that passed filter 1
    :param f2txt: the text file with a list that passed filter 2
    :param f3txt: the text file with a list that passed filter 3
    :param f4txt: the text file with a list that passed filter 4
    :param f5txt: the text file with a list that passed filter 5
    :return:
    """
    # get the data

    obs, param = extractrrf(rrffile=rrffile)  # todo add optimised model to these... and it will propogate through
    #opt obs can come from the rei files quite easily, but need to filter out the piezo group #todo

    lower_param = param_from_rec(opt_lower_rec, -1)
    upper_param = param_from_rec(opt_upper_rec, -2)
    param = pd.concat((param,pd.DataFrame(lower_param), pd.DataFrame(upper_param)), axis=1) #todo check order like crazy
    pst_param = rd_pst_parameters(pst_file)

    # set up netcdf file
    nc_file = nc.Dataset(nc_outfile, 'w')
    nc_file.notes = 'phi lower and phi upper are -1 and -2, respectivly and where present will appear at the end of ' \
                    'the nsmc_variable'

    # make dimensions
    nc_file.createDimension('nsmc_num', nsmc_dim)
    nc_file.createDimension('layer', layer_dim)
    nc_file.createDimension('rch_ppt', rch_dim)
    nc_file.createDimension('sfr_cond', sfr_dim)
    nc_file.createDimension('khv_ppt', khv_dim)
    nc_file.createDimension('well_name')
    nc_file.createDimension('drns', khv_dim)

    # variables
    nsmc_num = nc_file.createVariable('nsmc_num', 'i4', ('nsmc_num',), fill_value=-9)
    nsmc_num.setncatts({'units': 'none',
                        'long_name': 'Null Space Monte Carlo Realisation Number',
                        'comments': 'unique identifier phi lower and phi upper are -1 and -2, respectively',
                        'missing_value': -9})
    nsmc_num[:] = range(1, nsmc_dim + -1) + [-1, -2]

    layer = nc_file.createVariable('layer', 'i4', ('layer',), fill_value=-9)
    layer.setncatts({'units': 'none',
                     'long_name': 'model layer',
                     'comments': '1 indexed',
                     'missing_value': -9})
    layer[:] = range(1, layer_dim + 1)

    # parameters
    _add_simple_params(param, pst_param, nc_file)

    _add_rch_params(param, rch_ppt_tpl, pst_param, nc_file)

    _add_sfr_cond(param, pst_param, nc_file)

    _add_drain_cond(param, pst_param, nc_file)

    _add_kv_kh(param, kh_kv_ppt_file, pst_param, nc_file)

    # add observations convergence and phis
    _add_well_obs(obs, rei_file, nc_file)

    _add_other_obs(obs, rei_file, nc_file)

    _add_convergence(obs, nc_file)

    _add_phis(rec_file, opt_lower_rec, opt_upper_rec, nc_file)

    # add filters
    _add_filter_1(f1txt, nc_file)  # phi filter
    _add_filter_2(f2txt, nc_file)  # vert filter
    _add_filter_3(f3txt, nc_file)  # piezo filter
    _add_filter_4(f4txt, nc_file)  # intersect of vert and piezo filter
    _add_filter_5(f4txt, nc_file)  # end member mixing filter

    # add general comments
    nc_file.description = (
        'parameters, observations, and filters for the waimakariri Null Space Monte Carlo.  '
        'The nsmc_num are unique ids.  -1 correspons to the lower phi optimisation(NsmcBase) and -2 corresponds to the '
        'upper phi optimisation (NsmcBaseB)')
    nc_file.history = 'created {}'.format(datetime.datetime.now().isoformat())
    nc_file.source = 'script: {}'.format(sys.argv[0])
    nc_file.close()

if __name__ == '__main__':
    #todo debug and spotcheck
    print('done')