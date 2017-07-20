# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 20/07/2017 12:55 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt, _get_constant_heads
import geopandas as gpd


# todo I need to get targets for everything from our data. most of the old targets are in the shp file.
# todo it might be worth popping a target dict in this script

def gen_drn_target_array():
    drn_data = _get_drn_spd(smt.reach_v, smt.wel_version)
    out_dict = {}
    out_array = np.zeros((smt.rows, smt.cols))
    for i, group in enumerate(set(drn_data['target_group']), 1):
        temp = drn_data.loc[drn_data.target_group == group]
        temp_array = smt.df_to_array(temp, 'k')
        out_array[np.isfinite(temp_array)] = i
        out_dict[i] = group

    return out_array, out_dict


def gen_sfr_flow_target_array():  # todo talk to cath/brioch about which to include, but can be done from existing shape files
    shp_path = '{}/m_ex_bd_inputs/shp/org_str_flow_targets.shp'.format(smt.sdp)
    target_array = smt.shape_file_to_model_array(shp_path, 'RASTERVALU', True)
    target_array[np.isnan(target_array)] = 0
    num_to_name = {1: 'sfo_c_benn',
                   2: 'sfo_c_tip',
                   3: 'sfo_c_pat',
                   4: 'sfo_c_oxf',
                   5: 'sfo_c_swan',
                   6: 'sfo_1drn',
                   7: 'sfo_2drn',
                   8: 'sfo_3drn',
                   9: 'sfo_4drn',
                   10: 'sfo_5drn',
                   11: 'sfo_7drn',
                   12: 'sfo_c_tlks',
                   13: 'sfo_c_skew',
                   14: 'sfo_e_wolf'}
    return target_array, num_to_name


def gen_sfr_flux_target_array():  # todo talk to cath/brioch about which to include but can be done from existing shape files
    shp_path = '{}/m_ex_bd_inputs/shp/org_str_flux_targets.shp'.format(smt.sdp)
    target_array = smt.shape_file_to_model_array(shp_path, 'GRID_CODE', True)
    target_array[np.isnan(target_array)] = 0
    num_to_name = {10: 'sfx_a1_con',
                   11: 'sfx_c1_swa',
                   12: 'sfx_c2_mil',
                   13: 'sfx_e1_stf',
                   14: 'sfx_w1_cou',
                   15: 'sfx_w2_tom',
                   16: 'sfx_w3_ros',
                   17: 'sfx_w4_mcl',
                   18: 'sfx_w5_wat',
                   19: 'sfx_w6_sh1',
                   22: 'sfx_a2_gol',
                   23: 'sfx_a4_sh1',
                   24: 'sfx_a3_tul'}

    return target_array, num_to_name


def gen_constant_head_targets():  # watch if we have constant heads in the sw boundary also note that this is 3d
    chbs = _get_constant_heads()
    shp_path = "{}/m_ex_bd_inputs/shp/coastal_head_target_zones.shp".format(smt.sdp)
    zones = np.repeat(smt.shape_file_to_model_array(shp_path, 'Id', alltouched=True)[np.newaxis, :, :], smt.layers,
                      axis=0)
    zones[np.isnan(chbs) | (chbs < -900)] = 0
    zones[np.isnan(zones)] = 0
    zone_data = gpd.read_file(shp_path)
    zone_data = zone_data.set_index('Id')
    zone_data = zone_data.loc[:, 'name'].to_dict()

    return zones, zone_data


def get_target_group_values():
    target_group_val = {'chb_ash': 5.25, # 3.0 to 7.5 #todo talk over these
                        'chb_chch': 0.9, # 0.3 to 1.5
                        'chb_cust': 0.3, # 0.1 to 0.5
                        'chb_sely': 'sel_off',

                        # drains
                        'd_ash_car': 'd_ash_car', #todo ask brioch
                        'd_ash_est': 1.2, # 0.2 to 2
                        'd_ash_s': None,
                        'd_bul_avon': 'chch_str',
                        'd_bul_styx': 'chch_str',
                        'd_cam_mrsh': 'd_cam_mrsh', #todo look up
                        'd_cam_revl': 'd_cam_revl', #todo look up
                        'd_cam_s': None,
                        'd_cam_yng': 'd_cam_yng', #todo look up
                        'd_chch_c': 'd_chch_c', #todo ask brioch
                        'd_cour_nrd': 'd_cour_nrd', #todo look up
                        'd_court_s': None,
                        'd_cust_c': 'd_cust_c', #todo ask brioch
                        'd_dlin_c': 'sel_str',
                        'd_dsel_c': 'sel_str',
                        'd_dwaimak': None,
                        'd_ect': None,
                        'd_emd_gard': 'd_emd_gard', #todo look up
                        'd_greig_s': None,
                        'd_kaiapo_s': None,
                        'd_kairaki': 'd_kairaki', #todo lookup

                        #todo start here
                        'd_kuku_leg': 'd_kuku_leg',
                        'd_nbk_mrsh': 'd_nbk_mrsh',
                        'd_oho_btch': 'd_oho_btch',
                        'd_oho_jefs': 'd_oho_jefs',
                        'd_oho_kpoi': 'd_oho_kpoi',
                        'd_oho_misc': 'd_oho_misc',
                        'd_oho_mlbk': 'd_oho_mlbk',
                        'd_oho_whit': 'd_oho_whit',
                        'd_salt_fct': 'd_salt_fct',
                        'd_salt_s': 'd_salt_s',
                        'd_salt_top': 'd_salt_top',
                        'd_sbk_mrsh': 'd_sbk_mrsh',
                        'd_sil_harp': 'd_sil_harp',
                        'd_sil_heyw': 'd_sil_heyw',
                        'd_sil_ilnd': 'd_sil_ilnd',
                        'd_smiths': 'd_smiths',
                        'd_tar_gre': 'd_tar_gre',
                        'd_tar_stok': 'd_tar_stok',
                        'd_taran_s': 'd_taran_s',
                        'd_ulin_c': 'd_ulin_c',
                        'd_usel_c': 'd_usel_c',
                        'd_uwaimak': 'd_uwaimak',
                        'd_waihora': 'sel_off',
                        'd_waikuk_s': 'd_waikuk_s',

                        # surface water flow
                        'sfo_1drn': 'sfo_1drn',
                        'sfo_2drn': 'sfo_2drn',
                        'sfo_3drn': 'sfo_3drn',
                        'sfo_4drn': 'sfo_4drn',
                        'sfo_5drn': 'sfo_5drn',
                        'sfo_7drn': 'sfo_7drn',
                        'sfo_c_benn': 'sfo_c_benn',
                        'sfo_c_oxf': 'sfo_c_oxf',
                        'sfo_c_pat': 'sfo_c_pat',
                        'sfo_c_skew': 'sfo_c_skew',
                        'sfo_c_swan': 'sfo_c_swan',
                        'sfo_c_tip': 'sfo_c_tip',
                        'sfo_c_tlks': 'sfo_c_tlks',
                        'sfo_e_wolf': 'sfo_e_wolf',

                        # surface water flux
                        'sfx_a1_con': 'sfx_a1_con',
                        'sfx_a2_gol': 'sfx_a2_gol',
                        'sfx_a3_tul': 'sfx_a3_tul',
                        'sfx_a4_sh1': 'sfx_a4_sh1',
                        'sfx_c1_swa': 'sfx_c1_swa',
                        'sfx_c2_mil': 'sfx_c2_mil',
                        'sfx_e1_stf': 'sfx_e1_stf',
                        'sfx_w1_cou': 'sfx_w1_cou',
                        'sfx_w2_tom': 'sfx_w2_tom',
                        'sfx_w3_ros': 'sfx_w3_ros',
                        'sfx_w4_mcl': 'sfx_w4_mcl',
                        'sfx_w5_wat': 'sfx_w5_wat',
                        'sfx_w6_sh1': 'sfx_w6_sh1',

                        # groups
                        'sel_off': 11, #1.2-17
                        'chch_str': 10, # 7.5 to 12.5
                        'sel_str' : 9.8 # no range

                    # todo add groups here
                    }


if __name__ == '__main__':
    zones, zone_data = gen_constant_head_targets()
    drn_array, drn_dict = gen_drn_target_array()
    flow_array, flow_dict = gen_sfr_flow_target_array()
    flux_array, flux_dict = gen_sfr_flux_target_array()
    print 'done'
