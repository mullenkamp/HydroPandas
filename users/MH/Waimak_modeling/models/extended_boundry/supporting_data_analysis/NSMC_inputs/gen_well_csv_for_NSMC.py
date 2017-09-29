# -*- coding: utf-8 -*-
"""
Author: mattH
Date Created: 28/08/2017 4:17 PM
"""

from __future__ import division
from core import env
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import _get_wel_spd_v1

def get_well_NSMC_base():
    all_wells = _get_wel_spd_v1(recalc=True)
    all_wells.loc[:,'nsmc_type'] = ''
    #pumping wells
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='chch'),'nsmc_type'] = 'pump_c'
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='selwyn'),'nsmc_type'] = 'pump_s'
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='waimak'),'nsmc_type'] = 'pump_w'

    #selwyn_hillfeds
    all_wells.loc[all_wells.type=='river','nsmc_type'] = 'sriv'

    #races
    all_wells.loc[(all_wells.type == 'race')&(all_wells.zone=='n_wai'),'nsmc_type'] = 'n_race'
    all_wells.loc[(all_wells.type == 'race')&(all_wells.zone=='s_wai'),'nsmc_type'] = 's_race'

    #boundary fluxes
    all_wells.loc[all_wells.type == 'llr_boundry_flux','nsmc_type'] = 'llrzf'
    all_wells.loc[all_wells.type == 'ulr_boundry_flux','nsmc_type'] = 'ulrzf'
    all_wells.loc[all_wells.type == 'boundry_flux','nsmc_type'] = 'nbndf'

    all_wells = all_wells.loc[:,['layer','row','col', 'flux','nsmc_type']]
    return all_wells

def get_template_data():
    groups = ['pump_c', 'pump_s', 'pump_w', 'sriv', 'n_race', 's_race', 'nbndf','llrzf', 'ulrzf']
    pram_id = ['${}$'.format(e) for e in groups]
    out_data = pd.DataFrame(index=groups,data=pram_id)
    return out_data