# -*- coding: utf-8 -*-
"""
Author: mattH
Date Created: 28/08/2017 4:17 PM
"""

from __future__ import division
from core import env
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import _get_wel_spd_v1


if __name__ == '__main__':

    all_wells = _get_wel_spd_v1()
    all_wells.loc[:,'nsmc_type'] = ''

    #pumping wells
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='chch'),'nsmc_type'] = 'p_c'
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='selwyn'),'nsmc_type'] = 'p_s'
    all_wells.loc[(all_wells.type=='well') & (all_wells.cwms=='waimak'),'nsmc_type'] = 'p_w'

    #selwyn_hillfeds
    all_wells.loc[all_wells.type=='river','nsmc_type'] = 'sriv'

    #races
    all_wells.loc[(all_wells.type == 'race')&(all_wells.zone=='n_wai'),'nsmc_type'] = 'n_rc'
    all_wells.loc[(all_wells.type == 'race')&(all_wells.zone=='s_wai'),'nsmc_type'] = 's_rc'

    #boundary fluxes
    all_wells.loc[all_wells.type == 'llr_boundry_flux','nsmc_type'] = 'l_sf'
    all_wells.loc[all_wells.type == 'ulr_boundry_flux','nsmc_type'] = 'u_sf'
    all_wells.loc[all_wells.type == 'boundry_flux','nsmc_type'] = 'nf'

    all_wells = all_wells.loc[:,['layer','row','col', 'flux','nsmc_type']]
    all_wells.to_csv() #todo add path
