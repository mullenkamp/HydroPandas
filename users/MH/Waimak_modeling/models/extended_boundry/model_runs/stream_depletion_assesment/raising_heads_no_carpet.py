# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 26/10/2017 3:22 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.base_modflow_wrapper import import_gns_model

def remove_ncarpet_drains(model): # assume steady state for now
    org_drn_data = _get_drn_spd(1,1)
    drn = m.get_package('drn').stress_period_data.data[0]
    raise NotImplementedError

def raise_top_elv(model):
    # try first only in carpet drain cells, then second elsewhere
    raise NotImplementedError

if __name__ == '__main__':
    test_type = [1]
    if 1 in test_type:
        m = import_gns_model('StrOpt','test_raise_elv',r"C:\Users\MattH\Desktop\test_raise_elv")
        remove_ncarpet_drains(m)
        m.write_inputs()
        m.write_name_file()
        m.run_model()