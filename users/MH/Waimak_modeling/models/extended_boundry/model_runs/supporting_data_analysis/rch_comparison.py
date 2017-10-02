# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 2/10/2017 5:14 PM
"""

from __future__ import division
from core import env
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.rch_packages import _get_rch
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_bc_data.LSR_arrays import get_lsrm_base_array
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.cwms_index import get_zone_array_index
from copy import deepcopy

if __name__ == '__main__':
    w_idx = get_zone_array_index('waimak')
    old_rch = _get_rch()
    new_rch = get_lsrm_base_array('current',None,None,None,'mean')
    new_rch[~np.isfinite(new_rch)] = 0

    print 'done'