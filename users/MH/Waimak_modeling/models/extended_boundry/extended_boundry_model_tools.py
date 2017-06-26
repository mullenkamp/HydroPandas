"""
Author: matth
Date Created: 22/06/2017 5:05 PM
"""

from __future__ import division
from core import env
from users.MH.Model_Tools.ModelTools import ModelTools
from users.MH.Waimak_modeling.supporting_data_path import sdp, temp_file_dir
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.supporting_scripts import _elvdb_calc

smt = ModelTools(
    'ex_bd_va', sdp='{}/ex_bd_va_sdp'.format(sdp), ulx=1512162.53275, uly=5215083.5772, layers=17, rows=364, cols=365,
    grid_space=200, no_flow=None, temp_file_dir=temp_file_dir, elv_calculator=_elvdb_calc, base_mod_path=None
)
