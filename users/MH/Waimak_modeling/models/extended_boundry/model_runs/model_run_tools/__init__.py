# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/09/2017 2:13 PM
"""

from __future__ import division
from core import env
from model_setup.base_modflow_wrapper import mod_gns_model,import_gns_model,get_model
from model_bc_data.wells import get_race_data,get_full_consent,get_max_rate