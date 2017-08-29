"""
Author: matth
Date Created: 27/04/2017 8:37 AM
"""

from __future__ import division

import timeit

import flopy
import numpy as np
import matplotlib.pyplot as plt
import users.MH.Waimak_modeling.model_tools as mt
from core import env
from core.classes.hydro import hydro

'from users.MH.Waimak_modeling.models.extended_boundry.supporting_data_analysis.NSMC_inputs.well_pkg_adjust import create_nsmc_well'

print timeit.timeit('create_nsmc_well()','from users.MH.Waimak_modeling.models.extended_boundry.supporting_data_analysis.NSMC_inputs.well_pkg_adjust import create_nsmc_well',number=100)/100