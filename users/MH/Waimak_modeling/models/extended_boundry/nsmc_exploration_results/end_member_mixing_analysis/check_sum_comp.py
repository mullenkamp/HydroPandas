# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 17/11/2017 7:09 PM
"""

from __future__ import division
from core import env
import flopy
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt

alpine = flopy.utils.UcnFile(r"K:\mh_modeling\data_from_gns\alpine\mt_aw_ex_alpine_philow.ucn").get_data(kstpkper=(0,0))
inland = flopy.utils.UcnFile(r"K:\mh_modeling\data_from_gns\EM_inland_ucnrepo\mt_aw_ex_inland_philow.ucn").get_data(kstpkper=(0,0))
coastal = flopy.utils.UcnFile(r"K:\mh_modeling\data_from_gns\EM_coastal_ucnrepo\mt_aw_ex_coastal_philow.ucn").get_data(kstpkper=(0,0))

alpine[alpine==-1] = np.nan
inland[inland==-1] = np.nan
coastal[coastal==-1] = np.nan

sums = alpine+inland+coastal

for l in range(smt.layers):
    smt.plt_matrix(sums[l],title='layer {}'.format(l))

plt.show()