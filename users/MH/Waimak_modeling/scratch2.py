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



dup_array = mt.get_stream_duplication_array()

test_stream = flopy.utils.CellBudgetFile(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\supporting_data_for_scripts\base_model_runs\base_ss_mf\base_SS.csts")
flows = test_stream.get_data(kstpkper=(0,0),full3D=True)[0][0]
test_model = flopy.utils.CellBudgetFile(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\supporting_data_for_scripts\base_model_runs\base_ss_mf\base_SS.cstm")
models = test_model.get_data(kstpkper=(0,0),full3D=True)[0][0]
pltmodel = models
pltflows = flows
pltdup = dup_array

xs, ys = np.meshgrid(range(365),range(190))

fig, ax = plt.subplots(1,1)
fig1, ax1 = plt.subplots(1,1)

ax.pcolormesh(xs,-1*ys,pltdup)
ax1.pcolormesh(xs,-1*ys,pltdup)
for x,y,z ,m ,mod in zip(xs.flatten(),ys.flatten(),pltflows.flatten(), pltflows.mask.flatten(),pltmodel.flatten()):
    if not m:
        ax.text(x,-1*y-1,z)
        ax1.text(x,-1*y-1,mod)

plt.show(fig)
plt.show(fig1)