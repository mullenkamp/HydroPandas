from __future__ import division
import numpy as np
import pandas as pd
import geopandas as gpd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt
from copy import deepcopy
from glob import glob
from core.classes.hydro import hydro, all_mtypes
from matplotlib.colors import from_levels_and_colors
import statsmodels.formula.api as sm
from scipy.stats import skewnorm
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
import flopy

import os
import traceback

drn_data = _get_drn_spd(1,1)
cbb = flopy.utils.CellBudgetFile("C:\Users\MattH\Desktop\mf_aw_ex_vert\mf_aw_ex.cbc")
flux = cbb.get_data(kstpkper=cbb.get_kstpkper()[-1],text='drain',full3D=True)[0][0]
ncarpet = (smt.df_to_array(drn_data.loc[drn_data.group == 'ash_carpet'],'j'))

outdata = {}
for target_id in set(drn_data.target_group):
    temp = np.isfinite(smt.df_to_array(drn_data.loc[drn_data.target_group == target_id],'j'))
    outdata[target_id] = flux[temp].sum()

outdata = pd.DataFrame({'flux':outdata})/86400
print(outdata)


ecam = 295
ns = 41

print('done')