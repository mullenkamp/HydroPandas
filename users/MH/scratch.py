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
from scipy.interpolate import griddata
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.stream_depletion_assesment.stream_depletion_numerical_model_runs.grid_sd.visualise_grid_sd import get_mask

stream = 'kaiapoi_swaz'
layer = 0
mask = get_mask()[layer]
inputdata = pd.read_csv(r"K:\mh_modeling\StrOpt_grid_sd\StrOpt_sd_grid_data_flux_-2160.0.csv", skiprows=1)
data = inputdata.loc[inputdata[stream].notnull()]
grid_x, grid_y = smt.get_model_x_y()
idx = data.layer == layer
val = data.loc[idx, stream].values
x = data.loc[idx, 'mx'].values
y = data.loc[idx, 'my'].values
xs = grid_x[~mask]
ys = grid_y[~mask]
outdata = smt.get_empty_model_grid()*np.nan

test = griddata(points=(x,y), values=val, xi=(xs,ys), method='cubic')

outdata[~mask] = test
print('done')


