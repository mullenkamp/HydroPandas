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
import netCDF4 as nc
import os
import traceback
from scipy.interpolate import griddata
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.stream_depletion_assesment.stream_depletion_numerical_model_runs.grid_sd.visualise_grid_sd import get_mask

base_dir = r"K:\mh_modeling\nsmc_plots"

f = open(r"C:\Users\MattH\Downloads\check_plots.txt",'w')

for dirs in os.listdir(base_dir):
    f.write('{}\n'.format(dirs))
    for sub in os.listdir(os.path.join(base_dir,dirs)):
        f.write('{}\n'.format(sub))
    f.write('\n')
f.close()