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

data = nc.Dataset(r"K:\mh_modeling\netcdfs_of_key_modeling_data\emma_unc.nc")
def read_netcdf(n=1):
    temp = data.variables['coastal'][:,0:n,1,1]