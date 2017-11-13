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

data = np.repeat(flopy.utils.UcnFile(r"K:\mh_modeling\data_from_gns\EM_coast_ucnrepo\mt_aw_ex_coastal_1.ucn").get_data(kstpkper=(0,0))[np.newaxis,:,:,:],11,axis=0)
def write_to_nc(layers):
    np.random.seed()
    path = r"C:\Users\Public\Desktop\test_speed\test_speed{}{}.nc".format(layers,np.random.randint(20000))
    nc_file = nc.Dataset(path, 'w')
    nc_file.createDimension('nsmc', 100)
    nc_file.createDimension('layer', smt.layers)
    nc_file.createDimension('row', smt.rows)
    nc_file.createDimension('col', smt.cols)
    var = nc_file.createVariable('test','f4',('nsmc','layer','row','col'),zlib=True)
    if layers == 'all':
        var[:] = data[:]
    else:
        var[0:layers] = data[0:layers]
        smt.plt_matrix()

if __name__ == '__main__':
    write_to_nc('all')