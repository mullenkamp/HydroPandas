from __future__ import division
import numpy as np
import pandas as pd
import flopy
import glob
import matplotlib.pyplot as plt
import os
from core.ecan_io import rd_sql, sql_db
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from pykrige.ok import OrdinaryKriging as okrig
import geopandas as gpd



old = gpd.read_file(r"C:\Users\MattH\Downloads\old_head_targets.shp")

new = pd.read_csv(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\targets\head_targets\head_targets_2008_inc_error.csv", index_col=0)

new = new.dropna(subset=['h2o_elv_mean'])

print set(old.Well_No) - set(new.index)

print 'done'