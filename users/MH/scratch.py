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

_min, _q1, m, _q2, _max = 0,10,11,12,20

samples = 10000

min_q1_samples = samples/4
q1 = np.linspace(_min, _q1,samples/4)
q2 = np.linspace(_q1, m,samples/4)
q3 = np.linspace(m, _q2,samples/4)
q4 = np.linspace(_q2, _max,samples/4)

test = np.concatenate((q1,q2,q3,q4))

a = 3*(test.mean() - np.median(test))
loc = m
scale = test.std()

skewnorm.pdf()
print'done'
