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


import os
import traceback

def caues_error():
    raise ValueError('this raised an exeception')

def do_stuff():
    caues_error()

def fun_():
    try:
        do_stuff()
    except Exception as val:
        temp = traceback.format_exc()

    print('done')

if __name__ == '__main__':
    fun_()