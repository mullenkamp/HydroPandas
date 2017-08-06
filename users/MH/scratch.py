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

h1= hydro().get_data(mtypes=['flow_m'], sites=[1146,1147,1148,1149,343,289],from_date='2008-01-01')

print 'done'