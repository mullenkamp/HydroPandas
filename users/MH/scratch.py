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


h1= pd.DataFrame(hydro().get_data(mtypes=['flow'], sites=[66417]).data['flow',66417])
h2= pd.DataFrame(hydro().get_data(mtypes=['flow_m'], sites=[343]).data['flow_m',343])

temp = pd.merge(h2,h1,right_index=True,left_index=True)
temp[temp<=0] = np.nan
temp[temp>20] = np.nan
temp = temp.dropna()

result = sm.ols(formula="data_x ~ data_y", data=temp).fit()
print result.params

print 'done'