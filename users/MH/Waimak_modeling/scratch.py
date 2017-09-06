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
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_wel_spd
from pykrige.ok import OrdinaryKriging as okrig
import geopandas as gpd
from core.classes.hydro import hydro

site2 = 66402
site1 = 66403
h1 = hydro().get_data(['flow'],sites=[66403]).data['flow',site1]
h2 = hydro().get_data(['flow'],sites=[66402]).data['flow',site2]

temp = pd.merge(pd.DataFrame(h1,columns=['otara']),pd.DataFrame(h2,columns=['gorge']),right_index=True,left_index=True)
print temp.describe()
print 'done'