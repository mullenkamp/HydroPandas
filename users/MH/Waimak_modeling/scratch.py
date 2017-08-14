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


h1 = hydro().get_data(['flow'],sites=[67801], from_date='1991-04-01',to_date='2006-12-31')
h2 = hydro().get_data(['flow'],sites=[67801], from_date='2008-01-01',to_date='2016-12-31')
print h1
print h2
print 'done'