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


well_data = get_wel_spd(1)
well_data = well_data.loc[(well_data.type=='well') & (well_data.zone == 's_wai')]

usage_data = hydro().get_data('usage',sites=list(well_data.index))

print 'done'