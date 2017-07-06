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

data = pd.read_csv(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\targets\head_targets\first_pass_head_targets_2008.csv", index_col=0)

data2 = data.loc[data['monitoring_well']]
data2.loc[:,'factor_range'] =data2.loc[:,[u's_factor_jan', u's_factor_feb', u's_factor_mar',
       u's_factor_apr', u's_factor_may', u's_factor_jun', u's_factor_jul',
       u's_factor_aug', u's_factor_sep', u's_factor_oct', u's_factor_nov',
       u's_factor_dec']].max(axis=1)-  data2.loc[:,[u's_factor_jan', u's_factor_feb', u's_factor_mar',
       u's_factor_apr', u's_factor_may', u's_factor_jun', u's_factor_jul',
       u's_factor_aug', u's_factor_sep', u's_factor_oct', u's_factor_nov',
       u's_factor_dec']].min(axis=1)

data2.loc[:,'factor_sd'] = data2.loc[:,[u's_factor_jan', u's_factor_feb', u's_factor_mar',
       u's_factor_apr', u's_factor_may', u's_factor_jun', u's_factor_jul',
       u's_factor_aug', u's_factor_sep', u's_factor_oct', u's_factor_nov',
       u's_factor_dec']].std(axis=1)

reading_pers = data2[[u'reading_jan', u'reading_feb', u'reading_mar',
       u'reading_apr', u'reading_may', u'reading_jun', u'reading_jul',
       u'reading_aug', u'reading_sep', u'reading_oct', u'reading_nov',
       u'reading_dec']].div(data2['readings'],axis=0)

idx = ((reading_pers > 0.04) & (reading_pers < 0.126)).all(axis=1)

data2 = data2.loc[idx]



data2.to_csv(r"C:\Users\MattH\Downloads\target_factors_reduced.csv")

print ('done')