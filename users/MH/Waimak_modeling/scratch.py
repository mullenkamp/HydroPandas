from __future__ import division
import numpy as np
import pandas as pd
import users.MH.Waimak_modeling.model_tools as mt
import flopy
import glob
import matplotlib.pyplot as plt
from users.MH.Waimak_modeling.supporting_data_path import base_mod_dir
import os
from core.ecan_io import rd_sql, sql_db


m = mt.wraps.mflow.import_gns_model('test',r"C:\Users\MattH\Desktop\test_dir",safe_mode=False)
m.remove_package('STR')
mt.wraps.mf_packages.create_sfr_package(m)

m.write_input()
m.write_name_file()
m.run_model()