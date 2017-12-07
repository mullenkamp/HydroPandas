# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/12/2017 12:43 PM
"""

from __future__ import division
from core import env

from set_up_run_per_modpath import make_mp_particles, get_cbc, setup_run_modpath
from extract_data import save_emulator
from time import time
import os

if __name__ == '__main__':
    t = time()
    mp_ws = r"D:\mh_waimak_models\modpath_emulator"
    if not os.path.exists(mp_ws):
        os.makedirs(mp_ws)
    cbc = get_cbc('NsmcBase', mp_ws)
    print('{} min to make cbc'.format((time() - t) / 60))
    t = time()
    setup_run_modpath(cbc, mp_ws, 'NsmcBase_first_try')
    print('{} min to setup and run modpath'.format((time() - t) / 60))
    t = time()
    path_file = os.path.join(mp_ws, 'NsmcBase_first_try.mppth')
    save_emulator(path_file,path_file.replace('.mppth','.nc'))
    print('{} min to make emulator'.format((time() - t) / 60))
