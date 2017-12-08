# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/12/2017 12:43 PM
"""

from __future__ import division
from core import env

from set_up_run_per_modpath import make_mp_particles, get_cbc, setup_run_modpath
from extract_data import save_emulator
from con_array import _make_mednload_approx
from run_emulator import run_emulator
from time import time
import os
import pickle
import numpy as np

if __name__ == '__main__':
    t = time()
    mp_ws = r"D:\mh_waimak_models\modpath_emulator"
    mp_name = 'NsmcBase_first_try'
    if not os.path.exists(mp_ws):
        os.makedirs(mp_ws)
    cbc = get_cbc('NsmcBase', mp_ws)
    print('{} min to make cbc'.format((time() - t) / 60))
    t = time()

    setup_run_modpath(cbc, mp_ws, mp_name)
    print('{} min to setup and run modpath'.format((time() - t) / 60))
    t = time()
    path_file = os.path.join(mp_ws, 'NsmcBase_first_try.mppth')

    save_emulator(path_file,path_file.replace('.mppth','.hdf'))
    print('{} min to make emulator'.format((time() - t) / 60))
    t = time()

    bnd_type = np.loadtxt(os.path.join(mp_ws,'{}_bnd_type.txt'.format(mp_name)))
    load = _make_mednload_approx(bnd_type)
    outdata = run_emulator(path_file.replace('.mppth','.hdf'),load)
    pickle.dump(outdata, open(os.path.join(os.path.dirname(path_file),'{}_test_med_n.p'.format(mp_name)),'w'))
    print('{} min to run emulator'.format((time() - t) / 60))

