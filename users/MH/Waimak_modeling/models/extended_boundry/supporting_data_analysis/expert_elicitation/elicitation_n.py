# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 20/10/2017 3:05 PM
"""

from __future__ import division
from core import env
from glob import glob
import pandas as pd
import os
import itertools
import numpy as np


def amalgimate_data(base_dir):
    experts = ['OM', 'MR', 'LL', 'AM', 'LF']
    components = ['Sheep_Beef_Deer_exhill_Upsc_Light',
                  'Dairy_Rep_Medium',
                  'Sheep_Beef_Deer_exhill_Trans_S',
                  'Dairy_Struc_Medium',
                  'Dairy_Upsc_Light',
                  'Dairy_Inp_PD',
                  'Dairy_Upsc_Medium',
                  'Dairy_Upsc_PD',
                  'Sheep_Beef_Deer_exhill_Inp_F',
                  'Sheep_Beef_Hill_S1_S4 soils',
                  'Sheep_Beef_Deer_exhill_Trans_F',
                  'Sheep_Beef_Deer_exhill_Rep_Light',
                  'Dairy_Inp_Medium',
                  'Dairy_Trans_Light',
                  'Dairy_Trans_Medium',
                  'Lifestyle',
                  'Sheep_Beef_Deer_exhill_Inp_S',
                  'Sheep_Beef_Deer_exhill_Inp_Light',
                  'Sheep_Beef_Deer_exhill_Rep_S',
                  'Sheep_Beef_Deer_exhill_Trans_Light',
                  'Sheep_Beef_Deer_exhill_Struc_Light',
                  'Dairy_Rep_Light',
                  'Forest_Tussock_DOC',
                  'Sheep_Beef_Deer_exhill_Rep_F',
                  'Dairy_Trans_PD',
                  'Dairy_Struc_PD',
                  'Dairy_Rep_PD',
                  'Dairy_Struc_Light',
                  'Sheep_Beef_Deer_exhill_Upsc_S',
                  'Sheep_Beef_Deer_exhill_Struc_S',
                  'Dairy_Inp_Light',
                  'Sheep_Beef_Deer_exhill_Struc_F',
                  'Sheep_Beef_Deer_exhill_Upsc_F']
    out_ex = []
    out_comp = []
    out_alpha = []
    out_beta = []
    out_l = []
    out_u = []
    missing_paths = []
    for i, (expert, component) in enumerate(itertools.product(experts, components)):
        out_ex.append(expert)
        out_comp.append(component)
        path = os.path.join(base_dir, '{}_{}.txt'.format(component, expert))
        missing = False
        if not os.path.exists(path):
            missing_paths.append(os.path.basename(path) + '\n')
            missing = True
        if missing:
            out_alpha.append(np.nan)
            out_beta.append(np.nan)
            out_u.append(np.nan)
            out_l.append(np.nan)
        else:
            with open(path) as f:
                lines = f.readlines()
            beta = [e.replace('Scaled Beta:', '').split(';') for e in lines if 'Scaled Beta' in e][0]
            out_alpha.append(float(beta[0].split(' = ')[-1]))
            out_beta.append(float(beta[1].split(' = ')[-1]))
            out_l.extend([float(e.split(' ')[-1]) for e in lines if 'minimum' in e.lower()])
            out_u.extend([float(e.split(' ')[-1]) for e in lines if 'maximum' in e.lower()])

    missing_paths.sort()
    with open(os.path.join(base_dir, 'missing_txts.txt'), 'w') as f:
        f.writelines(missing_paths)

    outdata = pd.DataFrame(
        {'Component': out_comp, 'Expert': out_ex, 'A': out_alpha, 'B': out_beta, 'L': out_l, 'U': out_u})
    outdata = outdata.sort_values(['Component','Expert'])
    outdata = outdata.loc[:, ['Component', 'Expert', 'A', 'B', 'L', 'U']]
    outdata.to_excel(r"\\gisdata\projects\SCI\Groundwater\Waimakariri\Landuse\N load expert judgement workshop\Elicitation_results_import.xlsx")

    print'done'


if __name__ == '__main__':
    amalgimate_data(r"C:\Users\MattH\Downloads")
