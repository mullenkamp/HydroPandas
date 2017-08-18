# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 18/08/2017 9:42 AM
"""

from __future__ import division
from core import env
import pandas as pd
import os
from glob import glob
from file_set_up import user_codes, users, minor_qoi, major_qoi, values
from warnings import warn

all_dir = "C:/Users/MattH/OneDrive - Environment Canterbury/waimakariri_elicitation/all_qois"
if not os.path.exists(all_dir):
    os.makedirs(all_dir)
def combine_QOI (qoi):
    qoi_dir = '{}/{}'.format(all_dir,qoi)
    if not os.path.exists(qoi_dir):
        os.makedirs(qoi_dir)
    base_dir = "C:/Users/MattH/OneDrive - Environment Canterbury/waimakariri_elicitation/*/*{}*".format(qoi)
    paths = glob(base_dir)
    for sub_qoi in minor_qoi[qoi]:
        out_data = pd.DataFrame(index=user_codes.values().sort(),columns=values)
        for path in paths:
            if 'all_qois' in path:
                continue
            user = path.split('\\')[-2] #todo check
            ucode = user_codes[user]
            temp = pd.read_excel(path, index_col=0)
            out_data.loc[ucode,:] = temp.loc[sub_qoi,:]

        if any(out_data.isnull()):
            print('{} has null values from users: {}'.format(sub_qoi, list(out_data.index[out_data.isnull().any(1)])))

        out_data.to_csv('{}/{}_combined.csv'.format(qoi_dir,sub_qoi))



if __name__ == '__main__':
    combine_QOI(major_qoi[0])






