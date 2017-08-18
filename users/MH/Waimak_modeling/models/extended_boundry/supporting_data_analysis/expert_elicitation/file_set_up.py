# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 18/08/2017 9:10 AM
"""

from __future__ import division
from core import env
import os
import pandas as pd

users = ['paul_white', 'peter_callander', 'scott_wilson', 'jens_rekker', 'john_talbot', 'lee_burbery', 'zeb_etheridge','zeb_test','maureen_test']
user_codes = {'jens_rekker': 'A',
              'john_talbot': 'B',
              'lee_burbery': 'C',
              'peter_callander': 'D',
              'paul_white': 'E',
              'scott_wilson': 'F',
              'zeb_etheridge': 'G',
              'maureen_test': 'M', #todo remove
              'zeb_test': 'Z'} #todo remove

major_qoi = ['pumping', 'possible_ks', 'offshore', 'LRZ_flux', 'LSR', 'race_loss'] #todo make sure this is in order
minor_qoi = {'pumping': ['model_average_pumping', 'extra_1', 'extra_2', 'extra_3'],
             'possible_ks': ['possible_ks', 'extra_1', 'extra_2', 'extra_3'],
             'offshore': ['N_waimak', 'S_waimak', 'CHCH', 'Selwyn',  'extra_1', 'extra_2', 'extra_3'],
             'LRZ_flux': ['LRZ_flux', 'extra_1', 'extra_2', 'extra_3'],
             'LSR': ['waimak_irrigated', 'waimak_dryland', 'inland_chch',
                     'coastal_chch', 'selwyn_irrigated','selwyn_dryland' 
                     'extra_1', 'extra_2', 'extra_3'],
             'race_loss': ['waimak_races','sewlyn_races', 'extra_1', 'extra_2', 'extra_3']}

values = ['lower_limit', 'upper_limit', 'median', 'lower_quartile', 'upper_quartile']

def make_empty_files(base_dir,date):
    if os.path.exists(base_dir):
        raise ValueError('the path already exists, do not overwrite!')
    os.makedirs(base_dir)
    for u in users:
        os.makedirs('{}/{}'.format(base_dir,u))
        for qoi in major_qoi:
            temp = pd.DataFrame(columns=values,index=minor_qoi[qoi])
            temp.to_excel('{b}/{us}/W_elicitation_{d}_{us}_{q}.xlsx'.format(b=base_dir, us=u, d=date, q=qoi))



if __name__ == '__main__':
    make_empty_files(r"C:\Users\MattH\Desktop\test_waimak_elicitation2",'21-08-2017')

    print 'done'