# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 18/10/2017 11:29 AM
"""

from __future__ import division
from core import env
import os

# a scipt to test whether there are any overlaps... looking for duplicates

if __name__ == '__main__':
    directory = r'C:\Users\MattH\OneDrive - Environment Canterbury\Ecan_code4\Ecan.Science.Python.Base\users\MH\Waimak_modeling\models\extended_boundry'
    all_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                all_paths.append(os.path.join(root,file))
    scripts = []
    pickle_paths = []
    for path in all_paths:
        script = os.path.basename(path)
        with open(path) as f:
            lines = f.readlines()
        for line in lines:
            if 'pickle_path' in line and '=' in line:
                scripts.append(script)
                pickle_paths.append(line)

    print 'done'

