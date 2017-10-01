# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 29/09/2017 3:44 PM
"""

from __future__ import division
from core import env


def converged(list_path):
    """

    :param list_path:
    :return: True if converged, False if not, None if not realised
    """
    converg = True
    end_neg = 'FAILED TO MEET SOLVER'.lower()
    with open(list_path) as temp:
        for i in temp:
            if end_neg in i.lower():
                converg = False
                break
    return converg

if __name__ == '__main__':
    print(converged(r"D:\mh_model_runs\forward_runs_2017_09_29\opt_current\opt_current.list"))