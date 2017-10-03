# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 29/09/2017 3:44 PM
"""

from __future__ import division
from core import env
import os
import zipfile


def zipped_converged(name_File_path, zipped_file_name = 'non_essential_components.zip',return_nans = False):
    """
    check if a path converged, if retun nan's then if the path is missing retuns a nan otherwise raises an ioerror
    :param name_File_path:
    :param zipped_file_name:
    :param return_nans:
    :return:
    """

    try:
        archive = zipfile.ZipFile(os.path.join(os.path.dirname(name_File_path),zipped_file_name),mode='r')
        list_file = archive.read(os.path.basename(name_File_path).replace('.nam','') + '.list').lower()
        converg = True
        end_neg = 'FAILED TO MEET SOLVER'.lower()
        if end_neg in list_file:
            converg = False
    except IOError as val:
        if return_nans:
            converg = None
        else:
            raise IOError(val)

    return converg

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