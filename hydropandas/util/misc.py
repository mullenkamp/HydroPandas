# -*- coding: utf-8 -*-
"""
Misc functions.
"""
from __future__ import print_function
from re import search, IGNORECASE, findall
import patoolib, fnmatch, os
from datetime import datetime
import pandas as pd
import numpy as np
import geopandas as gpd

##########################################
### Base stats for the default view of the class (once data has been loaded)


def _base_stats_fun(self):
    grp1 = self.tsdata.groupby(level=['mtype', 'site'])
    start = grp1.apply(lambda x: x.first_valid_index()[2])
    start.name = 'start_time'
    end = grp1.apply(lambda x: x.last_valid_index()[2])
    end.name = 'end_time'
    stats1 = grp1.describe()[['min', '25%', '50%', '75%', 'mean', 'max', 'count']].round(2)
    out1 = pd.concat([stats1, start, end], axis=1)
    setattr(self, '_base_stats', out1)


#########################################
### Misc utils


def unarchive_dir(folder, ext='zip', rem_original=False):
    """
    Function to unarchive files in all subfolders into those subfolders.

    folder -- The base directory.\n
    ext -- The archive file extension to be extracted.\n
    rem_original -- Should the original archive files be removed after extraction?
    """

    for root, dirs, files in os.walk(folder):
        for filename in fnmatch.filter(files, '*.' + ext):
            print(os.path.join(root, filename))
#            pyunpack.zipfile.ZipFile(os.path.join(root, filename)).extractall(root)
            patoolib.extract_archive(os.path.join(root, filename), outdir=root, interactive=False)
            if rem_original:
                os.remove(os.path.join(root, filename))


def save_df(df, path_str, index=True, header=True):
    """
    Function to save a dataframe based on the path_str extension. The path_str must  either end in csv or h5.

    df -- Pandas DataFrame.\n
    path_str -- File path (str).\n
    index -- Should the row index be saved? Only necessary for csv.
    """

    path1 = os.path.splitext(path_str)

    if path1[1] in '.h5':
        df.to_hdf(path_str, 'df', mode='w')
    if path1[1] in '.csv':
        df.to_csv(path_str, index=index, header=header)


def get_subdir(a_dir, full_path=False):
    """
    Simple function to get all subdirectories from a directory.
    """
    if full_path:
        return [os.path.join(a_dir, name) for name in os.listdir(a_dir) if os.path.isdir(os.path.join(a_dir, name))]
    else:
        return [name for name in os.listdir(a_dir) if os.path.isdir(os.path.join(a_dir, name))]


def pytime_to_datetime(pytime):
    """
    Function to convert a PyTime object to a datetime object.
    """

    dt1 = datetime(year=pytime.year, month=pytime.month, day=pytime.day, hour=pytime.hour, minute=pytime.minute)
    return dt1


def lst_rem_files(path, pattern, rem=False):
    """
    Function to remove all files matching a specific extension.
    """

    # Return all files in dir, and all its subdirectories, ending in pattern
    def gen_files(path, pattern):
       for dirname, subdirs, files in os.walk(path):
          for f in files:
             if f.endswith(pattern):
                yield os.path.join(dirname, f)

    t1 = gen_files(path, pattern)
    # Remove all files matching pattern in the current dir
    if rem:
        for f in t1:
           os.remove(f)
    return t1


def select_sites(x):
    """
    Function to check for different object types and create an array of values.
    """

    if isinstance(x, np.ndarray):
        x1 = x.copy()
    elif isinstance(x, (list, tuple)):
        x1 = np.array(x).copy()
    elif isinstance(x, (pd.Series, pd.Index)):
        x1 = x.values.copy()
    elif isinstance(x, pd.DataFrame):
        x1 = x.iloc[:, 0].values.copy()
    elif isinstance(x, str):
        if x.endswith('.shp'):
            x1 = gpd.read_file(x).copy()
        else:
            x1 = pd.read_csv(x).iloc[:, 0].values.copy()
    elif x is None:
        x1 = x
    else:
        raise TypeError("I'm sure you can find some valid type to pass")

    return x1


def rd_dir(data_dir, ext, file_num_names=False, ignore_case=True):
    """
    Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
    """

    if ignore_case:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename, IGNORECASE)])
    else:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename)])

    if file_num_names:
        site_names = np.array([int(findall("\d+", fi)[0]) for fi in files])
        return files, site_names
    else:
        return files


def time_switch(x):
    """
    Convenience codes to convert for time text to pandas time codes.
    """
    return {
        'min': 'Min',
        'mins': 'Min',
        'minute': 'Min',
        'minutes': 'Min',
        'hour': 'H',
        'hours': 'H',
        'day': 'D',
        'days': 'D',
        'week': 'W',
        'weeks': 'W',
        'month': 'M',
        'months': 'M',
        'year': 'A',
        'years': 'A',
        'water year': 'A-JUN',
        'water years': 'A-JUN',
    }.get(x, 'A')

