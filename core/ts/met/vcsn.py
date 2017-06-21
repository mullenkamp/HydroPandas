# -*- coding: utf-8 -*-
"""
Functions for processing vcsn met data from NIWA.
"""


def rd_vcn_ecan_dat(data_dir, file1, rd_index=False):
    """
    Function to read an individual ecan dat file from the updated vcn precip data. Should likely be only called from another function to read in many files at once.
    """
    from os import path
    from pandas import read_table

    if rd_index:
        t1 = read_table(path.join(data_dir, file1), header=None, sep=" ", skipinitialspace=True, usecols=[0, 1], parse_dates=True, infer_datetime_format=True, index_col=0)
    else:
        t1 = read_table(path.join(data_dir, file1), header=None, sep=" ", skipinitialspace=True, usecols=[1])

    return(t1)


def rd_vcn_niwa_orig_lst(data_dir, file1, rd_index=False):
    """
    Function to read an individual NIWA lst file from the original vcn precip data. Should likely be only called from another function to read in many files at once.
    """
    from os import path
    from pandas import read_csv

    if rd_index:
        t1 = read_csv(path.join(data_dir, file1), header=None, usecols=[3, 4, 5], parse_dates=True, infer_datetime_format=True, index_col=0)
        t1.columns = ['precip', 'ET']
    else:
        t1 = read_csv(path.join(data_dir, file1), header=None, usecols=[4, 5])
        t1.columns = ['precip', 'ET']

    return(t1)


def rd_vcn_niwa_lst(data_dir, file1, cols=all, header=0):
    """
    Function to read an individual NIWA lst file from the new vcn meteorological data. Should likely be only called from another function to read in many files at once.

    Arguments:\n
    data_dir -- The directory that contains the files.\n
    file1 -- The file within the directory to be read in.\n
    cols -- Either 'all' or a list of column names to be read in.
    """
    from os import path
    from pandas import read_csv

    if cols is all:
        cols = [0,3,4,5,6,7,8,9,10,11,12,13,14]

    t1 = read_csv(path.join(data_dir, file1), header=header, usecols=cols)

    return(t1)


def rd_vcn_niwa_lst_dir(data_dir, ext='dat', cols=['Date', 'Agent', 'Rain', 'PET'], col_names=['Date', 'Agent', 'Rain', 'PET'], date_format='%d/%m/%Y', date_col=1, header=0):
    """
    Connected with the rd_vcn_niwa_lst function, but reads in an entire directory and concat's the files together. Requires the 'Date' field/column.
    """
    from core.ts.met import rd_vcn_niwa_lst
    from core.misc import rd_dir
    from pandas import to_datetime, concat

    files = rd_dir(data_dir, ext, False)
    df1 = concat(rd_vcn_niwa_lst(data_dir, f, cols, header) for f in files)
    df1.columns = col_names
    df1[col_names[date_col-1]] = to_datetime(df1[col_names[date_col-1]], format=date_format)
    return(df1)


def append_vcn(source_dir, new_data_dir, up_dir=''):
    """
    Function to append new NIWA data (precip and ET) onto the existing processed data. The data is then resaved as a csv file per VCN station.

    Arguments:\n
    source_dir -- The directory where the original files are stored (already processed data to be appended to).\n
    new_data_dir -- The directory where the new data is stored and will be appended.\n
    up_dir -- Optional directory within the source_dir where the porcessed data will be stored. Include if you do not want to overwrite existing files.
    """
    from os import path
    from pandas import read_csv, DateOffset, concat
    from core.misc import rd_dir
    from core.ts.met import rd_vcn_niwa_lst_dir

    ### Load in new data
    df = rd_vcn_niwa_lst_dir(new_data_dir)

    ### Make list of source files
    files, sites = rd_dir(source_dir, 'csv', True)

    ### Filter only needed sites from new data
    df2 = df[df['Agent'].isin(sites)]

    ### Loop through each file and append
    for i in range(len(files)):
        csv1 = files[i]
        site = sites[i]

        ## read source data
        t1 = read_csv(path.join(source_dir, csv1), index_col=0, parse_dates=True, infer_datetime_format=True)

        ## Select the site with data from new data and reorganize
        df_grp = df2.groupby('Agent').get_group(site)
        df_grp.index = df_grp['Date']
        df_grp1 = df_grp[['Rain', 'PET']]
        end1 = df_grp1.index[0] - DateOffset(days=1)
        df_grp1.columns = ['precip', 'ET']

        ## Append new data to old data
        t2 = concat([t1[:end1], df_grp1])
        t2.index.name = 'date'

        ## Save new file
        t2.to_csv(path.join(source_dir, up_dir, 'precip_ET_' + str(site) + '.csv'))
