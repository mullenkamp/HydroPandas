# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 14:28:25 2017

@author: michaelek
"""

folder =r'T:\Temp\Patrick\stats'
ext = '_os'

def sim_min_error(folder, ext='_os'):
    from os import stat, path
    from pandas import read_fwf, DataFrame, to_numeric
    from numpy import mean, sqrt, round, array
    from re import findall

    def rd_dir(data_dir, ext, file_num_names=False, num_set=1, ignore_case=True):
        """
        Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
        """
        from os import listdir
        from numpy import array
        from re import search, IGNORECASE, findall

        if ignore_case:
            files = array([filename for filename in listdir(data_dir) if search('.' + ext, filename, IGNORECASE)])
        else:
            files = array([filename for filename in listdir(data_dir) if search('.' + ext, filename)])

        if file_num_names:
            site_names = array([int(findall("\d+", fi)[num_set - 1]) for fi in files])
            return([files, site_names])
        else:
            return(files)

    ### Read directory and remove files with 0 bytes
    files1 = rd_dir(folder, ext)
    files2 = [i for i in files1 if path.getsize(path.join(folder, i)) != 0]

    ### Iterate through files and calc stats
    r1 = []
    for j in files2:
        d1 = read_fwf(path.join(folder, j), [(2, 20), (23, 40), (42, 45)])
        d1.columns = ['sim', 'obs', 'name']
        d2 = d1.loc[d1.name == 'hed', ['sim', 'obs']]
        d2.loc[:, 'sim'] = to_numeric(d2.loc[:, 'sim'], errors='coerce')
        d2.loc[:, 'obs'] = to_numeric(d2.loc[:, 'obs'], errors='coerce')
        num1 = int(findall("\d+", j)[2])

        mane = round(mean(abs(d2['sim'] - d2['obs'])/d2['obs']), 3)
        rmse = round(sqrt(mean((d2['sim'] - d2['obs'])**2)), 3)
        r1.append([num1, mane, rmse])

    ### Combine into a dataframe and calc the min
    df = DataFrame(r1, columns=['num', 'mane', 'rmse'])
    min1 = int(df.loc[df.rmse.idxmin(), 'num'])

    return(min1, df)














































