"""
quick script to pull an excel file into many files for fouad.  since I've been using pd.pivot more to do what he wanted
Author: matth
Date Created: 24/02/2017 10:44 AM
"""

from __future__ import division
import pandas as pd
import numpy as np

def split_excel(input_path, outdir, split_col, time_col, t_format = '%d/%m/%Y'):
    """
    splits dataframes by parameter and will name csv by the value (if / in value replaced by _)
    :param input_path: input excel or csv
    :param outdir: directory where csv's will be genereated
    :param split_col: column string to be split by
    :return:
    """
    if '.csv' in input_path:
        alldata = pd.read_csv(input_path)
    elif '.xls' in input_path:
        alldata = pd.read_excel(input_path)
    else:
        raise ValueError('input not recognised file type')

    number = []
    start_date = []
    end_date = []
    wells = list(set(alldata[split_col]))
    for well in wells:
        temp_data = alldata[alldata[split_col] == well]
        number.append(len(temp_data[split_col]))
        start_date.append(pd.to_datetime(temp_data[time_col],format=t_format).min())
        end_date.append(pd.to_datetime(temp_data[time_col],format=t_format).max())
        temp_data.to_csv("{}/{}.csv".format(outdir,well.replace('/','_')))

    overview = pd.DataFrame(
        {'well': wells,
         'number': number,
         'start date': start_date,
         'end date': end_date
         })

    overview.to_csv("{}/overview.csv".format(outdir))

    if len(alldata[split_col]) != np.array(number).sum():
        raise ValueError('some data was lost, please review')

if __name__ == '__main__':
    outdir = "T:/Temp/Matt_Hanson_TRAN/for_Fouad"
    input_path = r"T:\Temp\Matt_Hanson_TRAN\Query8.xlsx"
    split_excel(input_path,outdir,split_col='WELL_NO', time_col='DMY')
