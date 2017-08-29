# -*- coding: utf-8 -*-
"""
Author: mattH
Date Created: 28/08/2017 4:14 PM
"""
from __future__ import division

def create_nsmc_well():
    from pandas import read_csv
    from numpy import float32, int32, sum
    path = r"C:\Users\MattH\Downloads\test_nsmc_well.csv"  # todo define this one is temp
    data = read_csv(path, dtype={'layer': int32, 'row': int32, 'col': int32, 'flux': float32})
    mult_path = r"C:\Users\MattH\Desktop\waimak_wel_adjuster.csv" #todo temp
    multipliers = read_csv(mult_path,index_col=0)

    mult_groups = ['pump_c', 'pump_s', 'pump_w', 'sriv', 'n_race', 's_race', 'nbndf']
    for group in mult_groups:
        data.loc[data.nsmc_type == group, 'flux'] *= multipliers.loc[group,'value']

    add_groups = ['llrzf', 'ulrzf']
    for group in add_groups:
        data.loc[data.nsmc_type == group, 'flux'] = multipliers.loc[group,'value']/(data.nsmc_type==group).sum()

    g = data.groupby(['layer', 'row', 'col'])
    well_ag = g.aggregate({'flux': sum}).reset_index()

    outdata = well_ag.loc[:, ['layer', 'row', 'col', 'flux']]
    outdata = outdata.rename(columns={'layer': 'k', 'row': 'i', 'col': 'j'})
    outdata.loc[:,['k','i','j']] += 1
    # todo save as well package? I think this should work... I need to test it
    out_file = r"C:\Users\MattH\Downloads\test_speed.wel"
    with open(out_file,'w') as f:
        f.writelines(['11095    740    AUX    IFACE\n', '11095    0    # stress period 0\n'])
    outdata.to_csv(out_file,header=None,index=None,sep=' ', mode='a')




if __name__ == '__main__':
    create_nsmc_well()