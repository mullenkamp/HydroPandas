# -*- coding: utf-8 -*-
"""
Examples on how to use HydroPandas.
"""

#### Import hydro class
from core.classes.hydro import hydro
from pandas import read_csv

#################################################
#### Load data

### Parameters

## input
sites_csv = r'E:\ecan\local\Projects\requests\suz\2017-05-18\sites.csv'

sites = [69635, 70105]
mtypes1 = 'flow'
qual_codes = [10, 18, 20, 30]

## output
export_csv1 = r'E:\ecan\local\Projects\requests\suz\2017-05-18\malfs.csv'
export_csv2 = r'E:\ecan\local\Projects\requests\suz\2017-05-18\alfs.csv'
export_csv3 = r'E:\ecan\local\Projects\requests\suz\2017-05-18\missing_alf_days.csv'
export_csv4 = r'E:\ecan\local\Projects\requests\suz\2017-05-18\flow_stats.csv'

### Read in the data
sites = read_csv(sites_csv, header=None).iloc[:,0].values.astype('int32')


h1 = hydro().get_data(mtypes=mtypes1, sites=sites, qual_codes=qual_codes)



##################################################
#### Look at the attributes and data contained in the new object

## Look at the general stats of the imported data
print(h1)

################################################
#### Tools

### Flow tools

## MALF and flow stats - Applied directly on the original hydro object
fstats = h1.stats(mtypes='flow')
fstats

malf4, alfs, mis_alfs = h1.malf7d(return_alfs=True)
malf4
alfs
mis_alfs

#############################################
#### Saving data

malf4.to_csv(export_csv1)
alfs.to_csv(export_csv2)
mis_alfs.to_csv(export_csv3)
fstats.to_csv(export_csv4)










