# -*- coding: utf-8 -*-
"""
Author: mattH
Date Created: 28/08/2017 4:14 PM
"""

from __future__ import division
from pandas import read_csv
path = '' #todo define
data = read_csv(path)
""" make a csv of col, row, layer, flux, type, cwms  or just make a NSMC_type that would be better"""

chch_pump_mult = None #todo
sel_pump_mult = None #todo
wai_pump_mult = None #todo
sriv_mult = None #todo
n_race_mult = None #todo
s_race_mult = None #todo
llrzf = None #todo
ulrzf = None #todo
n_bound_mult = None #todo

# pumping wells
data.loc[data.nsmc_type == 'p_c', 'flux'] *= chch_pump_mult
data.loc[data.nsmc_type == 'p_s', 'flux'] *= sel_pump_mult
data.loc[data.nsmc_type == 'p_w', 'flux'] *= wai_pump_mult

# selwyn_hillfeds
data.loc[data.nsmc_type == 'sriv', 'flux'] *= sriv_mult

# races
data.loc[data.nsmc_type=='n_rc', 'flux'] *= n_race_mult
data.loc[data.nsmc_type=='s_rc', 'flux'] *= s_race_mult

# boundary fluxes
data.loc[data.nsmc_type == 'l_sf', 'flux'] = llrzf #todo think about how this will come out
data.loc[data.nsmc_type == 'u_sf', 'flux'] = ulrzf #todo think about how this will come out
data.loc[data.nsmc_type == 'nf', 'flux'] *= n_bound_mult

#todo save as well package
ln1 = '     11095       740   AUX  IFACE'
ln2 = '     11095         0  # stress period 0'
