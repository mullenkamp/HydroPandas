# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 4/07/2017 1:28 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.sfr2_packages import _get_reach_data
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_wel_spd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.rch_packages import _get_rch
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#todo a lot of this could go into model tools (maybe)

#todo run checks

def check_no_overlapping_features():
    no_flow = smt.get_no_flow()
    no_flow[no_flow<0]=0
    no_flow = pd.DataFrame(smt.model_where(~no_flow.astype(bool)), columns=['k','i','j'])
    no_flow['bc_type'] = 'no_flow'

    sfr_data = pd.DataFrame(_get_reach_data(smt.reach_v))
    sfr_data['bc_type'] = 'sfr'

    well_data = get_wel_spd(smt.wel_version).rename(columns={'row':'i','col':'j', 'layer':'k'})
    well_data['bc_type'] = 'well'

    drn_data = _get_drn_spd(smt.reach_v,smt.wel_version)
    drn_data['bc_type'] = 'drn'
    drn_data = drn_data.loc[~drn_data.group.str.contains('carpet')] # don't worry about carpet drains

    all_data = pd.concat((no_flow,sfr_data,well_data,drn_data))

    if any(all_data.duplicated(['i','j','k'],keep=False)):
        raise ValueError ('There are duplicate boundry conditions')

def check_layer_overlap():
    for i in range(smt.layers):
        fig, ax = smt.plt_matrix(smt.check_layer_overlap(use_elv_db=True,layer=i,required_overlap=0.50),title='layer {}'.format(i),vmax=2,vmin=-2,no_flow_layer=i)
        plt.show(fig)

def check_elv_db():
    no_flow = smt.get_no_flow(0)
    elv = smt.calc_elv_db()
    elv[~(np.repeat(no_flow[np.newaxis,:,:].astype(bool),smt.layers,axis=0))] = np.nan
    tops = elv[0:-1]
    bots = elv[1:]
    if any((bots>tops).flatten()):
        raise ValueError('some bottoms are higher than tops')

def check_noflow_overlap():
    no_flow = np.abs(smt.get_no_flow())
    nabove = no_flow[0:-1]
    nbelow = no_flow[1:]
    if any((nbelow>nabove).flatten()):
        raise ValueError('there exist pockets of flow under no_flow')

def check_null_spd():
    sfr_data = pd.DataFrame(_get_reach_data(smt.reach_v))
    if any(np.array(sfr_data.isnull()).flatten()):
        raise ValueError('null data in SFR spd')

    well_data = get_wel_spd(smt.wel_version)
    if any(np.array(well_data.loc[:,[u'col', u'flux', u'layer', u'row']].isnull()).flatten()):
        raise ValueError('null data in well spd')

    drn_data = _get_drn_spd(smt.reach_v, smt.wel_version)
    if any(np.array(drn_data.loc[:,['k','i','j','elev','cond']].isnull()).flatten()):
        raise ValueError('null data in drain spd')

    rch = _get_rch()
    if any(np.isnan(rch).flatten()):
        raise ValueError('null data in recharge spd')


# todo use this to make an automatic appendix
# todo make a model tools function to make said appendix
# todo make this an exicutable
# todo check out http://pbpython.com/pdf-reports.html for automatic reporting or pd.DataFrame.plot() should be able to only plot the table
# todo check out https://stackoverflow.com/questions/3444645/merge-pdf-files for pdf joining
# less manual checks/good to report to the group
#todo save plots and create a textfile for the tables ect so that I can just pop it into the memo
#todo link the below with the above into one mega function

# plots of cell overlap spatially...

# #locations of wells
def check_well_loc_discharge():
    # plot location layer by layer
    # plot location vs, flux
    # plot location of races
    # plot location of steams in the model
    # print zonal budgets (CWMS Zones/sub-zones)
    raise NotImplementedError

#locations of drains -- none below layer 0
def check_drain_locations():
    # check no non-layer 0 drains
    # plt drains group by group
    # plt drains target group by target group
    raise NotImplementedError
#locations of streams -- none below layer 0 #check segment data / check influx data
def check_sfr_locations():
    # check no streams in layers 1+
    # plot streams by group
    # plot streams by target group
    # plot stream flow targets
    # plot segments
    # plot stream slope
    # plot stream width
    # plot stream length
    # plot up/downstream segments
    # plot influxes
    # make plots of segment vs ground
    # table of stream segment data (inflow/ roughness/ width

    raise NotImplementedError

#rch spatially
def check_rch_spatially():
    # plot rch spatially
    # print zones of rch (CWMS zones/sub-zones)
    raise NotImplementedError
#check ibound
def check_ibound_spatially():
    # plot ibound for each layer
    raise NotImplementedError

#check constant heads
def check_constant_heads_spatially():
    # plot all constant heads
    raise NotImplementedError


#check elevations
def check_elevations_spatially():
    # plot 5m contours of each layer in the database

    # plot 5m contours of thickness for each layer
    raise NotImplementedError

#todo other semi-manual checks
#check wells are in teh right aquifer
#check target wells are in the right aquifer
#check starting heads, particularyly constant heads
#check bottom of layer 1 does not pool anything
#check package implementation
#check output
#run flopy check
#spot check all text files

if __name__ == '__main__':
    check_no_overlapping_features() #there are quite some
    #check_layer_overlap()
    check_elv_db()
    check_noflow_overlap()
    check_null_spd()