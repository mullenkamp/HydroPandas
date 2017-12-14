# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/12/2017 3:23 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.modpath_percentage import \
    create_mp_slf
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import flopy
import pandas as pd
import itertools
import numpy as np

def particle_loc_from_grid(grid_locs, root3_num_part = 2):
    """

    :param grid_locs: a list of tuples(k,i,j) or tuple (k,i,j)
    :return: starting location data
    """
    # grid_locs a list of tuples(k,i,j)
    # take a list of cell locations (k,i,j) and return a record array for starting location data
    # assume 8 particles per cel for now
    grid_locs = list(np.atleast_1d(grid_locs))
    num_per_cel = root3_num_part**3
    num_par = len(grid_locs)*num_per_cel
    locals_vals = [[e/(root3_num_part+1)] for e in range(1,root3_num_part+1)]
    ploc = list(itertools.product(grid_locs,locals_vals,locals_vals,locals_vals))
    ploc = np.array([list(itertools.chain(*e)) for e in ploc])
    # k,i,j,x,y,z
    outdata = flopy.modpath.mpsim.StartingLocationsFile.get_empty_starting_locations_data(npt=num_par)

    outdata['k0'] = ploc[:, 0]
    outdata['i0'] = ploc[:, 1]
    outdata['j0'] = ploc[:, 2]
    outdata['xloc0'] = ploc[:, 3]
    outdata['yloc0'] = ploc[:, 4]
    outdata['zloc0'] = ploc[:, 5]
    outdata['initialtime'] = 1 # should not matter for now as I am releasing all at once
    outdata['label'] = 's' # a filler so that the loc file will read properly
    return outdata

def setup_backward_modpath(mp_ws, mp_name, cbc_file, index=None, root3_num_part=1): #todo pathline or endpoint
    """
    set up backward particle tracking
    :param mp_ws: modpath working directory
    :param mp_name: name of the modpath simulation
    :param cbc_file: path to the cbc file (other necissary files are assumed to be in teh same folder with
                     the same naming conventions
    :param index: a smt.layer,row,col boolean array or None (places particles in every active cell)
    :param root3_num_part: the cubic root of the number of particles (placed evenly in the cell) e.g.
                           root3_num_part of 2 places 8 particles in each cell
    :return:
    """

    if index is None:
        index = smt.get_no_flow() == 1
    assert isinstance(index,np.ndarray), 'index must be ndarray'
    assert index.dtype == bool, 'index must be boolean'
    assert index.shape == (smt.layers,smt.rows,smt.cols), 'index shape must be {}, not {}'.format((smt.layers,
                                                                                                   smt.rows,smt.cols),
                                                                                                  index.shape)

    particles = particle_loc_from_grid(smt.model_where(index), root3_num_part)
    hd_file = cbc_file.replace('.cbc', '.hds')
    dis_file = cbc_file.replace('.cbc', '.dis')

    temp_particles = flopy.modpath.mpsim.StartingLocationsFile.get_empty_starting_locations_data(0)
    mp = create_mp_slf(particle_data=temp_particles, mp_ws=mp_ws, hdfile=hd_file, budfile=cbc_file, disfile=dis_file,
                  prsity=0.3, prsitycb=0.3, mp_name=mp_name, direction='backward',
                  simulation_type='endpoint', capt_weak_s=False, time_pts=1, hnoflo=1e+30, hdry=-888.0,
                  laytype=(1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    print('writing model {}; ignore the "no data to write" comment (this is a hack)'.format(mp_name))
    mp.write_input()
    mp.write_name_file()

    # write loc file with pandas to save time
    # simple speed test writing particles with flopy and running model took 30 min, writing with pandas took __min
    loc_path = mp.get_package('loc').fn_path
    loc_package = mp.get_package('loc')
    # write groups
    print ('writing group loc data')
    groups = particles[['particlegroup','groupname']].groupby('particlegroup').count().reset_index().rename(columns={'groupname':'count'})
    groups.loc[:,'groupname'] = groups.loc[:,'particlegroup'].replace(dict(particles[['particlegroup','groupname']].itertuples(False,None)))
    group_count = len(groups.index)
    groups = pd.Series(groups[['groupname','count']].astype(str).values.flatten())
    with open(loc_path,'w') as f:
        f.write('{}\n'.format(loc_package.heading))
        f.write('{:d}\n'.format(loc_package.input_style))
        f.write('{}\n'.format(group_count))

    groups.to_csv(loc_path,False,sep=' ',header=False, mode='a')

    # write particle data
    print ('writing loc particle data')
    particles.drop('groupname', 1, inplace=True)
    particles.to_csv(loc_path,sep=' ',header=False,index=False,mode='a')

    print('running model {}'.format(mp_name))
    mp.run_model()
