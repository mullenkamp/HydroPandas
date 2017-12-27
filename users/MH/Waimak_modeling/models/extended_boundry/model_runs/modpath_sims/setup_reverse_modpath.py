# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/12/2017 3:23 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.modpath_wrapper import \
    create_mp_slf
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import flopy
import pandas as pd
import itertools
import numpy as np
import os


def particle_loc_from_grid(grid_locs, group, root3_num_part=2):
    """

    :param grid_locs: a list of tuples(k,i,j) or tuple (k,i,j)
    :param group: a list of groups of same length gridlocs
    :param root3_num_part: the cubed root of the number of particles to put in each cell
    :return: starting location data
    """
    # grid_locs a list of tuples(k,i,j)
    # take a list of cell locations (k,i,j) and return a record array for starting location data
    # assume 8 particles per cel for now
    group = np.atleast_1d(group)
    t = set(group)
    group_mapper = {key: val for key, val in zip(range(1, len(t)+1), t)}
    group_num = np.array([group_mapper[e] for e in group])
    grid_locs = list(np.atleast_2d(grid_locs))
    grid_locs = [np.concatenate((grid, [gp, gn])) for grid, gp, gn in zip(grid_locs, group, group_num)]
    print('generating particles for {} cells'.format(len(grid_locs)))
    num_per_cel = root3_num_part ** 3
    num_par = len(grid_locs) * num_per_cel
    locals_vals = [[e / (root3_num_part + 1)] for e in range(1, root3_num_part + 1)]
    ploc = list(itertools.product(grid_locs, locals_vals, locals_vals, locals_vals))
    ploc = np.array([list(itertools.chain(*e)) for e in ploc])
    # k,i,j,x,y,z
    outdata = pd.DataFrame(flopy.modpath.mpsim.StartingLocationsFile.get_empty_starting_locations_data(npt=num_par))

    outdata['k0'] = ploc[:, 0]
    outdata['i0'] = ploc[:, 1]
    outdata['j0'] = ploc[:, 2]
    outdata['groupname'] = ploc[:, 3]
    outdata['particlegroup'] = ploc[:, 4]
    outdata['xloc0'] = ploc[:, 5]
    outdata['yloc0'] = ploc[:, 6]
    outdata['zloc0'] = ploc[:, 7]
    outdata['initialtime'] = 1  # should not matter for now as I am releasing all at once
    outdata['label'] = 's'  # a filler so that the loc file will read properly
    return group_mapper, outdata


def setup_run_backward_modpath(mp_ws, mp_name, cbc_file, index=None, group=None,
                               root3_num_part=1, capt_weak_s=False):
    """
    set up backward particle tracking
    :param mp_ws: modpath working directory
    :param mp_name: name of the modpath simulation
    :param cbc_file: path to the cbc file (other necissary files are assumed to be in teh same folder with
                     the same naming conventions
    :param index: a smt.layer,row,col boolean array or None (places particles in every active cell)
    :param group: None or array of ints defining the groups for each cell. If None and an index is passed assume that
                  Everything is in the same group, if None and an index is not passed assume that all cells are in their
                  own group
    :param root3_num_part: the cubic root of the number of particles (placed evenly in the cell) e.g.
                           root3_num_part of 2 places 8 particles in each cell
    :param capt_weak_s: bool if True terminate particles at weak sources
    :return:
    """

    if index is None:
        index = smt.get_no_flow() == 1
        if group is None:
            raise NotImplementedError('have not set up groups for all cells')

    if index is not None and group is None:
        group = smt.get_empty_model_grid(True).astype(int)
        group[index] = 1

    assert isinstance(index, np.ndarray), 'index must be ndarray'
    assert index.dtype == bool, 'index must be boolean'
    assert index.shape == (smt.layers, smt.rows, smt.cols), 'index shape must be {}, not {}'.format((smt.layers,
                                                                                                     smt.rows,
                                                                                                     smt.cols),
                                                                                                    index.shape)
    assert isinstance(group, np.ndarray), 'group must be ndarray'
    assert group.dtype == int, 'group must be int'
    assert group.shape == (smt.layers, smt.rows, smt.cols), 'group shape must be {}, not {}'.format((smt.layers,
                                                                                                     smt.rows,
                                                                                                     smt.cols),
                                                                                                    index.shape)

    if not os.path.exists(mp_ws):
        os.makedirs(mp_ws)

    group = group[index]
    group_mapper, particles = particle_loc_from_grid(smt.model_where(index), group, root3_num_part)
    hd_file = cbc_file.replace('.cbc', '.hds')
    dis_file = cbc_file.replace('.cbc', '.dis')
    group_mapper = pd.Series(group_mapper)
    group_mapper.to_csv(os.path.join(mp_ws,'{}_group_mapper.csv'.format(mp_name)))

    temp_particles = flopy.modpath.mpsim.StartingLocationsFile.get_empty_starting_locations_data(0)
    mp = create_mp_slf(particle_data=temp_particles, mp_ws=mp_ws, hdfile=hd_file, budfile=cbc_file, disfile=dis_file,
                       prsity=0.3, prsitycb=0.3, mp_name=mp_name, direction='backward',
                       simulation_type='pathline', capt_weak_s=capt_weak_s, time_pts=1, hnoflo=1e+30, hdry=-888.0,
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
    groups = particles[['particlegroup', 'groupname']].groupby('particlegroup').count().reset_index().rename(
        columns={'groupname': 'count'})
    groups.loc[:, 'groupname'] = groups.loc[:, 'particlegroup'].replace(
        dict(particles[['particlegroup', 'groupname']].itertuples(False, None)))
    group_count = len(groups.index)
    groups = pd.Series(groups[['groupname', 'count']].astype(str).values.flatten())
    with open(loc_path, 'w') as f:
        f.write('{}\n'.format(loc_package.heading))
        f.write('{:d}\n'.format(loc_package.input_style))
        f.write('{}\n'.format(group_count))

    groups.to_csv(loc_path, False, sep=' ', header=False, mode='a')

    # write particle data
    print ('writing loc particle data')
    particles.drop('groupname', 1, inplace=True)
    particles.to_csv(loc_path, sep=' ', header=False, index=False, mode='a')

    print('running model {}'.format(mp_name))
    mp.run_model()


if __name__ == '__main__':
    index = smt.get_empty_model_grid(True).astype(bool)
    temp = smt.shape_file_to_model_array(r"{}\m_ex_bd_inputs\shp\rough_chch.shp".format(smt.sdp), 'Id', True)
    index[5][np.isfinite(temp)] = True
    setup_run_backward_modpath(r"C:\Users\MattH\Desktop\test_reverse_modpath_strong", 'test_reverse',
                               r"C:\Users\MattH\Desktop\NsmcBase_simple_modpath\NsmcBase_modpath_base.cbc",
                               index,capt_weak_s=False)
