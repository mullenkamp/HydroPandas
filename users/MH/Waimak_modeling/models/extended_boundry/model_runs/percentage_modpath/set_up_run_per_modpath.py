# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 6/12/2017 1:47 PM
"""

from __future__ import division
from core import env
import numpy as np
import flopy
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.modpath_percentage import \
    create_mp_slf
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools import import_gns_model
import os

def part_group_cell_mapper():
    ibnd = smt.get_no_flow(0).flatten()
    js, iss = np.meshgrid(range(smt.cols), range(smt.rows)) # zero indexed to agree with python interpretation
    idx = ibnd==1
    out = dict(zip(range(1,idx.sum()+1),list(zip(iss.flatten()[idx],js.flatten()[idx]))))
    return out



def make_mp_particles(cbc_path):
    """
    make modpath particle locations from the cbc file.  particles are created in each top layer cell with the number
    relative to the influx
    :param cbc_path: path to the cell by cell budget file
    :return: np.record array
    """
    # particles must have a label as a string otherwise it terminates
    # mass weighted particles for each of the models for the sfr/well(races/rivers) and rch

    print('reading cbc')
    # calculate the flow into each top layer cell
    indata = flopy.utils.CellBudgetFile(cbc_path)
    rch = indata.get_data(kstpkper=(0, 0), text='recharge', full3D=True)[0][0].filled(0)  # take only the first row
    # take only the first row, no particles in southwertern boundary for wells
    well = indata.get_data(kstpkper=(0, 0), text='wells', full3D=True)[0][0].filled(0)
    sfr = indata.get_data(kstpkper=(0, 0), text='stream leakage', full3D=True)[0][0].filled(
        0)  # take only the first row
    well[well < 0] = 0
    sfr[sfr < 0] = 0
    flow = rch + well + sfr
    flow[smt.get_no_flow(0) != 1] = 0

    # generate particles (minimum of 1 per cell) #todo how many particles are reasonable? I could double this
    num_parts = np.round(flow / flow[flow > 0].min()).astype(int)

    # identify boundary condition types
    bd_type = smt.get_empty_model_grid()  # 0=rch,1=well,2==sfr
    temp = np.concatenate((rch[np.newaxis], well[np.newaxis], sfr[np.newaxis]), axis=0).max(axis=0)
    bd_type[np.isclose(rch,temp)] = 0
    bd_type[np.isclose(well,temp)] = 1
    bd_type[np.isclose(sfr,temp)] = 2
    bd_type[(smt.get_no_flow(0)!=1) | np.isclose(temp, 0)] = -1

    outdata = flopy.modpath.mpsim.StartingLocationsFile.get_empty_starting_locations_data(num_parts.sum())
    outdata['label'] = 's'
    outdata['k0'] = 0  # I think that particles in flopy are 1 indexed, but 0 means active most layer
    js, iss = np.meshgrid(range(1, smt.cols + 1), range(1, smt.rows + 1))  # this is passed to the file as 1 indexed

    ibnd = smt.get_no_flow(0).flatten()
    idx = ibnd == 1
    group_dict = part_group_cell_mapper()
    start_idx = 0
    print('generating particles')
    for l, (num, i, j, bt) in enumerate(zip(num_parts.flatten()[idx], iss.flatten()[idx], js.flatten()[idx], bd_type.flatten()[idx])):
        if num == 0:
            raise ValueError('unexpected zero points')
        end_idx = start_idx + num
        # set particle starting location
        outdata['particlegroup'][start_idx:end_idx] = l+1
        outdata['groupname'][start_idx:end_idx] = '{:03d}_{:03d}'.format(*group_dict[l+1])
        outdata['i0'][start_idx:end_idx] = i
        outdata['j0'][start_idx:end_idx] = j
        outdata['xloc0'][start_idx:end_idx] = np.random.uniform(size=num)
        outdata['yloc0'][start_idx:end_idx] = np.random.uniform(size=num)
        outdata['zloc0'][start_idx:end_idx] = 1 if bt != 1 else np.random.uniform(size=num)
        start_idx = end_idx
    return outdata

def get_cbc(model_id, base_dir): # todo probably ask brioch/run pest control file
    cbc_path = os.path.join(base_dir,'{}_for_modpath'.format(model_id),'{}_for_modpath.cbc'.format(model_id)) #todo check

    if os.path.exists(cbc_path):
        return cbc_path

    m = import_gns_model(model_id,'for_modpath',os.path.join(base_dir,'for_modpath'),False)
    m.write_name_file()
    m.upw.iphdry = 0  # hdry is -888.0

    m.write_input()
    m.run_model()

    return cbc_path


def setup_run_modpath(cbc_path, mp_ws, mp_name):
    particles = make_mp_particles(cbc_path)
    mp = create_mp_slf(particle_data=particles, mp_ws=mp_ws, hdfile=cbc_path.replace('cbc','hds'), budfile=cbc_path,
                       disfile=cbc_path.replace('cbc', 'dis'), mp_name=mp_name)
    print('writing model {}'.format(mp_name))
    mp.write_input()
    mp.write_name_file()
    print('running model {}'.format(mp_name))
    mp.run_model()


if __name__ == '__main__':
    import time
    t = time.time()
    test = make_mp_particles(r"C:\Users\MattH\Desktop\NsmcBase_simple_modpath\NsmcBase_modpath_base.cbc")
    print(time.time()-t)
    print('done')