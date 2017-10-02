"""
Author: matth
Date Created: 30/09/2017 9:14 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import numpy as np

def get_zone_array_index(zones):
    """
    returns a boolean array with true for the zones listed
    :param zones: one or more of chch, waimak, selwyn
    :return:
    """
    zones = np.atleast_1d(zones)
    zone_dict = {'waimak':4, 'chch':7 , 'selwyn':8}
    if any(~np.in1d(zones,zone_dict.keys())):
        raise ValueError('unknown zone')

    new_no_flow = smt.get_no_flow()
    zones_idx = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/cwms_zones.shp".format(smt.sdp),'ZONE_CODE')
    zones_idx[~new_no_flow[0].astype(bool)] = np.nan

    use_codes = [zone_dict[e] for e in zones]
    out_idx = np.in1d(zones_idx,use_codes)
    out_idx = np.reshape(out_idx,(smt.rows,smt.cols))
    return out_idx

if __name__ == '__main__':
    test = get_zone_array_index(['waimak','chch'])
    print('done')
