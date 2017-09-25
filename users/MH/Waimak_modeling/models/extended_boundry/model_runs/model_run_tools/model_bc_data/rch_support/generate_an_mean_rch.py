# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 25/09/2017 1:56 PM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
from glob import glob
import os

test_path = r"K:\niwa_netcdf\lsrm\lsrm_results\vcsn_65perc.h5"


def gen_water_year_average_lsr_irr(path):
    org_data = pd.read_hdf(path)
    g = org_data.groupby([pd.Grouper('site'), pd.Grouper(key='time', freq='A-JUN')])
    org_data.loc[:, 'total_drainage'] = org_data.loc[:, 'irr_drainage'] + org_data.loc[:, 'non_irr_drainage']
    org_data.loc[org_data.total_drainage.isnull(), 'total_drainage'] = org_data.loc[
        org_data.total_drainage.isnull(), 'non_irr_drainage']
    temp = org_data.drop_duplicates('site').set_index('site')
    temp.loc[temp.irr_area_ratio.isnull(), 'irr_area_ratio'] = 0
    outdata = g.aggregate({'precip': np.nansum, 'pet': np.nansum, 'paw': np.nansum, 'irr_drainage': np.nansum, 'irr_demand': np.nansum,
                           'non_irr_drainage': np.nansum, 'total_drainage': np.nansum}).reset_index()
    outdata.loc[:,'site_area'] = outdata.loc[:,'site']
    outdata.loc[:,'irr_area_ratio'] = outdata.loc[:,'site']
    outdata = outdata.replace({'site_area': temp.site_area.to_dict()})
    outdata = outdata.replace({'irr_area_ratio': temp.irr_area_ratio.to_dict()})
    for key in ['precip', 'pet', 'paw', 'total_drainage']: # raising memory error so trying to do it one at a time
        outdata[key] *= (1 / (365 * outdata.site_area * 1000))
    outdata['irr_drainage'] = (1 / (365 * outdata.site_area * outdata.irr_area_ratio * 1000))  # per area of irrigation
    outdata['irr_demand'] = (1 / (365 * 1000)) # m3/day
    outdata['non_irr_drainage'] = (1 / (365 * outdata.site_area * (1 - outdata.irr_area_ratio) * 1000)) # per area of non-irrigation
    return outdata

def gen_all_wy_averages(base_dir):
    out_dir = '{}/water_year_means'.format(base_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    paths = glob('{}/vcsn*.h5'.format(base_dir)) #todo update to all .h5
    for path in paths:
        outdata = gen_water_year_average_lsr_irr(path)
        outdata.to_hdf(os.path.join(out_dir,'wym_{}'.format(os.path.basename(path))),'wym',mode='w')



if __name__ == '__main__':
    gen_all_wy_averages(r"K:\niwa_netcdf\lsrm\lsrm_results")
