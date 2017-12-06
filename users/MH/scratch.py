from __future__ import division
from users.MH.Waimak_modeling.models.extended_boundry.m_packages import create_wel_package
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import create_wel_package

import netCDF4 as nc
import numpy as np

if __name__ == '__main__':
    data = nc.Dataset(r"C:\Users\MattH\Downloads\test.nc",'w')
    data.createDimension('bound',2)
    data.createDimension('unbound',2)

    temp = data.createVariable('test',float,('bound','unbound'),fill_value=np.nan)
    temp[0,:] = [1,2,3]
    temp[1] = [4,5,6,7,8,9]

    print 'done'