from __future__ import division
from users.MH.Waimak_modeling.models.extended_boundry.m_packages import create_wel_package
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import create_wel_package
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from scipy.interpolate import rbf
import netCDF4 as nc
import numpy as np

def timeit_test():
    layers, rows, cols = np.meshgrid(range(smt.layers), range(smt.rows), range(smt.cols), indexing='ij')
    ids = ['{:02d}_{:03d}_{:03d}'.format(k, i, j) for k, i, j in zip(layers.flatten(), rows.flatten(), cols.flatten())]

if __name__ == '__main__':
    print 'hello world'
    data = nc.Dataset(r"C:\Users\MattH\Downloads\test.nc",'w')
    data.createDimension('bound',2)
    data.createDimension('unbound',2)

    temp = data.createVariable('test',float,('bound','unbound'),fill_value=np.nan)
    temp[0,:] = [1,2,3]
    temp[1] = [4,5,6,7,8,9]

    print 'done'