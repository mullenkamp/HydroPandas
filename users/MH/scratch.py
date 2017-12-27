from __future__ import division
from users.MH.Waimak_modeling.models.extended_boundry.m_packages import create_wel_package
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import create_wel_package
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from scipy.interpolate import rbf
import netCDF4 as nc
import numpy as np
from core import env
import shutil
import os

def timeit_test():
    base_converter_dir = "{}/base_for_nsmc_real".format(smt.sdp)
    # check if the model has previously been saved to the save dir, and if so, load from there
    converter_dir = os.path.join(os.path.expanduser('~'),'temp_nsmc_generation{}'.format(os.getpid()))
    shutil.copytree(base_converter_dir,converter_dir)
    shutil.rmtree(converter_dir)




if __name__ == '__main__':
    print 'hello world'
    data = nc.Dataset(r"C:\Users\MattH\Downloads\test.nc",'w')
    data.createDimension('bound',2)
    data.createDimension('unbound',2)

    temp = data.createVariable('test',float,('bound','unbound'),fill_value=np.nan)
    temp[0,:] = [1,2,3]
    temp[1] = [4,5,6,7,8,9]

    print 'done'