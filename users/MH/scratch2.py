# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/11/2017 2:12 PM
"""

import timeit



print('one read')
print timeit.timeit('read_netcdf()',setup='from users.MH.scratch import read_netcdf',number=1000)/1000

print '5read'
print timeit.timeit('read_netcdf(5)',setup='from users.MH.scratch import read_netcdf',number=1000)/1000
print('done')

