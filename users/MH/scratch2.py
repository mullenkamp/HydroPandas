# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/11/2017 2:12 PM
"""

import timeit



print('compressed')
print timeit.timeit('read_netcdf_comp()',setup='from users.MH.scratch import read_netcdf_comp',number=10)/10
print('uncompressed')
print timeit.timeit('read_netcdf_uncomp()',setup='from users.MH.scratch import read_netcdf_uncomp',number=10)/10


