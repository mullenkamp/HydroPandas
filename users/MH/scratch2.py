# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/11/2017 2:12 PM
"""

import timeit

#import flopy

print('1 layer')
print(timeit.timeit(stmt='write_to_nc(1)',setup='from users.MH.scratch import write_to_nc',number=1)/1)
print('5 layer')
print(timeit.timeit(stmt='write_to_nc(5)',setup='from users.MH.scratch import write_to_nc',number=1)/1)
print('11 layer')
print(timeit.timeit(stmt='write_to_nc(11)',setup='from users.MH.scratch import write_to_nc',number=1)/1)
print('all')
print(timeit.timeit(stmt='write_to_nc("all")',setup='from users.MH.scratch import write_to_nc',number=1)/1)
