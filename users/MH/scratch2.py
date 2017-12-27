# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/11/2017 2:12 PM
"""

import timeit



print(timeit.timeit('timeit_test()',setup='from users.MH.scratch import timeit_test',number=10)/10)


