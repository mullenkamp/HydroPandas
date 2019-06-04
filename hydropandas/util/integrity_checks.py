# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:42:06 2017

@author: MichaelEK
Functions for checking integrity.
"""
from numpy import in1d


def _check_mtypes_sites(self, mtype):
    d1 = {i: all(in1d(mtype, self.sites_mtypes[i])) for i in self.sites_mtypes}
    return d1


### mtypes checking for importing tools
def _mtype_check(self, name):
    cond = name in self.mtypes
    return cond

