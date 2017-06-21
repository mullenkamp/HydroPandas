# -*- coding: utf-8 -*-
"""
Created on Wed May 10 09:06:07 2017

@author: TinaB
"""

##############################################
#### Reading squalarc data

from core.ecan_io import rd_squalarc

sites1 = ['SQ30141']
sites2 = ['SQ10805']
sites3 = ['SQ30147']


wq1 = rd_squalarc(sites1)


wq2 = rd_squalarc(sites2)
wq2

wq3 = rd_squalarc(sites3, mtypes=['Rain', 'Rain Previously'])
wq3['parameter'].unique()

wq3 = rd_squalarc(sites3)
wq3
wq3['parameter'].sort_values().unique().tolist()
wq3['source'].sort_values().unique().tolist()

wq3.date.dt.month


















































