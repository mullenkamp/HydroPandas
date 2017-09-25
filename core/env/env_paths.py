"""
Author: matth
Date Created: 23/03/2017 2:20 PM
"""

def gisdata(path):
    return '{}/{}'.format('//GISDataFS/GISData',path)

def sci(path):
    return '{}/{}'.format('//gisdata/Projects/SCI',path)

def data(path):
    return '{}/{}'.format('//fileservices02/managedshares/data', path)

def transfers(path):
    return '{}/{}'.format('//FileServices02/ManagedShares/Transfers',path)

def temp(path):
    return transfers('Temp/{}'.format(path))

def gw_met_data(path):
    return '//fs02/GroundWaterMetData$/{}'.format(path)

