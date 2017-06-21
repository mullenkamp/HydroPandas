"""
Author: matth
Date Created: 23/03/2017 1:33 PM
"""

from __future__ import division
import geopandas as gp
import matplotlib.pyplot as plt
from osgeo import ogr


"""
The goal here is to define a function/object which opens the topo layers in our dataset and then plots the well or list
of wells on top of that layer labeled by the well name

possible arguments.
extent: BL/TR or None
well_list: list of wells or None
subplots: to be integrated with matplot lib
axes: an axes to plot on to integrate with matplotlib

functions to load, save, plot, quickplot, return fig and ax

set a minimum axis bound (e.g. 20k) and then set the extent plotted to

it looks like GDAL may be able to open layer files it should be simple from there.
"""

test = ogr.Open(r"L:\Base_Map\Topo\NZ Topo 250.lyr")

print (test)
"""
another function to plot geotiffs on an axis or fig or to return fig/axes
"""