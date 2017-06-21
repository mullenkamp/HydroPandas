"""
quick script to export all of the flow data in CHCH west melton for lincon agritech
Author: matth
Date Created: 3/03/2017 3:48 PM
"""

from __future__ import division

from core.ecan_io import flow_import

site_shape = "C:/Users/MattH/Downloads/CHCH_WM.shp"
outdir = "P:/Groundwater/Christchurch West Melton/GSHP_mounding_project/Flow_data/"
flow_import(rec_sites=site_shape,
            gauge_sites=site_shape,
            export_flow=True,
            export_shp=True,
            export_rec_path=outdir+'recorder_data.csv',
            export_rec_shp_path=outdir + 'recorder_shape',
            export_gauge_path=outdir + 'gauge_data.csv',
            export_gauge_shp_path=outdir + 'gauge_shape')