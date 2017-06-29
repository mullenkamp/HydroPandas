# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 13:22:49 2017

@author: MichaelEK

Example script to extract vcsn data.
"""

from core.ecan_io.met import sel_niwa_vcsn

###########################################
### Parameters

mtypes1 = 'precip'
mtypes2 = ['precip', 'PET']

sites = ['P160113', 'P160112']
poly = r'Y:\VirtualClimate\examples\example_area.shp'

include_sites = True
out_crs = 2193

out_csv = r'Y:\VirtualClimate\examples\test1.csv'
out_nc = r'Y:\VirtualClimate\examples\test1.nc'

##########################################
### Get the data

## Get the precip data for the two sites, include the sites in the output, and convert the x and y coordinates from wgs 84 to NZTM
df1 = sel_niwa_vcsn(mtypes=mtypes1, sites=sites, include_sites=include_sites, out_crs=out_crs)

## Get both the precip and ET data for all sites within the polygon poly and add the sites to the output
df2 = sel_niwa_vcsn(mtypes=mtypes2, sites=poly, include_sites=include_sites)

#########################################
### Save the data

## Export the data as a csv file -- It might be large
df2.to_csv(out_csv, index=False)

## Export the data as a netcdf -- Will be smaller than csv
df2 = sel_niwa_vcsn(mtypes=mtypes2, sites=poly, include_sites=include_sites, netcdf_out=out_nc)






























