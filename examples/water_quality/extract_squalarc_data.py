# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 13:22:49 2017

@author: MichaelEK

Example script to extract vcsn data.
"""

from core.ecan_io.mssql import rd_squalarc

###########################################
### Parameters

mtypes1 = ['Water Temperature', 'Ammonia Nitrogen']

sites = ['SQ35872', 'J39/0109']
poly = r'Y:\VirtualClimate\examples\example_area.shp'

from_date = '2000-01-01'
to_date = '2017-01-01'

convert_dtl = True

out_csv = r'Y:\VirtualClimate\examples\squalarc_test1.csv'

##########################################
### Get the data

## Get all parameters (mtypes) for the two sites within the time period and don't convert the values below the detection limit (dtl) to numeric
df1 = rd_squalarc(sites=sites, from_date=from_date, to_date=to_date)

## Get two parameters for the two sites during the time period and convert the dtl (taking half the dtl)
df2 = rd_squalarc(sites=sites, mtypes=mtypes1, from_date=from_date, to_date=to_date, convert_dtl=convert_dtl)

## Get two parameters for all the sites within the polygon during the time period and convert the dtl (taking half the dtl)
df3 = rd_squalarc(sites=poly, mtypes=mtypes1, from_date=from_date, to_date=to_date, convert_dtl=convert_dtl)

#########################################
### Save the data

## Export the data as a csv file -- from the output dataframe
df2.to_csv(out_csv, encoding='utf-8', index=False)

## Export the data as a csv file -- from the original function
df2 = rd_squalarc(sites=sites, mtypes=mtypes1, from_date=from_date, to_date=to_date, convert_dtl=convert_dtl, export_path=out_csv)






























