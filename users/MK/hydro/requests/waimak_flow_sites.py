# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 14:24:37 2016

@author: MichaelEK
"""

from import_fun import rd_henry, rd_hydstra_csv


##################################
#### Parameters

gauging_sites_csv = 'C:/ecan/local/Projects/Waimakariri/GIS/tables/waimak_min_flow_sites.csv'
rec_flow_csv = 'C:/ecan/local/Projects/Waimakariri/data/RAW/waimak_flow.csv'

min_filter = 10
min_qual_code=20

export_path = 'C:/ecan/local/Projects/Waimakariri/data/waimak_gaugings.csv'
export_path_flow = 'C:/ecan/local/Projects/Waimakariri/data/waimak_flow.csv'

#################################
#### Gaugings data

waimak_gauge = rd_henry(sites=gauging_sites_csv, sites_col=1, years='all', agg_day=True, sites_by_col=False, min_filter=min_filter, export=True, export_path=export_path)

waimak_flow = rd_hydstra_csv(rec_flow_csv, qual_codes=True, min_qual_code=min_qual_code)

waimak_flow.to_csv(export_path_flow)



























