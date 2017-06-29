# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 10:42:42 2017

@author: MichaelEK
"""

from core.ecan_io.met import rd_niwa_vcsn
from core.ts.met.interp import sel_interp_agg


#############################################
### Parameters

server1 = 'SQL2012PROD05'
db1 = 'GIS'
tab_irr = 'AQUALINC_NZTM_IRRIGATED_AREA_20160629'
tab_paw = 'vLANDCARE_NZTM_Smap_Consents'

irr_cols = ['type']
irr_cols_rename = {'type': 'irr_type'}
paw_cols = ['WeightAvgPAW']
paw_cols_rename = {'WeightAvgPAW': 'paw'}

bound_shp = 'E:\ecan\local\Projects\requests\waimak\2017-06-12\waimak_area.shp'









































