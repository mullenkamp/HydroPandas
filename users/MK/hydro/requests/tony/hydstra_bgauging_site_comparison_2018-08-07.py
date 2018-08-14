# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 10:19:51 2018

@author: MichaelEK
"""

#####################################
### Comparison of Bgauging sites to Hydstra sites

import os
import numpy as np
import pandas as pd
from pdsql import mssql

base_dir = r'E:\ecan\local\Projects\requests\tony\2018-08-08'
export_mis_bg = 'sites_missing_in_BG.csv'
export_zero_hy = 'sites_without_loc_hydstra.csv'
export_bad_loc = 'sites_mismatch_location.csv'


bg1 = mssql.rd_sql('sql2012prod04', 'bgauging', 'rsites', ['SiteNumber', 'NZTMX', 'NZTMY', 'Altitude'], rename_cols=['site', 'x', 'y', 'z'])
bg2 = bg1[bg1.site > 0].copy()
bg2.x = bg2.x.round()
bg2.y = bg2.y.round()
bg2.z[bg2.z <= 0] = 0
bg2.z[bg2.z.isnull()] = 0
bg2 = bg2.sort_values('site')

hy1 = mssql.rd_sql('sql2012prod03', 'hydstra', 'site', ['station', 'easting', 'northing', 'elev'], rename_cols=['site', 'x', 'y', 'z'])
hy1.site = pd.to_numeric(hy1.site, errors='coerce')
hy2 = hy1[hy1.site > 0].copy()
hy2.x = hy2.x.round()
hy2.y = hy2.y.round()
hy2.z[hy2.z <= 0] = 0
hy2.z[hy2.z.isnull()] = 0
hy2 = hy2.sort_values('site')

not_bg = hy2[~hy2.site.isin(bg2.site)]

not_hy = bg2[~bg2.site.isin(hy2.site)]

zero_loc_hy = hy2[(hy2.x < 1000000) | (hy2.y < 4700000)]

merge1 = pd.merge(bg2, hy2, on='site', suffixes=['_bg', '_hy'])

#bad_loc = merge1[(merge1.x_bg != merge1.x_hy) | (merge1.y_bg != merge1.y_hy) | (merge1.z_bg != merge1.z_hy)]

merge1['x_diff'] = merge1.x_bg - merge1.x_hy
merge1['y_diff'] = merge1.y_bg - merge1.y_hy
merge1['z_diff'] = merge1.z_bg - merge1.z_hy

bad_loc = merge1[((merge1.x_diff < -1) | (merge1.x_diff > 1 )) | ((merge1.y_diff < -1 ) | (merge1.y_diff > 1 )) | (merge1.z_diff != 0)].sort_values(['x_diff', 'y_diff'])

## Save
not_bg.to_csv(os.path.join(base_dir, export_mis_bg), index=False)
zero_loc_hy.to_csv(os.path.join(base_dir, export_zero_hy), index=False)
bad_loc.to_csv(os.path.join(base_dir, export_bad_loc), index=False)