# -*- coding: utf-8 -*-
"""
Created on Tue Dec 05 09:34:22 2017

@author: MichaelEK
"""
from os.path import join
from core.allo_use.ros import current_site_restr
#from core.misc.misc import save_df

############################################
### Parameters

base_dir = r'C:\onedrive\OneDrive - Environment Canterbury\share'
output_csv1 = 'restr_site.csv'
output_csv2 = 'restr_site_band.csv'

###########################################
### Run function

basic, complete = current_site_restr()

basic.to_csv(join(base_dir, output_csv1), index=False)
complete.to_csv(join(base_dir, output_csv2), index=False)



