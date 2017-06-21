# -*- coding: utf-8 -*-
"""
Created on Wed Oct 05 14:04:49 2016

@author: MichaelEK
"""

from import_fun import rd_hydrotel


########################################
#### Parameters

select = ['Browns Rock (WIL)']
input_type='name'

export_path = 'C:\\ecan\\local\\Projects\\requests\\browns_rock_export\\browns_rock_data.csv'


#######################################
#### Query data

df = rd_hydrotel(select, input_type='name', export=True, export_path=export_path)



















