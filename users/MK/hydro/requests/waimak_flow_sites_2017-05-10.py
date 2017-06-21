# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 14:24:37 2016

@author: MichaelEK
"""

from pandas import read_csv, concat
from core.ecan_io import rd_ts
from core.ts.sw import malf7d
from core.misc import rd_dir

##################################
#### Parameters

flow_dir = r'E:\ecan\local\Projects\Waimakariri\analysis\flow'

#flow_rec_csv = r'E:\ecan\shared\projects\waimak\data\waimak_flow_rec_data.csv'
#flow_nat_csv = r'E:\ecan\shared\projects\waimak\data\waimak_flow_nat_data.csv'

export_path = r'E:\ecan\shared\projects\waimak\data\waimak_malfs.csv'

#################################
#### Load data

files = rd_dir(flow_dir, 'csv')






#### Run MALFS

flow_rec_malf = malf7d(flow_rec)
flow_rec_malf.columns.name = 'flow_rec_malfs'

flow_nat_malf = malf7d(flow_nat)
flow_nat_malf.columns.name = 'flow_nat_malfs'

#### Combine data

malfs1 = concat([flow_rec_malf, flow_nat_malf], axis=1, keys=['flow_rec_malfs', 'flow_nat_malfs'])























