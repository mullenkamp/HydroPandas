# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from misc_fun import printf
from gaugings_fun import rd_henry, gauge_proc

########################################
##### Parameters

#### Import parameters

sites = "C:/ecan/Projects/otop/gains_losses/opihi/gauging_sites_loc.csv"
#sites = "C:/ecan/Projects/otop/gains_losses/orari/gauging_sites_loc.csv"

min_sites = 15

export_flow_path = "C:/ecan/Projects/otop/gains_losses/opihi/gauging_flows.csv"
#export_flow_path = "C:/ecan/Projects/otop/gains_losses/orari/gauging_flows.csv"

########################################
### Run processes

gauge_flow = rd_henry(sites, sites_col=3, agg_day=True, sites_by_col=True)

final_flow, gauge_count = gauge_proc(gauge_flow, min_gauge=min_sites, export_path=export_flow_path)
