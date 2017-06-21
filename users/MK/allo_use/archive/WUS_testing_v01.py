# -*- coding: utf-8 -*-
"""
Created on Fri Jun 03 08:41:04 2016

@author: MichaelEK
"""

ash2 = ash1[ash1.Dates == '2014-06-30']
ash3 = ash2[ash2.Usage.notnull()]

ash3_stock = ash3[ash3.WaterUseDetailText == 'stockwater']


ash3_stock[ash3_stock.WellNo == 'K36/0903']
t2 = allo2.loc[allo2.RecordNo == 'CRC012123', ['WellNo', 'FullEstAllocation', 'MaxRate', 'MaxVolume', 'consecDays', 'AllocatedRate']]
t3 = allo4.loc[allo2.RecordNo == 'CRC012123', ['WellNo', 'AllocatedRate',     'yearly_vol_m3', 'rate_tot', 'Ind_yr_vol_m3']]
t1 = allo.loc[allo.RecordNo == 'CRC012123', ['WellNo', 'FullEstAllocation', 'MaxRate', 'MaxVolume', 'consecDays', 'AllocatedRate']]


allo3.loc[allo3.MaxRate != allo3.AllocatedRate, ['WellNo', 'MaxRate', 'AllocatedRate', 'FullEstAllocation', 'MaxVolume']]


allo_use3[allo_use3.WellNo == 'K36/0903']

all1[all1.WellNo == 'BU24/0007']


otop2 = otop1[otop1.Dates == '2012-06-30']
otop3 = otop2[otop2.Usage.notnull()]

big_index = (otop3.Usage / otop3.Mon_vol_m3) > 1
otop3.loc[big_index, ['RecordNo', 'WellNo', 'Usage', 'Mon_vol_m3']]

allo.loc[allo.RecordNo == 'CRC981008.2', ['WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3']]

allo2.loc[allo.RecordNo == 'CRC981008.2', ['WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3']]

allo3.loc[allo.RecordNo == 'CRC012031', ['WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3', 'FullEstAllocation']]

otop2 = otop1[otop1.Dates == '2014-06-30']
otop3 = otop2[otop2.Usage.notnull()]

otop4 = otop3[otop3.WaterUseDetailText == 'public_supply']


allo.loc[allo.RecordNo == 'CRC981008.2', ['WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3', 'FullEstAllocation']]



allo.loc[:, ['RecordNo', 'WellNo', 'ActivityCode', 'FullEstAllocation', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'FullEstAllocation']]


allo3.loc[:, ['RecordNo', 'WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3', 'FullEstAllocation']]

allo3.loc[:, ['RecordNo', 'WellNo', 'MaxRate', 'MaxVolume',  'AllocatedRate', 'consecDays', 'daily_vol_m3', 'FullEstAllocation','yearly_vol_m3']]


allo_use1[allo_use1.WellNo == 'BU24/0007']

allo2[allo2.RecordNo == 'CRC154725']
allo2[allo2.WellNo == 'n33/0441']

####################################
## New combo!

# These have the WAP and the greater zones
Wells.dbo.WMCR_Zones
Wells.dbo.WELL_DETAILS

# These have crc, wap, take type, allocation zones, catchments, rates, use type, ...
DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterCombined
DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterWellsSwaps

# This has irrigation parameters used for the FullEstAllocation calc (with crc)
DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterIrrigation

sum(takes3.max_rate.isnull())
sum(takes3.max_rate == 0)

sum(takes3.max_rate_wap.isnull())
sum(takes3.max_rate_wap == 0)


takes2[takes2.crc == 'CRC000299']
takes4[takes4.crc == 'CRC000385']
takes4[takes4.ind_ann_allo.isnull()]
takes6[['crc', 'wap', 'cwms_zone']]

def wqn10(irr_par):
    
    irr_par.loc[:, 'ann1'] = 910 - 1.6*(irr_par.paw - 100)
    irr_par.loc[irr_par.paw < 100, 'ann1'] = 750
    irr_par.loc[irr_par.paw > 200, 'ann1'] = 910

    irr_par.loc[:, 'demand'] = irr_par.ann1 - irr_par.siwl
    irr_par.loc[:, 'ann_vol'] = irr_par.demand * 10 * irr_par.irr_area
    irr_par.loc[irr_par.ann_vol.isnull(), 'ann_vol'] = 0

    return(irr_par.ann_vol)


ann_vol = wqn10(irr_par)

test2 = allo_proc(takes, wap, dates, zone, zone_add, takes_names, wap_names, dates_names, zone_names, zone_add_names, irr_par, irr_names, use_cav=False, export_path='C:/ecan/base_data/usage/takes_results2.csv')

'C:/ecan/base_data/usage/allo_mon4.csv'

col1 = ['crc', 'wap', 'usage_m3', 'max_allo_m3']

df11 = df10.loc[df10.usage_m3.notnull(), col1]

ratio1 = df11.usage_m3 / df11.max_allo_m3

df11[ratio1 >1]

takes[takes.crc == 'CRC042190.2']
wap[wap.PARENT_B1_ALT_ID == 'CRC042190.2']

w_use[w_use.wap == 'K36/0830']

sum(series.max_allo_m3 == inf)
sum(series.usage_m3 == inf)

sum(data2.Mon_vol_m3 == inf)

sum(allo1.ind_ann_allo == inf)

sum(takes7.ind_ann_allo.isnull())


allo1 = read_csv('C:/ecan/base_data/usage/takes_results2.csv')
allo1.columns

allo1['crc', u'wap', u'max_rate_wap', ]


crc = ['CRC950292.3', 'CRC971810.3', 'CRC991239.4', 'CRC050870.1']

q2 = w_query(allo_use1, grp_by=['dates'], allo_col=['ann_allo_m3', 'ann_allo_wqn_m3'], export=False, crc=crc)

q1 = w_query(allo_use5, grp_by=['dates'], allo_col=['ann_allo_m3', 'ann_allo_wqn_m3', 'up_allo_m3', 'up_allo_wqn_m3'], export=False)





