# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""
import os
import pandas as pd
#from core.ecan_io.hilltop import rd_hilltop_data, rd_hilltop_sites, proc_ht_use_data, convert_site_names
from hydropandas.io.tools.hilltop import rd_hilltop_sites, rd_ht_quan_data, convert_site_names, proc_ht_use_data
from hydropandas.util.misc import rd_dir, save_df
from hydropandas.tools.general.ts.resampling import grp_ts_agg


#######################################################
#### Parameters

#fpath = {'tel': r'E:\ecan\hilltop\xml_test\tel', 'annual': r'E:\ecan\hilltop\xml_test\annual', 'archive': r'E:\ecan\hilltop\xml_test\archive'}
#fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive'}
fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive', 'ending_2017': r'\\hilltop01\Hilltop\Data\Annual\ending_2017'}
exclude_hts = ['Anonymous_AccToVol.hts', 'Anonymous_FlowToVolume.hts', 'RenameSites.hts', 'Telemetry.hts', 'Boraman2015-16.hts', 'FromWUS.hts', 'WUS_Consent2015.hts', 'AIC.hts', 'Boraman2015-16BU20170919.hts', 'Boraman2016-17BU20170919.hts', 'FromWUSBU20170919.hts', 'WUS_Consent2015.hts']

hts1 = r'\\hilltop01\Hilltop\Data\Telemetry\WaterMetrics.hts'
allo_gis_csv = 'E:/ecan/shared/base_data/usage/allo_gis.csv'
allo_use_2017_export = r'E:\ecan\local\Projects\requests\Ilja\2017-08-04\allo_use_2016-2017_v02.csv'

output_base = r'E:\ecan\local\Projects\hilltop\2018-01-19'
sites_csv = 'ht_site_info.csv'
sites_csv2 = 'ht_site_info2.csv'
out_data_w_m = 'ht_usage_with_Ms.csv'
out_data = 'ht_usage_no_Ms.csv'
out_data_hdf = 'ht_usage_daily.h5'
out_data_summ_csv = 'ht_use_allo_2016-2017.csv'

sql_raw_export = {'table': 'HilltopTSUsageDailyRaw', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
sql_raw_dtypes = {'Site': 'varchar(79)', 'FeatureMtypeSourceID': 'int', 'Time': 'date', 'Value': 'float', 'QualityCode': 'smallint'}
sql_raw_pkey = ['Site', 'FeatureMtypeSourceID', 'Time']
sql_raw_fkey = ['FeatureMtypeSourceID']
sql_raw_ftable = 'FeatureMtypeSource'

sql_prime_export = {'table': 'HilltopTSUsageDailyPrime', 'server': 'SQL2012DEV01', 'database': 'Hydro'}
sql_prime_dtypes = {'Site': 'varchar(79)', 'FeatureMtypeSourceID': 'int', 'Time': 'date', 'Value': 'float', 'QualityCode': 'smallint'}
sql_prime_pkey = ['Site', 'FeatureMtypeSourceID', 'Time']
sql_prime_fkey = ['FeatureMtypeSourceID']
sql_prime_ftable = 'FeatureMtypeSource'

########################################################
#### Functions



####################################################

ht_sites_lst = []
ht_data = pd.DataFrame()
for i in fpath:
    hts_files = rd_dir(fpath[i], '.hts')
    for j in hts_files:
        if j not in exclude_hts:
            print(os.path.join(fpath[i], j))
            ### Data
            ht_data1, sdata = rd_ht_quan_data(os.path.join(fpath[i], j), agg_period='day', output_site_data=True, exclude_mtype=['Regularity'])
            ## Sites
            sdata['hts_file'] = j
            sdata['folder'] = os.path.split(fpath[i])[1]
            ht_sites_lst.append(sdata)
            if ht_data.empty:
                ht_data = ht_data1.copy()
            else:
                ht_data = ht_data.combine_first(ht_data1)

ht_sites = pd.concat(ht_sites_lst)
ht_sites = ht_sites.drop_duplicates()
#ht_data = concat(ht_data_lst)

ht_data1 = ht_data.reset_index()

ht_sites.to_csv(os.path.join(output_base, sites_csv), index=False, encoding='utf8')

#ht_data = rd_hilltop_data(hts1, sites=None, mtypes=None, start=None, end=None, agg_period='day', agg_n=1, fun=None)

ht_site_names = convert_site_names(ht_sites.site)

ht2 = proc_ht_use_data(ht_data, n_std=20)
ht2.to_csv(os.path.join(output_base, out_data_w_m), header=True, encoding='utf8')

ht3 = ht2.reset_index()
site_names = ht3.site

site_names1 = convert_site_names(site_names)
ht3.loc[:, 'site'] = site_names1
ht4 = ht3[ht3.site.notnull()]

ht5 = ht4.groupby(['site', 'time']).data.sum().round(2)
ht5.to_csv(os.path.join(output_base, out_data), header=True)
save_df(ht5, os.path.join(output_base, out_data_hdf))

###################################################
#### Load in the allocation info

start = '2016-07-01'
end = '2017-06-30'

allo_gis = read_csv(allo_gis_csv)
ht5 = read_hdf(join(output_base, out_data_hdf))
ht6 = ht5.reset_index()
ht6 = ht6[(ht6.time >= start) & (ht6.time <= end)]
ht6.columns = ['wap', 'date', 'usage']
allo_ts_mon = allo_ts_proc(allo_gis, start=start, end=end, freq='M', in_allo=False)
allo_use = allo_use_proc(allo_ts_mon, ht6)
count1 = ht6.groupby('wap')['usage'].count()
first1 = ht6.groupby('wap')['date'].first()
last1 = ht6.groupby('wap')['date'].last()

stats1 = concat([first1, last1, count1], axis=1)
stats1.columns = ['first_date', 'last_date', 'data_count']

ht_sites1 = ht_sites.copy()
ht_sites1.loc[:, 'site'] = convert_site_names(ht_sites.site)
ht_sites2 = ht_sites1[ht_sites1.site.notnull()]
ht_sites2.to_csv(join(output_base, sites_csv2), index=False, encoding='utf8')

allo_use1 = grp_ts_agg(allo_use, ['crc', 'take_type', 'wap'], 'date', 'A-JUN', 'sum')

#################################################
#### Combine all results

res1 = merge(allo_use1, stats1.reset_index(), on='wap', how='left')
res1.to_csv(join(output_base, out_data_summ_csv), index=False)










#####################################################
#### Testing

hts1 = r'\\hilltop01\Hilltop\Data\Telemetry\WaterMetrics.hts'
hts2 = r'\\hilltop01\Hilltop\Data\Telemetry\WaterCheck.hts'

site = ['K38/1845-M1']

ht1 = rd_hilltop_data(hts1, sites=site, mtypes=None, start=None, end=None, agg_period='day', agg_n=1, fun=None)

ht1.index = ht1.index.droplevel(['mtype', 'site'])

ht1.plot()

ht2 = proc_ht_use_data(ht1, n_std=20)
ht2.index = ht2.index.droplevel(['site'])
ht2.plot()

data = read_csv(join(output_base, out_data_summ_csv))
site_info = read_csv(join(output_base, sites_csv2))

wap1 = 'M35/11235'

s1 = site_info[site_info.site == wap1]
d1 = data[data.wap == wap1]

ht1 = rd_hilltop_data(hts2, sites=[wap1 + '-M1'], mtypes=None, start=None, end=None, agg_period=None, agg_n=1, fun=None)

ht1.index = ht1.index.droplevel(['mtype', 'site'])
start1 = ht1.idxmin()
end1 = ht1.idxmax()
diff1 = end1 - start1
diff1.days

ht1.count()/4/24

ht2 = ht1.groupby(Grouper(level='time', freq='D')).count()
ht2[ht2 < 96]

ht1['2016-03-05']


res1 = read_csv(join(output_base, out_data_summ_csv))

res1[res1.crc == 'CRC030682']



hts_files = hts_files[1:]

fpath = {'archive': r'\\hilltop01\Hilltop\Data\Archive', 'ending_2017': r'\\hilltop01\Hilltop\Data\Annual\ending_2017'}












