

select Site, Time, Value as WUsage from Hydro.dbo.vTSUsageDaily where Site in ('M33/0241', 'M33/0322')
order by Site, Time

select Site, Time, Value as WUsage 
from Hydro.dbo.vTSUsageDaily 
where Site in (select wap
				from Hydro.dbo.[allo_gis_2017-10-02]
				where crc in ('CRC951305'))
order by Site, Time

select catch_grp, Time, sum(Value) as WUsage 
from Hydro.dbo.vTSUsageDaily
inner join (select distinct wap, catch_grp
				from Hydro.dbo.[allo_gis_2017-10-02]
				where catch_grp in (664, 651)) as a1 
			on a1.wap = vTSUsageDaily.Site
group by catch_grp, Time
order by catch_grp, Time



select * from Hydro.dbo.est_allo_usage_2017_09_28 where wap in ('M33/0241', 'M33/0322')
order by wap, time

select * 
from Hydro.dbo.est_allo_usage_2017_09_28
where wap in (select wap
				from Hydro.dbo.[allo_gis_2017-10-02]
				where crc in ('CRC951305'))
order by wap, time

select catch_grp, time, sum(mon_allo_m3) as mon_allo_m3, sum(mon_restr_allo_m3) as mon_restr_allo_m3, sum(mon_usage_m3) as mon_usage_m3,
	sum(usage_est) as usage_est, sum(sd_usage) as sd_usage
from Hydro.dbo.est_allo_usage_2017_09_28
inner join (select distinct wap, catch_grp
				from Hydro.dbo.[allo_gis_2017-10-02]
				where catch_grp in (664, 651)) as a1 
			on a1.wap = est_allo_usage_2017_09_28.wap
group by catch_grp, time
order by catch_grp, Time










select use_type, time, round(sum(mon_allo_m3), 0) as allo, round(sum(sd_usage), 0) as sd_usage
from Hydro.dbo.est_allo_usage_2017_09_28
where crc in (select crc 
	from [allo_gis_2017-10-02]
	where catch_grp = 701 and status_details = 'Issued - Active' and allo_block = 'A')
	and sd_usage > 0
	and allo_block = 'A'
group by use_type, time


select use_type, round(sum(max_rate * sd1_150 * 0.01), 2) as max_rate
from [allo_gis_2017-10-02]
where catch_grp = 701 and status_details = 'Issued - Active' and allo_block = 'A'
group by use_type


select use_type, time, round(sum(mon_allo_m3), 0) as allo, round(sum(sd_usage), 0) as sd_usage
from Hydro.dbo.est_allo_usage_2017_09_28
where crc in (select crc 
	from [allo_gis_2017-10-02]
	where swaz in ('Upper Pareora River', 'Lower Pareora River') and status_details = 'Issued - Active' and allo_block = 'A')
	and sd_usage > 0
	and allo_block = 'A'
group by use_type, time


select swaz, use_type, round(sum(max_rate * sd1_150 * 0.01), 2) as max_rate
from [allo_gis_2017-10-02]
where swaz in ('Upper Pareora River', 'Lower Pareora River')  and status_details = 'Issued - Active' and allo_block = 'A'
group by swaz, use_type
order by swaz, use_type

select swaz, use_type, round(sum(max_rate * sd1_150 * 0.01), 2) as max_rate
from [allo_gis_2017-10-02]
where catch_grp = 701  and status_details = 'Issued - Active' and allo_block = 'A'
group by swaz, use_type
order by swaz, use_type













