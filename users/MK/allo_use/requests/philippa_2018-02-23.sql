

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

select a.crc, a.take_type, a.allo_block, a.use_type, a.wap, b.GW_province, a.[time], a.mon_allo_m3, a.mon_restr_allo_m3, a.mon_usage_m3, a.usage_est, a.sd_usage
from Hydro.dbo.est_allo_usage_2017_09_28 as a
inner join Hydro.dbo.[consents_gw_envts_2018-02-23] as b
	on b.crc = a.crc
	and b.take_type = a.take_type
	and b.allo_block = a.allo_block
	and b.wap = a.wap
	and b.use_type = a.use_type











