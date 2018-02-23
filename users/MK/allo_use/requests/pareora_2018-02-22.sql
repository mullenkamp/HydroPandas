

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















