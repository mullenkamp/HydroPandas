
select *
from [est_allo_usage_2017_09_28]
where crc in (select distinct crc from [allo_gis_2017-10-02] where swaz in ('Temuka')) and time >= '2013-01-01' and time < '2014-01-01'

