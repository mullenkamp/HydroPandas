

select Site, Time, Value as Flow
from Hydro.dbo.vBgaugingTSData
where Site in ('253', '66215', '389', '387')
	and FeatureMtypeSourceID = 8

	
select *
from [allo_gis_2017-10-02]
where swaz in ('Saltwater Creek', 'Ashley River', 'Taranaki Creek', 'Waikuku Stream', 'Little Ashley')
	and status_details = 'Issued - Active' 
	and allo_block = 'A'


select *
from Hydro.dbo.est_allo_usage_2017_09_28
where crc in (select DISTINCT(crc)
from [allo_gis_2017-10-02]
where swaz in ('Saltwater Creek', 'Ashley River', 'Taranaki Creek', 'Waikuku Stream', 'Little Ashley')
	and status_details = 'Issued - Active')


select Site, Time, Value as Precip
FROM Hydro.dbo.NiwaAquaAtmosTSDataDaily
where Site = 4842 
	and FeatureMtypeSourceID in (26, 38)

select Site, Time, Value as PenmanET
FROM Hydro.dbo.NiwaAquaAtmosTSDataDaily
where Site = 4843 
	and FeatureMtypeSourceID in (24, 25)
order by Time











