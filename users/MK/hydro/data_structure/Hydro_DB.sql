
select * from Hydro.dbo.HilltopWQMtypes 
pivot (
max(Value) for Param in ([data], [Lab Method], [Lab Name])) as piv1


select * from Hydro.dbo.NiwaAquaAtmosTSDataDaily
where Site = 10332 and FeatureMtypeSourceID = 18 and Time > '2010-01-01'

select * from NiwaAquaAtmosTSDataDaily where Site in (10332, 10530) and FeatureMtypeSourceID = 38

select OBJECT_ID('HilltopWQSites', 'U')


select * 
from Hydro.dbo.HilltopWQMtypes
where SiteID = 'SQ00049'
	and CollectionTime = '2015-10-29 08:54:00'
	and Param = 'data'
	and MeasurementType = 'Dissolved Reactive Phosphorus'


select * 
from Hydro.dbo.HilltopWQMtypes
where SiteID = 'SQ00049' and Param = 'data'
order by SiteID, MeasurementType, CollectionTime


select * 
from MetConnect.dbo.RainFallPredictionsGrid
where PredictionDateTime >= '2018-02-19' 
	and PredictionDateTime <= '2018-02-22'
	and MetConnectID = 3

select *
from HilltopWQSites
where SiteID in
(select * from
(select ROW_NUMBER() over (order by SiteID) as row_num, SiteID, MeasurementType, Units, FromDate, ToDate
from HilltopWQSites) as r1
where row_num in (1, 2, 3))

delete
from HilltopWQSites
where exists (select * 
		from (values ('0', 'Air Tempreature'),
			('0', 'Alkalinity to pH 4.5 as CaCO3'),
			('K38/0106', 'Acidity')) as t1
			(SiteID, MeasurementType)
		where t1.SiteID = HilltopWQSites.SiteID
		and t1.MeasurementType = HilltopWQSites.MeasurementType)

(select *
from (values ('0', 'Air Tempreature'),
			('0', 'Alkalinity to pH 4.5 as CaCO3'),
			('K38/0106', 'Acidity')) as t1
			(SiteID, MeasurementType)) as t2


select count(SiteID) from Hydro.dbo.HilltopWQMtypes
select count(SiteID) from Hydro.dbo.HilltopWQSamples
select count(SiteID) from Hydro.dbo.HilltopWQSites

SELECT table_schema, table_name, column_name, data_type, character_maximum_length,
    is_nullable, column_default, numeric_precision, numeric_scale
FROM information_schema.columns
ORDER BY table_schema, table_name, ordinal_position

select *
from HilltopWQSites
where SiteID = 'SQ30212'



