
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

select Site, HType, Time, count(*)
from
(select cast(u.STN as VARCHAR(19)) AS Site, cast(HType as VARCHAR(19)) as HType,
(u.MEAS_DATE + cast(STUFF(RIGHT('0000' + CAST(isnull(IIF(cast(u.END_TIME as int) > 0, cast(u.END_TIME as int), 0), 0) AS VARCHAR(4)), 4), 3, 0, ':') as datetime)) as Time, 
u.Value, QUALITY as QualityCode, (u.DATEMOD + cast(STUFF(RIGHT('0000' + CAST(isnull(IIF(u.TIMEMOD > 0, u.TIMEMOD, 0), 0) AS VARCHAR(4)), 4), 3, 0, ':') as datetime)) as ModTime
from Hydstra.dbo.GAUGINGS
unpivot
(Value for HType in (M_GH, FLOW, TEMP)) as u
) as bg1
	group by Site, HType, Time
	having count(*) >1


CREATE TABLE HydstraArchive.dbo.FeatureMtype (
       FeatureID int NOT NULL,
       MtypeID int NOT NULL,
       PRIMARY KEY (FeatureID, MtypeID),
)	

CREATE TABLE HydstraArchive.dbo.TSDataSecondary (
	   TSDataSiteID int NOT NULL,
	   TSParamID int NOT NULL,
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   PRIMARY KEY (TSDataSiteID, TSParamID, Time),
	   FOREIGN KEY (TSDataSiteID, TSParamID) REFERENCES FeatureMtype (FeatureID, MtypeID)
)
	
	
	
	