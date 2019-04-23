
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
	
	
select getdate()


select NZTMX, NZTMY, max(SiteZ) as SiteZ
from
(select round(NZTMX, 0) as NZTMX, round(NZTMY, 0) as NZTMY, round(SiteZ, 3) as SiteZ
from ExternalSite) as m
group by NZTMX, NZTMY


select *
from Hydro.dbo.SiteLink
where ExtSiteID not in 
(select DISTINCT Site
from Hydro.dbo.HydstraTSDataDaily)

select DISTINCT Site
from Hydro.dbo.HydstraTSDataDaily
where Site not in
(select ExtSiteID
from Hydro.dbo.SiteLink)


select SiteNumber, NZTMX, NZTMY, Altitude
FROM Bgauging.dbo.RSITES
where SiteNumber in ('66429', '66423', '69644')

select *
from Hydro.dbo.HydstraTSDataDaily
where Site = '3452533'

update Hydro.dbo.HydstraTSDataDaily
set Site = '3496686'
where Site = '34966867'

update Hydro.dbo.HydstraTSDataDaily
set Site = '3452533'
where Site = '34525337'

update Hydro.dbo.HydstraTSDataHourly
set Site = '3496686'
where Site = '34966867'

update Hydro.dbo.HydstraTSDataHourly
set Site = '3452533'
where Site = '34525337'

alter table Hydro.dbo.HydstraTSDataDaily
add ModDate datetime default getdate();

update Hydro.dbo.HydstraTSDataDaily
set ModDate = getdate();

alter table Hydro.dbo.HydstraTSDataHourly
add ModDate datetime default getdate();

update Hydro.dbo.HydstraTSDataHourly
set ModDate = getdate();

select *
from Hydro.dbo.ExternalSite
where ExtSiteID in ('3452533', '3496686')



select Site, TIDEDA_FLAG, Time, avg(Value) as Value, min(QualityCode) as QualityCode, max(stamp) as stamp
from
(select UPPER(LTRIM(RTRIM(WELL_NO))) AS Site, TIDEDA_FLAG, DATE_READ as Time, DEPTH_TO_WATER as Value, 200 as QualityCode, cast(timestamp as int) as stamp
from Wells.dbo.DTW_READINGS) as wells1
where Value is not null 
	and Value > -990
	and TIDEDA_FLAG in ('N', 'F')
group by Site, TIDEDA_FLAG, Time

DELETE from Hydro.dbo.temp_up_table

merge TSDataNumeric 
using temp_up_table on (TSDataNumeric.ExtSiteID = temp_up_table.ExtSiteID and 
						TSDataNumeric.DatasetTypeID = temp_up_table.DatasetTypeID and  
						TSDataNumeric.DateTime = temp_up_table.DateTime) 
when matched then update set TSDataNumeric.Value = temp_up_table.Value, 
							TSDataNumeric.QualityCode = temp_up_table.QualityCode, 
							TSDataNumeric.ModDate = temp_up_table.ModDate 
WHEN NOT MATCHED BY TARGET THEN INSERT (ExtSiteID, DateTime, DatasetTypeID, Value, QualityCode, ModDate) 
values (temp_up_table.ExtSiteID, temp_up_table.DateTime, temp_up_table.DatasetTypeID, temp_up_table.Value, temp_up_table.QualityCode, temp_up_table.ModDate);


select sites.MetConnectID, sites.SiteString, sites.TidedaID, grid.PredictionDateTime, grid.ReadingDateTime, grid.HourlyRainfall
from MetConnect.dbo.RainFallPredictionSitesGrid as sites
inner join MetConnect.dbo.RainFallPredictionsGrid as grid
on sites.MetConnectID = grid.MetConnectID

select sites.MetConnectID, sites.SiteString, sites.TidedaID, grid.PredictionDateTime, grid.ReadingDateTime, grid.HourlyRainfall
from MetConnect.dbo.RainFallPredictionSitesGrid as sites
inner join MetConnect.dbo.RainFallPredictions as grid
on sites.MetConnectID = grid.MetConnectID
where grid.PredictionDateTime > '2017-12-05'

DELETE from Hydro.dbo.TSDataNumericDaily
DELETE from Hydro.dbo.TSDataNumericHourly
DELETE from Hydro.dbo.SiteLink
DELETE from Hydro.dbo.Site
DELETE from Hydro.dbo.ExternalSite

update Hydro.dbo.ExternalSite
set ModDate = '2018-04-27 00:00:00'
where ModDate is NULL


select max(RunTimeStart) from HydroLog where HydroTable='ExternalSite' and RunResult='pass' and ExtSystem='Hydro'

merge Site using temp_up_table on (Site.ExtSiteID = temp_up_table.ExtSiteID) when matched then update set Site.SiteID = temp_up_table.SiteID;

merge TSDataNumeric using temp_up_table on (TSDataNumeric.ExtSiteID = temp_up_table.ExtSiteID and TSDataNumeric.DatasetTypeID = temp_up_table.DatasetTypeID and TSDataNumeric.DateTime = temp_up_table.DateTime) when matched then update set TSDataNumeric.Value = temp_up_table.Value, TSDataNumeric.QualityCode = temp_up_table.QualityCode, TSDataNumeric.ModDate = temp_up_table.ModDate WHEN NOT MATCHED BY TARGET THEN INSERT (ExtSiteID, DateTime, DatasetTypeID, Value, QualityCode, ModDate) values (temp_up_table.ExtSiteID, temp_up_table.DateTime, temp_up_table.DatasetTypeID, temp_up_table.Value, temp_up_table.QualityCode, temp_up_table.ModDate);

select distinct FeatureMtypeSourceID
from Hydro.dbo.WusTSDataDaily

select *
from Hydro.dbo.WusTSDataDaily
where QualityCode = 150

update WusTSDataDaily
set QualityCode = 200
where QualityCode = 150

ALTER TABLE Hydro.dbo.ExternalSite
ADD SiteID int FOREIGN KEY REFERENCES Site(SiteID);

update es
set SiteID = sk.SiteID
from Hydro.dbo.ExternalSite as es
inner join SiteLink as sk
on es.ExtSiteID = sk.ExtSiteID


ALTER TABLE Hydro.dbo.TSDataNumericDaily
DROP CONSTRAINT FK__TSDataNum__ExtSi__6DF7358C;

ALTER TABLE Hydro.dbo.TSDataNumericDaily
ADD FOREIGN KEY (ExtSiteID) REFERENCES ExternalSite(ExtSiteID);

ALTER TABLE Hydro.dbo.TSDataNumericHourly
DROP CONSTRAINT FK__TSDataNum__ExtSi__683E5C36;

ALTER TABLE Hydro.dbo.TSDataNumericHourly
ADD FOREIGN KEY (ExtSiteID) REFERENCES ExternalSite(ExtSiteID);

select distinct Site
from Hydro.dbo.NiwaAquaAtmosTSDataDaily
where Site not in (select ExtSiteID from ExternalSite)

select *
from Hydro.dbo.TSDataNumericDailySumm
where ExtSiteID in (
	select ExtSiteID
	from Hydro.dbo.TSDataNumericDailySumm
	where DatasetTypeID in (18, 20, 34, 28))
order by ExtSiteID, DatasetTypeID

select *
from Hydro.dbo.TSDataNumericDailySumm
where DatasetTypeID = 34
order by ExtSiteID, DatasetTypeID

delete from Hydro.dbo.WQMeasurements
delete from Hydro.dbo.WQSamples
delete from Hydro.dbo.WQSites

insert into WQSites (ExtSiteID, MeasurementType, Units, FromDate, ToDate)
select SiteID, MeasurementType, Units, FromDate, ToDate from HilltopWQSites

insert into WQSamples (ExtSiteID, CollectionTime, SampleParam, Value)
select SiteID, CollectionTime, Param, Value from HilltopWQSamples

insert into WQMeasurements (ExtSiteID, MeasurementType, CollectionTime, MTypeParam, Value)
select SiteID, MeasurementType, CollectionTime, Param, Value from Hydro.dbo.HilltopWQMtypes

delete from WQMeasurements
where Value = ''

delete from WQSamples
where Value = ''


select * into HydstraArchive.dbo.TSDataNumeric1
from Hydro.dbo.TSDataNumeric


delete from HydstraArchive.dbo.TSDataNumeric1

insert into HydstraArchive.dbo.TSDataNumeric1 WITH(TABLOCK) (ExtSiteID, DatasetTypeID, DateTime, Value, QualityCode, ModDate) 
select ExtSiteID, DatasetTypeID, DateTime, Value, QualityCode, ModDate from Hydro.dbo.TSDataNumeric

select *
from Hydro.dbo.TSDataNumericDaily
where DatasetTypeID = 12 and DateTime > '2018-05-14' 
order by ModDate DESC

delete from Hydro.dbo.HilltopUsageSiteDataLog
where date = '2018-05-24'

select * 
from LowFlowRestrSite
where date = '2018-05-31' and flow_method = 'Telemetered' and days_since_flow_est > 0
order by days_since_flow_est desc

delete from Hydro.dbo.WQTSSample
where Value IS NULL

ALTER TABLE Hydro.dbo.LowFlowRestrSite
ADD FOREIGN KEY (site) REFERENCES ExternalSite(ExtSiteID);

ALTER TABLE Hydro.dbo.LowFlowRestrSite
ALTER COLUMN site varchar(29) not null;

ALTER TABLE Hydro.dbo.LowFlowRestrSite
DROP CONSTRAINT PK_Person;

ALTER TABLE Hydro.dbo.CrcAllo
ALTER COLUMN return_period float;

select *
from ExternalSite
where ExtSiteID in
	(select distinct wap from CrcWapAllo)


SELECT geometry::Point(NZTMY, NZTMX, 2193) as GEOM
FROM Hydro.dbo.ExternalSite

alter table Hydro.dbo.ExternalSite 
add Shape as geometry::Point(NZTMX, NZTMY, 2193);

alter table Hydro.dbo.ExternalSite 
add Shape geometry

update Hydro.dbo.ExternalSite 
set Shape = geometry::Point(NZTMX, NZTMY, 2193);

CREATE SPATIAL INDEX si_ExternalSite_shape
	   ON dbo.ExternalSite(Shape)
	   USING GEOMETRY_AUTO_GRID
	   WITH 
	   ( 
	         BOUNDING_BOX= (xmin=165, ymin=-48, xmax=180, ymax=-34)
	   );

alter table Hydro.dbo.Site 
add Shape geometry

update Hydro.dbo.Site 
set Shape = geometry::Point(NZTMX, NZTMY, 2193);

CREATE SPATIAL INDEX si_Site_shape
	   ON dbo.Site(Shape)
	   USING GEOMETRY_AUTO_GRID
	   WITH 
	   ( 
	         BOUNDING_BOX= (xmin=165, ymin=-48, xmax=180, ymax=-34)
	   );



alter table Hydro.dbo.Site
add Shape as geometry::Point(NZTMX, NZTMY, 2193);

alter table Hydro.dbo.ExternalSite 
add ExtSiteName varchar(299)

alter table Hydro.dbo.ExternalSite 
add
   CatchmentName varchar(99),
   CatchmentNumber int,
   CatchmentGroupName varchar(99),
   CatchmentGroupNumber int,
   SwazName varchar(99),
   SwazGroupName varchar(99),
   SwazSubRegionalName varchar(99),
   GwazName varchar(99),
   CwmsName varchar(99)






select *
from Hydro.dbo.ExternalSite
where CatchmentName like '%ahuriri river%'

select *
from Hydro.dbo.CrcWapAllo
where wap in (
	select ExtSiteID 
	from Hydro.dbo.ExternalSite
	where CatchmentName like '%ahuriri river%'
	)


select *
from Hydro.dbo.CrcAllo
where crc in (
	select distinct crc
	from Hydro.dbo.CrcWapAllo
	where wap in (
		select ExtSiteID 
		from Hydro.dbo.ExternalSite
		where CatchmentName like '%ahuriri river%'
		)
	)



alter table CrcWapAllo
alter column take_type varchar(29) not null

alter table CrcAllo
alter column take_type varchar(29) not null

update CrcAllo
set max_vol = NULL
where max_vol <= 0


alter table Hydro.dbo.ExternalSite 
drop column Shape

alter table Hydro.dbo.Site 
drop column Shape

delete from Hydro.dbo.TSDataNumericDaily
where QualityCode = 100

delete from Hydro.dbo.TSDataNumericHourly
where QualityCode = 100


SELECT        ds.DatasetTypeID, dbo.Feature.Feature, dbo.MeasurementType.MeasurementType, dbo.CollectionType.CollectionType, dbo.DataCode.DataCode, dbo.DataProvider.DataProvider
FROM  Hydro.dbo.DatasetType as ds  INNER JOIN
                         dbo.Feature ON ds.FeatureID = dbo.Feature.FeatureID INNER JOIN
                         dbo.MeasurementType ON ds.MTypeID = dbo.MeasurementType.MTypeID INNER JOIN
                         dbo.CollectionType ON ds.CTypeID = dbo.CollectionType.CTypeID INNER JOIN
                         dbo.DataCode ON ds.DataCodeID = dbo.DataCode.DataCodeID INNER JOIN
                         dbo.DataProvider ON ds.DataProviderID = dbo.DataProvider.DataProviderID
                         
                         
select tsdata.ExtSiteID,
	tsdata.DatasetTypeID,
	min(tsdata.Value) as Min,
	avg(tsdata.Value) as Mean,
	max(tsdata.Value) as Max,
	count(tsdata.Value) as Count,
	min(tsdata.[DateTime]) as FromDate,
	max(tsdata.[DateTime]) as ToDate
from TSDataNumeric as tsdata
inner join
	(select distinct ExtSiteID, DatasetTypeID
	 from TSDataNumeric
	 where ModDate >= '2018-06-27') as set1
on tsdata.ExtSiteID = set1.ExtSiteID
and tsdata.DatasetTypeID = set1.DatasetTypeID
where tsdata.Value is not NULL
group by tsdata.ExtSiteID, tsdata.DatasetTypeID


select SiteID, ex.ExtSiteID, summ.DatasetTypeID, summ.Min, summ.Mean, summ.Max, summ.Count, summ.FromDate, summ.ToDate
from Hydro.dbo.ExternalSite as ex
inner join Hydro.dbo.TSDataNumericDailySumm as summ on summ.ExtSiteID = ex.ExtSiteID

SELECT SiteID, es.ExtSiteID, ds.DatasetTypeID, Feature.Feature, MeasurementType.MeasurementType, 
	CollectionType.CollectionType, DataCode.DataCode, DataProvider.DataProvider, summ.Min, 
	round(summ.Mean, 3), summ.Max, summ.Count, summ.FromDate, summ.ToDate
FROM ExternalSite as es INNER JOIN
	dbo.TSDataNumericDailySumm as summ on summ.ExtSiteID = es.ExtSiteID inner join
	dbo.DatasetType as ds on ds.DatasetTypeID = summ.DatasetTypeID inner join
	dbo.Feature ON ds.FeatureID = dbo.Feature.FeatureID INNER JOIN
	dbo.MeasurementType ON ds.MTypeID = dbo.MeasurementType.MTypeID INNER JOIN
	dbo.CollectionType ON ds.CTypeID = dbo.CollectionType.CTypeID INNER JOIN
	dbo.DataCode ON ds.DataCodeID = dbo.DataCode.DataCodeID INNER JOIN
	dbo.DataProvider ON ds.DataProviderID = dbo.DataProvider.DataProviderID


select distinct hts_file
from Hydro.dbo.HilltopUsageSiteDataLog

delete from Hydro.dbo.HilltopUsageSiteDataLog
where date = '2018-07-02'

delete from Hydro.dbo.HilltopUsageSiteSummLog
where date = '2018-07-02'


alter table Hydro.dbo.DatasetType
add DatasetTypeName varchar(99)

SELECT ds.DatasetTypeID, Feature.Feature, MeasurementType.MeasurementType, 
	CollectionType.CollectionType, DataCode.DataCode, DataProvider.DataProvider
FROM DatasetType as ds INNER JOIN
	Feature ON ds.FeatureID = Feature.FeatureID INNER JOIN
	MeasurementType ON ds.MTypeID = MeasurementType.MTypeID INNER JOIN
	CollectionType ON ds.CTypeID = CollectionType.CTypeID INNER JOIN
	DataCode ON ds.DataCodeID = DataCode.DataCodeID INNER JOIN
	DataProvider ON ds.DataProviderID = DataProvider.DataProviderID
order by ds.DatasetTypeID


update Hydro.dbo.DatasetType
set DatasetType.DatasetTypeName = 
concat(Feature.Feature, ' - ',  MeasurementType.MeasurementType, ' - ',
	CollectionType.CollectionType,' - ', DataCode.DataCode,' - ', DataProvider.DataProvider)
FROM DatasetType as ds INNER JOIN
	Feature ON ds.FeatureID = Feature.FeatureID INNER JOIN
	MeasurementType ON ds.MTypeID = MeasurementType.MTypeID INNER JOIN
	CollectionType ON ds.CTypeID = CollectionType.CTypeID INNER JOIN
	DataCode ON ds.DataCodeID = DataCode.DataCodeID INNER JOIN
	DataProvider ON ds.DataProviderID = DataProvider.DataProviderID
	

select tsdata.ExtSiteID,
	tsdata.DatasetTypeID,
	min(tsdata.Value) as Min,
	avg(tsdata.Value) as Mean,
	max(tsdata.Value) as Max,
	count(tsdata.Value) as Count,
	min(tsdata.[DateTime]) as FromDate,
	max(tsdata.[DateTime]) as ToDate
from TSDataNumericDaily as tsdata
inner join
	(select distinct ExtSiteID, DatasetTypeID
	 from TSDataNumericDaily
	 where ModDate >= '2018-06-01') as set1
on tsdata.ExtSiteID = set1.ExtSiteID
and tsdata.DatasetTypeID = set1.DatasetTypeID
where tsdata.Value is not NULL
group by tsdata.ExtSiteID, tsdata.DatasetTypeID
       
ALTER TABLE Hydro.dbo.TSDataNumericDailySumm
ALTER COLUMN FromDate date

ALTER TABLE Hydro.dbo.TSDataNumericDailySumm
ALTER COLUMN ToDate date

CREATE TABLE SiteFeature (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       FeatureID int not null FOREIGN KEY REFERENCES Feature(FeatureID),
       Parameter varchar(99) not null,
       Value varchar(299) not null,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, FeatureID, Parameter)
)

select DatasetTypeID, Feature, MeasurementType, CollectionType, DataCode, DataProvider
from DatasetType as ds
inner join Feature on ds.FeatureID = Feature.FeatureID
INNER join MeasurementType on ds.MTypeID = MeasurementType.MTypeID
inner join CollectionType on ds.CTypeID = CollectionType.CTypeID
inner join DataCode on ds.DataCodeID = DataCode.DataCodeID
inner join DataProvider on ds.DataProviderID = DataProvider.DataProviderID
where Feature = 'atmosphere' and MeasurementType = 'precipitation' and CollectionType = 'recorder' and DataCode = 'raw' and DataProvider = 'niwa'

delete from Hydro.dbo.TSDataNumericDaily
where DatasetTypeID in (10, 424, 1, 3, 2, 14)

delete from Hydro.dbo.TSDataNumericHourly
where DatasetTypeID in (10, 424, 1, 3, 2, 14)

SELECT CAST(GETDATE() + 1 AS DATE)

select DatasetTypeID, Feature, MeasurementType, CollectionType, DataCode, DataProvider
from DatasetType as ds
inner join Feature on ds.FeatureID = Feature.FeatureID
INNER join MeasurementType on ds.MTypeID = MeasurementType.MTypeID
inner join CollectionType on ds.CTypeID = CollectionType.CTypeID
inner join DataCode on ds.DataCodeID = DataCode.DataCodeID
inner join DataProvider on ds.DataProviderID = DataProvider.DataProviderID
where DatasetTypeName is null

select distinct crc_status
from CrcAllo

select count(ExtSiteID)
from Hydro.dbo.TSDataNumericDaily
where DatasetTypeID in (9, 12) and Value is null and [DateTime] > '2018-09-10'

select *
from Hydro.dbo.TSDataNumericDaily
where DatasetTypeID in (9, 12)
order by [DateTime] DESC

select *
from hydro.dbo.HydroLog
where extsystem = 'hilltop usage' and runresult = 'pass'
order by RunTimeStart DESC

select *
from Hydro.dbo.TSDataNumericDaily
where DatasetTypeID in (9, 12) and Value is null and [DateTime] > '2018-08-01'
order by [DateTime] DESC

select *
from Hydro.dbo.TSDataNumericDaily
where ExtSiteID = 'BU25/5051' and [DateTime] > '2018-08-01'
order by [DateTime] DESC

select [date], COUNT(site) as number_of_sites
from Hydro.dbo.LowFlowRestrSite
where site_type = 'LowFlow' and flow_method in ('Correlated from Telem', 'Gauged', 'Manually Calculated', 'Visually Gauged')
group by [date]
order by [date] DESC

select site
from Hydro.dbo.LowFlowRestrSite
where [date] = '2018-09-11'

select site
from Hydro.dbo.LowFlowRestrSite
where [date] = '2018-09-12'


select distinct flow_method
from Hydro.dbo.LowFlowRestrSite

ALTER TABLE Hydro.dbo.LowFlowRestrSite
ALTER COLUMN restr_category varchar(19)

ALTER TABLE Hydro.dbo.LowFlowRestrSiteBand
add op_flag varchar(9)

select distinct ExtSiteID
from Hydro.dbo.TSDataNumericDailySumm
where DatasetTypeID < 10 or DatasetTypeID > 1000 and ExtSiteID > 200000 and ExtSiteID < 1000000

ALTER TABLE Hydro.dbo.ExternalSite
add SourceType varchar(29)

select [DateTime], DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0) AS date
from Hydro.dbo.TSDataNumericHourly
where ExtSiteID = '69607' and DatasetTypeID = 5

select *
from Hydro.dbo.TSDataNumericHourly
where ExtSiteID = '69607' and DatasetTypeID = 5

--update Hydro.dbo.TSDataNumericDaily
--set [DateTime] = cast(DATEADD(day, DATEDIFF(day, 0, [DateTime]) - 1, 0) AS date)
--where DatasetTypeID in (9, 12)

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID in (1, 2, 3)
--
--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID in (7, 8, 13, 17, 1636, 1637, 1638, 4557, 4558, 4559, 4569, 4570, 4571)

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID = 4

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID = 5

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID = 6

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID in (10, 11, 12, 424)

--update Hydro.dbo.TSDataNumericHourly WITH(TABLOCK)
--set [DateTime] = DATEADD(hour, DATEDIFF(hour, 0, [DateTime]) + 1, 0)
--WHERE DatasetTypeID in (6, 14, 15, 16)

select *
from hydro.dbo.CrcAllo
where crc = 'CRC183510'

select count(distinct crc)
from Hydro.dbo.CrcAllo
where crc_status like '%issue%' and crc in 
	(select distinct crc
	from Hydro.dbo.CrcWapAllo
	where wap in 
		(select distinct ExtSiteID
		from Hydro.dbo.TSDataNumericDailySumm
		where datasettypeid in (9, 12)))

select count(distinct crc)
from Hydro.dbo.CrcAllo
where crc_status like '%issue%'

select count(distinct ExtSiteID)
	from Hydro.dbo.TSDataNumericDailySumm
	where datasettypeid in (9, 12) AND
	

select count(distinct wap)
from Hydro.dbo.CrcWapAllo
where crc in (
	select distinct crc
	from Hydro.dbo.CrcAllo
	where crc_status like '%issue%')
	and
	wap in (select distinct ExtSiteID
	from Hydro.dbo.TSDataNumericDailySumm
	where datasettypeid in (9, 12))


select *
from Hydro.dbo.TSDataNumericDailySumm
where DatasetTypeID = 24

select *
from Hydro.dbo.TSDataNumericDailySumm
where ExtSiteID in (
	select ExtSiteID
	from Hydro.dbo.ExternalSite
	where CwmsName = 'Orari-Temuka-Opihi-Pareora')
	and DatasetTypeID in (24, 25, 38)

alter table Hydro.dbo.LowFlowRestrSite
add mod_date datetime not null default getdate()

alter table Hydro.dbo.LowFlowRestrSiteBand
add mod_date datetime not null default getdate()

alter table Hydro.dbo.LowFlowRestrSiteBandCrc
add mod_date datetime not null default getdate()

select distinct restr_category
from LowFlowRestrSite

select distinct Owner
from ExternalSite

update ExternalSite
set SiteID = NULL

CREATE TABLE Site (
       SiteID int NOT NULL,
       NZTMX int not null,
       NZTMY int not null,
       Altitude float,
       PRIMARY KEY (SiteID)
)

create view vDatasetTypeNamesAll as
select DatasetTypeID, Feature, MeasurementType, CollectionType, DataCode, DataProvider
from DatasetType
inner join Feature on DatasetType.FeatureID = Feature.FeatureID
INNER join MeasurementType on DatasetType.MTypeID = MeasurementType.MTypeID
inner join CollectionType on DatasetType.CTypeID = CollectionType.CTypeID
inner join DataCode on DatasetType.DataCodeID = DataCode.DataCodeID
inner join DataProvider on DataProvider.DataProviderID = DatasetType.DataProviderID

DELETE
from TSDataNumeric
where datasettypeid in (
7,
8,
13,
17,
1636,
1637,
1638,
4557,
4558,
4559,
4563,
4564,
4565,
4569,
4570,
4571)

DELETE
from TSDataNumericDaily
where datasettypeid in (
7,
8,
13,
17,
1636,
1637,
1638,
4557,
4558,
4559,
4563,
4564,
4565,
4569,
4570,
4571)

DELETE
from TSDataNumericHourly
where datasettypeid in (
7,
8,
13,
17,
1636,
1637,
1638,
4557,
4558,
4559,
4563,
4564,
4565,
4569,
4570,
4571)


DELETE
from TSDataNumericSumm

DELETE
from TSDataNumericHourlySumm

DELETE
from TSDataNumericDailySumm

DELETE
from TSDataNumeric
where DatasetTypeID = 13

DELETE
from TSDataNumericHourly
where DatasetTypeID = 13

DELETE
from TSDataNumericDaily
where DatasetTypeID = 13


SELECT ExtSiteID, DatasetTypeID, Median = AVG(1.0 * Value)
FROM
(
    SELECT s.ExtSiteID, s.DatasetTypeID, s.Value,  rn = ROW_NUMBER() OVER 
      (PARTITION BY s.ExtSiteID, s.DatasetTypeID ORDER BY s.Value), c.c
    FROM dbo.TSDataNumeric AS s
    INNER JOIN 
    (
      SELECT ExtSiteID, DatasetTypeID, c = COUNT(*) 
      FROM dbo.TSDataNumeric
      GROUP BY ExtSiteID, DatasetTypeID
    ) AS c
    ON s.ExtSiteID = c.ExtSiteID and 
    s.DatasetTypeID = c.DatasetTypeID
    where ModDate >= '2019-01-01' and 
      	Value is not NULL
) AS x
WHERE rn IN ((c + 1)/2, (c + 2)/2)
GROUP BY ExtSiteID, DatasetTypeID;

SELECT ExtSiteID, DatasetTypeID, Median = AVG(1.0 * Value)
FROM
(
    SELECT s.ExtSiteID, s.DatasetTypeID, s.Value,  rn = ROW_NUMBER() OVER 
      (PARTITION BY s.ExtSiteID, s.DatasetTypeID ORDER BY s.Value), c.c
    FROM dbo.TSDataNumeric AS s
    INNER JOIN 
    (
      SELECT ExtSiteID, DatasetTypeID, c = COUNT(*) 
      FROM dbo.TSDataNumeric
      GROUP BY ExtSiteID, DatasetTypeID
    ) AS c
    ON s.ExtSiteID = c.ExtSiteID and 
    s.DatasetTypeID = c.DatasetTypeID
) AS x
WHERE rn IN ((c + 1)/2, (c + 2)/2)
GROUP BY ExtSiteID, DatasetTypeID;


select tsdata.ExtSiteID,
	tsdata.DatasetTypeID,
	min(tsdata.Value) as Min,
	avg(tsdata.Value) as Mean,
	max(tsdata.Value) as Max,
	count(tsdata.Value) as Count,
	min(tsdata.[DateTime]) as FromDate,
	max(tsdata.[DateTime]) as ToDate
into #summ
from TSDataNumeric as tsdata
inner join
	(select distinct ExtSiteID, DatasetTypeID
	 from TSDataNumeric
	 where ModDate >= '1900-01-01') as set1
on tsdata.ExtSiteID = set1.ExtSiteID
and tsdata.DatasetTypeID = set1.DatasetTypeID
where tsdata.Value is not NULL
group by tsdata.ExtSiteID, tsdata.DatasetTypeID


select #summ.[ExtSiteID], #summ.[DatasetTypeID], #summ.[Min], #med.[Median], #summ.[Mean], 
	#summ.[Max], #summ.[Count], #summ.[FromDate], #summ.[ToDate]
from #summ
inner join #med
	on #summ.ExtSiteID = #med.ExtSiteID
	and #summ.DatasetTypeID = #med.DatasetTypeID

drop table #summ
drop table #med
	


CREATE TABLE TSDataNumericSumm (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
	     Min float not null,
       	 Median float not null,
	     Mean float not null,
	     Max float not null,
	     Count float not null,
	     FromDate datetime not null,
	     ToDate datetime not null,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID)
)

CREATE TABLE TSDataNumericHourlySumm (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
	     Min float not null,
         Median float not null,
	     Mean float not null,
	     Max float not null,
	     Count float not null,
	     FromDate datetime not null,
	     ToDate datetime not null,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID)
)

CREATE TABLE TSDataNumericDailySumm (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
	     Min float no- null,-
         Median float not null,
	     Mean float not null,
	     Max float not null,
	     Count float not null,
	     FromDate date not null,
	     ToDate date not null,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID)
)


SELECT ExtSiteID, DatasetTypeID, Median = AVG(1.0 * Value)
FROM
(
    SELECT s.ExtSiteID, s.DatasetTypeID, s.Value,  rn = ROW_NUMBER() OVER
      (PARTITION BY s.ExtSiteID, s.DatasetTypeID ORDER BY s.Value), c.c
    FROM TSDataNumericDaily AS s
    INNER JOIN
    (
      SELECT ExtSiteID, DatasetTypeID, c = COUNT(*)
      FROM TSDataNumericDaily GROUP BY ExtSiteID, DatasetTypeID
    ) AS c
    ON s.ExtSiteID = c.ExtSiteID and
    s.DatasetTypeID = c.DatasetTypeID
    where s.Value is not NULL
    and s.ExtSiteID in (SELECT distinct ExtSiteID
			FROM TSDataNumericDaily
			where ModDate >= '2019-01-24')
) AS x
WHERE rn IN ((c + 1)/2, (c + 2)/2)
GROUP BY ExtSiteID, DatasetTypeID


SELECT distinct ExtSiteID
FROM TSDataNumericDaily
where ModDate >= '2019-01-24' and
   Value is not NULL

select tsdata.ExtSiteID,
	tsdata.DatasetTypeID,
	min(tsdata.Value) as Min,
	avg(tsdata.Value) as Mean,
	max(tsdata.Value) as Max,
	count(tsdata.Value) as Count,
	min(tsdata.[DateTime]) as FromDate,
	max(tsdata.[DateTime]) as ToDate
from TSDataNumericDaily as tsdata
inner join
	(select distinct ExtSiteID, DatasetTypeID
	 from TSDataNumericDaily
	 where ModDate >= '2019-01-24') as set1
on tsdata.ExtSiteID = set1.ExtSiteID
and tsdata.DatasetTypeID = set1.DatasetTypeID
where tsdata.Value is not NULL
group by tsdata.ExtSiteID, tsdata.DatasetTypeID

select tsdata.ExtSiteID,
	tsdata.DatasetTypeID,
	min(tsdata.Value) as Min,
	avg(tsdata.Value) as Mean,
	max(tsdata.Value) as Max,
	count(tsdata.Value) as Count,
	min(tsdata.[DateTime]) as FromDate,
	max(tsdata.[DateTime]) as ToDate
from TSDataNumericDaily as tsdata
where tsdata.Value is not NULL AND
	ExtSiteID in (SELECT distinct ExtSiteID
		FROM TSDataNumericDaily
		where ModDate >= '2019-01-24')
group by tsdata.ExtSiteID, tsdata.DatasetTypeID


SELECT	d.ExtSiteID, d.DatasetTypeID, w.Median
FROM
(
  SELECT ExtSiteID, DatasetTypeID, COUNT(*) AS y
  FROM TSDataNumericDaily
  GROUP BY ExtSiteID, DatasetTypeID
) AS d
CROSS APPLY
(
  SELECT AVG(0E + Value)
  FROM
  (
    SELECT z.Value
     FROM TSDataNumericDaily AS z
     WHERE z.ExtSiteID = d.ExtSiteID AND
     	z.DatasetTypeID = d.DatasetTypeID
     ORDER BY z.Value
     OFFSET (d.y - 1) / 2 ROWS
     FETCH NEXT 2 - d.y % 2 ROWS ONLY
  ) AS f
) AS w(Median);


SELECT ExtSiteID, DatasetTypeID,
    PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY Value)
        OVER (PARTITION BY ExtSiteID, DatasetTypeID) AS Median
FROM TSDataNumericDaily
where Value is not NULL
    and ExtSiteID in (SELECT distinct ExtSiteID
			FROM TSDataNumericDaily
			where ModDate >= '2019-01-24')
ORDER BY ExtSiteID, DatasetTypeID

SELECT ExtSiteID, DatasetTypeID, Median = MAX(Median)
FROM
(
   SELECT ExtSiteID, DatasetTypeID, Median = PERCENTILE_CONT(0.5) WITHIN GROUP 
     (ORDER BY Value) OVER (PARTITION BY ExtSiteID, DatasetTypeID)
   FROM TSDataNumericDaily
   where ExtSiteID in (SELECT distinct ExtSiteID
			FROM TSDataNumericDaily
			where ModDate >= '2019-01-24')
) 
AS x
GROUP BY ExtSiteID, DatasetTypeID;


SELECT ExtSiteID, DatasetTypeID, Median = AVG(1.0 * Value)
FROM
(
    SELECT s.ExtSiteID, s.DatasetTypeID, s.Value,  rn = ROW_NUMBER() OVER
      (PARTITION BY s.ExtSiteID, s.DatasetTypeID ORDER BY s.Value), c.c
    FROM TSDataNumericDaily AS s
    INNER JOIN
    (
      SELECT ExtSiteID, DatasetTypeID, c = COUNT(*)
      FROM TSDataNumericDaily GROUP BY ExtSiteID, DatasetTypeID
    ) AS c
    ON s.ExtSiteID = c.ExtSiteID and
    s.DatasetTypeID = c.DatasetTypeID
    where s.Value is not NULL and
        s.ExtSiteID in (SELECT distinct ExtSiteID
			FROM TSDataNumericDaily
			where ModDate >= '2019-01-24')
) AS x
WHERE rn IN ((c + 1)/2, (c + 2)/2)
GROUP BY ExtSiteID, DatasetTypeID

DELETE
from TSDataNumericDaily
where DatasetTypeID in (19,21,23,25,26,27,29,31,33)

DELETE
from TSDataNumericDailySumm
where DatasetTypeID in (19,21,23,25,26,27,29,31,33)

select *
from TSdatanumericdaily
where ExtSiteID = '71136' and 
	datasettypeid = 5


SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
AND TABLE_NAME = 'ExternalSite' AND TABLE_SCHEMA = 'Schema'

SELECT * FROM sys.objects
WHERE type = 'PK' 
AND  parent_object_id = OBJECT_ID ('ExternalSite')

SELECT ORDINAL_POSITION AS [index], COLUMN_NAME AS name FROM Hydro.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'CrcAllo' AND CONSTRAINT_NAME LIKE 'PK%'
order by [index]


SELECT * FROM Hydro.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'consents_gw_envts_2018_02_23' AND CONSTRAINT_NAME LIKE '%PK%'

select*
from Hydro.dbo.CrcAllo
where (crc, take_type, allo_block) in (('CRC000002', 'Take Surface Water', 'A'))


DELETE
from USM.dbo.SiteAttribute

DELETE
from USM.dbo.Site

DELETE
from USM.dbo.SiteMaster

DELETE
from USM.dbo.SyncRunDate

ALTER TABLE Hydro.dbo.LowFlowRestrSite
ADD PRIMARY KEY (site, date);

ALTER TABLE Hydro.dbo.LowFlowRestrSite
DROP CONSTRAINT LowFlowRestrSite_PK

select *
from Hydro.dbo.CrcWapAllo
where crc = 'CRC180837'

select *
from Hydro.dbo.CrcAllo
where crc = 'CRC180837'

select sum(Value)
from Hydro.dbo.TSDataNumericDaily
where ExtSiteID = 'K36/0912' and 
[DateTime] >= '2017-07-01'

select ExtSiteID, count(ExtSiteID) as [count]
from Hydro.dbo.TSDataNumericDailySumm
where DatasetTypeID in (9, 12)
group by ExtSiteID
order by [count] desc

select *
from Hydro.dbo.TSDataNumericDaily
where ExtSiteID = 'J40/0719'

SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CrcWapAllo' AND TABLE_SCHEMA='dbo'

select *
from Hydro.dbo.TSDataNumericDailySumm
where ExtSiteID in (select ExtSiteID from Hydro.dbo.ExternalSite where extSiteName like 'waikuku stream%')

select ExtSiteID, [DateTime], Value
from Hydro.dbo.TSDataNumericDaily
where ExtSiteID in ('66417', '66409', '389') AND
	DatasetTypeID = 8

	
--DELETE
--from hydro.dbo.HilltopUsageSiteDataLog
--where hts_file like '%broken%'
--	
--DELETE
--from hydro.dbo.HilltopUsageSiteDataLog
--where date = '2019-03-04'
	
select distinct ExtSiteID, DatasetTypeID
from hydro.dbo.TSDataNumericDaily
	
select distinct SwazName
from hydro.dbo.ExternalSite
where SwazGroupName like 'ashburton%'

alter table Hydro.dbo.LowFlowRestrSite
add db_read_log varchar(150)


CREATE TABLE TSDataNumericMonthly (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DateTime date not null,
       Value float,
       QualityCode int FOREIGN KEY REFERENCES QualityCode(QualityCode) default 200,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE TSDataNumericMonthlySumm (
       ExtSiteID varchar(29) NOT NULL FOREIGN KEY REFERENCES ExternalSite(ExtSiteID),
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
	     Min float not null,
       Median float not null,
	     Mean float not null,
	     Max float not null,
	     Count float not null,
	     FromDate date not null,
	     ToDate date not null,
       ModDate datetime not null default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID)
)
	