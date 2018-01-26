
create view vTSDataNumeric as
select * from Hydro.dbo.vBgaugingTSData
UNION all
select * from Hydro.dbo.vWellsTSData

create view vTSDataNumericHourly as
select * from Hydro.dbo.vBgaugingTSDataHourly
UNION ALL
select * from Hydro.dbo.vWellsTSDataHourly
UNION ALL
select * from Hydro.dbo.HydstraTSDataHourly

create view vTSDataNumericDaily as
select * from Hydro.dbo.vBgaugingTSDataDaily
UNION ALL
select * from Hydro.dbo.vWellsTSDataDaily
UNION ALL
select * from Hydro.dbo.HydstraTSDataDaily
UNION ALL
select * from Hydro.dbo.vTSUsageDaily
--UNION
--select * from Hydro.dbo.NiwaAquaAtmosTSDataDaily

create view TSDataNumericWeekly as
SELECT Site, FeatureMtypeSourceID, DATEADD(week, DATEDIFF(week, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM TSDataNumericDaily GROUP BY Site, FeatureMtypeSourceID, DATEADD(week, DATEDIFF(week, 0, Time)/ 1 * 1, 0)

create view TSDataNumericMonthly as
SELECT Site, FeatureMtypeSourceID, DATEADD(month, DATEDIFF(month, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM TSDataNumericDaily GROUP BY Site, FeatureMtypeSourceID, DATEADD(month, DATEDIFF(month, 0, Time)/ 1 * 1, 0)

--create view TSDataNumericYearly as
--SELECT Site, FeatureMtypeSourceID, DATEADD(year, DATEDIFF(year, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
--FROM TSDataNumericDaily GROUP BY Site, FeatureMtypeSourceID, DATEADD(year, DATEDIFF(year, 0, Time)/ 1 * 1, 0)

--SELECT COUNT(*) from TSDataNumericDaily
--SELECT COUNT(*) from TSDataNumericHourly

create table TSDataNumeric (
	Site varchar(29) not null,
	FeatureMtypeSourceID int not null,
	Time datetime not null,
	Value float,
	QualityCode smallint,
	primary key (Site, FeatureMtypeSourceID, Time)
	)
	
create table TSDataNumericHourly (
	Site varchar(29) not null,
	FeatureMtypeSourceID int not null,
	Time datetime not null,
	Value float,
	QualityCode smallint,
	primary key (Site, FeatureMtypeSourceID, Time)
	)
	
create table TSDataNumericDaily (
	Site varchar(29) not null,
	FeatureMtypeSourceID int not null,
	Time date not null,
	Value float,
	QualityCode smallint,
	primary key (Site, FeatureMtypeSourceID, Time)
	)

insert into TSDataNumeric
select * from vTSDataNumeric

insert into TSDataNumericHourly
select * from vTSDataNumericHourly

insert into TSDataNumericDaily
select * from vTSDataNumericDaily


select * from TSDataNumeric
where Site='70105'




