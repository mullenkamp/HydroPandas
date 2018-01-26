/* TS data summary tables */

create table TSDataNumericDailySumm (
	Site varchar(29) not null FOREIGN KEY REFERENCES SiteLinkMaster(EcanSiteID),
	FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	Min float not null,
	Mean float not null,
	Max float not null,
	Count float not null,
	FromDate date not null,
	ToDate date not null
	primary key (Site, FeatureMtypeSourceID)
	)
	
insert into TSDataNumericDailySumm
select Site, FeatureMtypeSourceID, min(Value) as Min, avg(Value) as Mean, max(Value) as Max, count(Value) as Count, min(Time) as FromDate, max(Time) as ToDate
from vTSDataNumericDaily
group by Site, FeatureMtypeSourceID


select distinct Site
from vTSDataNumericDaily
EXCEPT
select distinct EcanSiteID as Site
from SiteLinkMaster

select *
from vTSDataNumericDaily
where Site='BU25/5066'






















CREATE TABLE Hydro.dbo.BgaugingLink (
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   BgaugingCode varchar(19) NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID)
)

INSERT INTO Hydro.dbo.BgaugingLink (FeatureMtypeSourceID, BgaugingCode) VALUES
	(8, 'Flow'), (7, 'Stage'), (17, 'Temperature')


create view vBgaugingTSData AS
select Site, FeatureMtypeSourceID, Time, avg(Value) as Value, min(QualityCode) as QualityCode
from
(select cast(u.RiverSiteIndex as varchar(19)) AS Site, BgaugingLink.FeatureMtypeSourceID, 
(u.Date + cast(STUFF(RIGHT('0000' + CAST(isnull(IIF(u.Time > 0, u.Time, 0), 0) AS VARCHAR(4)), 4), 3, 0, ':') as datetime)) as Time, 
u.Value, isnull(u.NEMSCode, 200) as QualityCode
from Bgauging.dbo.BGauging
full join HydstraQualCodeLink on HydstraQualCodeLink.HydstraQualCode = BGauging.GaugingNotToStandard
unpivot
(Value for Type in (Stage, Flow, Temperature)) as u
inner join BgaugingLink on u.Type = BgaugingLink.BgaugingCode
where RiverSiteIndex > 0) as bg1
group by Site, FeatureMtypeSourceID, Time

create view vBgaugingTSDataDaily as
SELECT Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM vBgaugingTSData 
GROUP BY Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0)

create view vBgaugingTSDataHourly as
SELECT Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM vBgaugingTSData 
GROUP BY Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0)





