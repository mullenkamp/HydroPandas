
CREATE TABLE Hydro.dbo.BgaugingLink (
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   BgaugingCode varchar(19) NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID)
)

INSERT INTO Hydro.dbo.BgaugingLink (FeatureMtypeSourceID, BgaugingCode) VALUES
	(8, 'Flow'), (7, 'Stage'), (17, 'Temperature')


create view BgaugingTSData AS
select u.RiverSiteIndex AS Site, BgaugingLink.FeatureMtypeSourceID, 
(u.Date + cast(STUFF(RIGHT('0000' + CAST(isnull(IIF(u.Time > 0, u.Time, 0), 0) AS VARCHAR(4)), 4), 3, 0, ':') as datetime)) as Time, 
u.Value, u.NEMSCode as QualityCode
from Bgauging.dbo.BGauging
full join HydstraQualCodeLink on HydstraQualCodeLink.HydstraQualCode = BGauging.GaugingNotToStandard
unpivot
(Value for Type in (Stage, Flow, Temperature)) as u
inner join BgaugingLink on u.Type = BgaugingLink.BgaugingCode

create view BgaugingTSDataDaily as
SELECT Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM BgaugingTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0)

create view BgaugingTSDataHourly as
SELECT Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM BgaugingTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0)





