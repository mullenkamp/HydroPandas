
--CREATE TABLE Hydro.dbo.WusLink (
--	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
--	   WusCode varchar(3) NOT NULL,
--	   PRIMARY KEY (WusCode)
--)
--
--INSERT INTO Hydro.dbo.WusLink (FeatureMtypeSourceID, WusCode) VALUES
--	(12, 'BO'), (12, 'CL'), (12, 'BA'), (12, 'BG'), (12, 'SP'), (12, 'GA'), (12, 'WA'), (9, 'SA'), (9, 'SG')

--CREATE TABLE Hydro.dbo.WusLink (
--	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
--	   WusCode varchar(39) NOT NULL,
--	   PRIMARY KEY (WusCode)
--)
--
--INSERT INTO Hydro.dbo.WusLink (FeatureMtypeSourceID, WusCode) VALUES
--	(12, 'Take Groundwater'), (9, 'Take Surface Water'), (9, 'Divert Surface Water')


--create view WUSTSDataDaily as
--select UsageSite AS Site, TimestampTo as Time, Usage as Value, DataQuality as QualityCode
--from WUS.dbo.WUS_Fact_WaterUsageDaily
--inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID

CREATE TABLE Hydro.dbo.WusTSDataDaily (
	   Site varchar(19) NOT NULL,
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   Time DATE NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (Site, FeatureMtypeSourceID, Time)
)

--insert into Hydro.dbo.WusTSDataDaily
--select UsageSite AS Site, FeatureMtypeSourceID, TimestampTo as Time, AVG(Usage) as Value, MAX(DataQuality) as QualityCode
--from WUS.dbo.WUS_Fact_WaterUsageDaily
--inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID
--inner join Wells.dbo.WELL_DETAILS on WUS_Fact_WaterUsageHeader.UsageSite = WELL_DETAILS.WELL_NO
--inner join Hydro.dbo.WusLink on WELL_DETAILS.WELL_TYPE = WusLink.WusCode
--group by UsageSite, FeatureMtypeSourceID, TimestampTo

insert into Hydro.dbo.WusTSDataDaily
select UsageSite AS Site, FeatureMtypeSourceID, TimestampTo as Time, AVG(Usage) as Value, MAX(DataQuality) as QualityCode
from WUS.dbo.WUS_Fact_WaterUsageDaily
inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID
inner join (select WAP, max(Activity) as WusCode from DataWarehouse.dbo.D_ACC_Act_Water_TakeWaterWAPAlloc where WAP!='Migration: Not Classified'
group by WAP) as WapCode on WUS_Fact_WaterUsageHeader.UsageSite = WapCode.WAP
inner join Hydro.dbo.WusLink on WapCode.WusCode = WusLink.WusCode
group by UsageSite, FeatureMtypeSourceID, TimestampTo
order by Site, FeatureMtypeSourceID, Time



