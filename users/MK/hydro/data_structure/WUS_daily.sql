
--create view SWAPs as
--select DISTINCT WAP as swap
--from DataWarehouse.dbo.D_ACC_Act_Water_TakeWaterWAPAlloc
--where Activity='Take Surface Water' and WAP!='Migration: Not Classified'
--
--
--create view WUSTSDataDaily as
--select UsageSite AS Site, TimestampTo as Time, Usage as Value, DataQuality as QualityCode
--from WUS.dbo.WUS_Fact_WaterUsageDaily
--inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID


create view WUSTSDataSwapDaily as
select UsageSite AS Site, 9 as FeatureMtypeSourceID, TimestampTo as Time, Usage as Value, DataQuality as QualityCode
from WUS.dbo.WUS_Fact_WaterUsageDaily
inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID
where UsageSite in 
(select swap from SWAPs)

create view WUSTSDataWellDaily as
select UsageSite AS Site, 12 as FeatureMtypeSourceID, TimestampTo as Time, Usage as Value, DataQuality as QualityCode
from WUS.dbo.WUS_Fact_WaterUsageDaily
inner join WUS.dbo.WUS_Fact_WaterUsageHeader on WUS_Fact_WaterUsageDaily.FactWaterUsageHeaderID = WUS_Fact_WaterUsageHeader.FactWaterUsageHeaderID
where not UsageSite in 
(select swap from SWAPs)

create view WUSTSDataDaily as
select * from WUSTSDataWellDaily
UNION
select * from WUSTSDataSwapDaily






