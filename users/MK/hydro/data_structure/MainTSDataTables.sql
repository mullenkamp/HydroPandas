
create view TSDataNumeric as
select * from Hydro.dbo.BgaugingTSData
UNION
select * from Hydro.dbo.WellsTSData

create view TSDataNumericDaily as
select * from Hydro.dbo.BgaugingTSDataDaily
UNION
select * from Hydro.dbo.WellsTSDataDaily
UNION
select * from Hydro.dbo.HydstraTSDataDaily






