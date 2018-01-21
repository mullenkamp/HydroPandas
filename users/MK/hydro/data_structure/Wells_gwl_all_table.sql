
create view WellsTSData AS
select WELL_NO AS Site, 13 as FeatureMtypeSourceID, DATE_READ as Time, DEPTH_TO_WATER as Value
from Wells.dbo.DTW_READINGS

create view WellsTSDataDaily as
SELECT Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value
FROM WellsTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0)

create view WellsTSDataHourly as
SELECT Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value
FROM WellsTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0)




