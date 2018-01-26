
create view vWellsTSData AS
select Site, FeatureMtypeSourceID, Time, avg(Value) as Value, min(QualityCode) as QualityCode
from
(select UPPER(LTRIM(RTRIM(WELL_NO))) AS Site, 13 as FeatureMtypeSourceID, DATE_READ as Time, DEPTH_TO_WATER as Value, 200 as QualityCode
from Wells.dbo.DTW_READINGS) as wells1
where Value is not null and Value > -990
group by Site, FeatureMtypeSourceID, Time

create view vWellsTSDataDaily as
SELECT Site, FeatureMtypeSourceID, cast(DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0) as date) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM vWellsTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(day, DATEDIFF(day, 0, Time)/ 1 * 1, 0);

create view vWellsTSDataHourly as
SELECT Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0) AS Time, round(avg(Value), 3) as Value, min(QualityCode) as QualityCode
FROM vWellsTSData GROUP BY Site, FeatureMtypeSourceID, DATEADD(hour, DATEDIFF(hour, 0, Time)/ 1 * 1, 0);




