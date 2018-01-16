
create view WellsTSData AS
select WELL_NO AS Site, 13 as FeatureMtypeSourceID, DATE_READ as Time, DEPTH_TO_WATER as Value
from Wells.dbo.DTW_READINGS



