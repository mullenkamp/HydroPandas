/* Site Tables */

create view vRSites as
select cast(SiteNumber as varchar(29)) collate Latin1_General_CI_AS as Site, Operational collate Latin1_General_CI_AS as Status, cast(NULL as varchar(99)) as Hazards, cast(NULL as datetime) as DateEstablished, cast(NULL as datetime) as DateDecommissioned,
Owner collate Latin1_General_CI_AS as Owner, cast(NULL as varchar(99)) as StreetAddress, cast(NULL as varchar(99)) as Locality, 3 as DataProviderID, cast(NZTMX as float) as NZTMX, cast(NZTMY as float) as NZTMY, LocationDescription collate Latin1_General_CI_AS as Description
from Bgauging.dbo.RSITES
where SiteNumber > 0
and NZTMX is not null 
and NZTMY is not null

create view vSQSites as
select cast(SITE_ID as varchar(29)) collate Latin1_General_CI_AS as Site, cast(NULL as varchar(99)) as Status, cast(NULL as varchar(99)) as Hazards, cast(NULL as datetime) as DateEstablished, cast(NULL as datetime) as DateDecommissioned,
cast(NULL as varchar(99)) as Owner, cast(NULL as varchar(99)) as StreetAddress, cast(NULL as varchar(99)) as Locality, 8 as DataProviderID, cast(NZTMX as float) as NZTMX, cast(NZTMY as float) as NZTMY, LocationDescription collate Latin1_General_CI_AS as Description
from Squalarc.dbo.SITES
where SITE_ID != '+'
and NZTMX is not null 
and NZTMY is not null

create view vWell_details as
select cast(WELL_NO as varchar(29)) as Site, ShortStatus as Status, cast(NULL as varchar(99)) as Hazards, cast(Date_Drilled as datetime) as DateEstablished, cast(Date_Defunct as datetime) as DateDecommissioned,
OWNER as Owner, ROAD_OR_STREET as StreetAddress, LOCALITY as Locality, 4 as DataProviderID, cast(NZTMX as float) as NZTMX, cast(NZTMY as float) as NZTMY, LOCATION as Description
from Wells.dbo.WELL_DETAILS
inner join Wells.dbo.Status_Codes on Status_Codes.Status_Code =  WELL_DETAILS.Well_Status
where NZTMX is not null 
and NZTMY is not null

create view vSiteData as
select * from
(select * from vRSites
UNION ALL
select * from vSQSites
UNION ALL
select * from vWell_details) as u

select cast(row_number() over (order by Site) as int) as SiteID, Site, Status, Hazards, DateEstablished, DateDecommissioned, Owner, StreetAddress, Locality, DataProviderID, NZTMX, NZTMY, Description
into Hydro.dbo.SiteLinkTemp
from vSiteData

insert into SiteMaster
select SiteID, Status, Hazards, DateEstablished, DateDecommissioned, Owner, StreetAddress, Locality, NZTMX, NZTMY, Description
from SiteLinkTemp

insert into Hydro.dbo.SiteLinkMaster
select Site as EcanSiteID, SiteID, DataProviderID
from SiteLinkTemp





--SELECT
--    'vRSites' as TName, col.name, col.collation_name
--FROM 
--    sys.columns col
--WHERE
--    object_id = OBJECT_ID('vRSites')
--UNION
--SELECT
--    'vSQsites' as TName, col.name, col.collation_name
--FROM 
--    sys.columns col
--WHERE
--    object_id = OBJECT_ID('vSQsites')
--UNION
--SELECT
--    'vWell_details' as TName, col.name, col.collation_name
--FROM 
--    sys.columns col
--WHERE
--    object_id = OBJECT_ID('vWell_details')  
--    
    






