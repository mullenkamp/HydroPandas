/* WQ Tables */

create view vD_WQ_Samples as
select [Sample ID] as SampleID, piv.SiteID as SiteName, NULL as SiteName2, cast(CollectionTime as date) as CollectionDate, 
cast(CollectionTime as time) as CollectionTime, Project as ProjectNumber, [Cost Code] as CostCode, Technician as SampledBy, 
[Sample Comment] as SampleComment, [Field Comment] as FieldComment
from 
(select * from Hydro.dbo.HilltopWQSamples
pivot (
	max(Value) for Param in ([Project], [Cost Code], [Technician], [Sample Comment], [Field Comment], [Sample ID])
) as piv1
) as piv
	inner join Hilltop.dbo.Sites on Hilltop.dbo.Sites.SiteName = piv.SiteID

create view vD_WQ_SampleResults as
select SampleID, NULL as SampleType, piv.MeasurementType as MeasurementName, [data] as Result, Units, [Lab Method] as Description, 
NULL as ResultComments, NULL as LabDetectionLimit, [Lab Name] as LabName, NULL as QualityCode
from 
(select * from Hydro.dbo.HilltopWQMtypes 
pivot (
max(Value) for Param in ([data], [Lab Method], [Lab Name])) as piv1
) as piv
	inner join HilltopWQSites on HilltopWQSites.SiteID = piv.SiteID
		and HilltopWQSites.MeasurementType = piv.MeasurementType
	inner join vD_WQ_Samples on vD_WQ_Samples.SiteName = piv.SiteID
		and (cast(vD_WQ_Samples.CollectionDate as datetime) + cast(vD_WQ_Samples.CollectionTime as datetime)) = piv.CollectionTime

select *
into F_WQ_SampleResults
from vD_WQ_SampleResults


create view vD_WQ_SampleResults as
select vD_WQ_Samples.SampleID, NULL as SampleType, piv.MeasurementType as MeasurementName, [data] as Result, Units, [Lab Method] as Description, 
NULL as ResultComments, NULL as LabDetectionLimit, [Lab Name] as LabName, NULL as QualityCode
from 
(select * from Hydro.dbo.HilltopWQMtypes 
pivot (
max(Value) for Param in ([data], [Lab Method], [Lab Name])) as piv1
) as piv
	inner join Hydro.dbo.HilltopWQSites as s1 on s1.SiteID = piv.SiteID
	inner join Hydro.dbo.HilltopWQSites as s2 on s2.MeasurementType = piv.MeasurementType
	inner join Hydro.dbo.vD_WQ_Samples as s3 on s3.SiteName = piv.SiteID
	inner join Hydro.dbo.vD_WQ_Samples as s4 on s4.CollectionDate = cast(piv.CollectionTime as date)



select *
from Hydro.dbo.HilltopWQSamples
	inner join Hilltop.dbo.Sites on Hilltop.dbo.Sites.SiteName = HilltopWQSamples.SiteID
	
	
select * from 
(select * from Hydro.dbo.HilltopWQMtypes 
pivot (
max(Value) for Param in ([data], [Lab Method], [Lab Name])) as piv1
) as piv
	inner join HilltopWQSites on HilltopWQSites.SiteID = piv.SiteID
		and HilltopWQSites.MeasurementType = piv.MeasurementType
	inner join vD_WQ_Samples on vD_WQ_Samples.SiteName = piv.SiteID
		and vD_WQ_Samples.CollectionDate = cast(piv.CollectionTime as date)


select SampleID, NULL as SampleType, piv.MeasurementType as MeasurementName, [data] as Result, Units, [Lab Method] as Description, 
NULL as ResultComments, NULL as LabDetectionLimit, [Lab Name] as LabName, NULL as QualityCode
from 
(select * from Hydro.dbo.HilltopWQMtypes 
pivot (
max(Value) for Param in ([data], [Lab Method], [Lab Name])) as piv1
) as piv
	inner join HilltopWQSites on HilltopWQSites.SiteID = piv.SiteID
		and HilltopWQSites.MeasurementType = piv.MeasurementType
	inner join vD_WQ_Samples on vD_WQ_Samples.SiteName = piv.SiteID
		and (cast(vD_WQ_Samples.CollectionDate as datetime) + cast(vD_WQ_Samples.CollectionTime as datetime)) = piv.CollectionTime

