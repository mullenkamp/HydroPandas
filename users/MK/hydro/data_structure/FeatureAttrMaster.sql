/* FeatureAttrMaster data */

select * 
from Bgauging.dbo.RSITES
where SiteNumber > 0

select * 
from Squalarc.dbo.SITES
where SITE_ID != '+'

select WELL_NO, u.FeatureAttrName 
from 
(select WELL_NO, WELL_TYPE, DRILLER, cast(DATE_DRILLED as varchar(99)) as DATE_DRILLED, DRILL_METHOD, DRILL_METHOD_SECONDARY, CASING_MATERIAL, cast(DEPTH as varchar(99)) as Depth, MEASURED_REPORTED, 
cast(Depth_Proposed as varchar(99)) as Depth_Proposed, cast(DIAMETER as varchar(99)) as DIAMETER, REFERENCE_DESCRIPTION, cast(GROUND_RL as varchar(99))as GROUND_RL, cast(REFERENCE_RL as varchar(99)) as REFERENCE_RL, cast(QAR_RL as varchar(99)) as QAR_RL, ALTITUDE_MEAS_REP, AQUIFER_TYPE,
AQUIFER_NAME, MULTI_AQUIFER, PUMP_TYPE, cast(PUMP_DEPTH as varchar(99)) as PUMP_DEPTH, cast(INITIAL_SWL as varchar(99)) as INITIAL_SWL, GEOPHYSICAL, cast(Strata as varchar(99)) as Strata, cast(Isotope as varchar(99)) as Isotope, cast(WaterUse as varchar(99)) as WaterUse, Diviner,
DivinerUsed, cast(DateOffProposed as varchar(99)) as DateOffProposed, GWLevelAccess, GWuseAlternateWell, QComment
from Wells.dbo.WELL_DETAILS) as wells
unpivot
(Value for FeatureAttrName in 
(WELL_TYPE, DRILLER, wells.DATE_DRILLED, DRILL_METHOD, DRILL_METHOD_SECONDARY, CASING_MATERIAL, wells.DEPTH, MEASURED_REPORTED, 
wells.Depth_Proposed, wells.DIAMETER, REFERENCE_DESCRIPTION, wells.GROUND_RL, wells.REFERENCE_RL, wells.QAR_RL, ALTITUDE_MEAS_REP, AQUIFER_TYPE,
AQUIFER_NAME, MULTI_AQUIFER, PUMP_TYPE, wells.PUMP_DEPTH, wells.INITIAL_SWL, GEOPHYSICAL, wells.Strata, wells.Isotope, wells.WaterUse, Diviner,
DivinerUsed, wells.DateOffProposed, GWLevelAccess, GWuseAlternateWell, QComment)) as u           


select *
from Wells.dbo.WELL_DETAILS

select * 
from Wells.INFORMATION_SCHEMA.COLUMNS 
where table_name = 'WELL_DETAILS'








