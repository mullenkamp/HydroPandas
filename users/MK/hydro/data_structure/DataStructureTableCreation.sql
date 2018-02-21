BEGIN TRANSACTION

CREATE TABLE Hydro.dbo.DataProviderMaster (
       DataProviderID int identity(1, 1) NOT NULL,
       DataProvider varchar(99) NOT NULL,
       SystemID varchar(99),
       Description varchar(99) NOT NULL,
       PRIMARY KEY (DataProviderID),
)

CREATE TABLE Hydro.dbo.SiteLinkMaster (
	EcanSiteID varchar(29) NOT NULL,
    SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
    DataProviderID int NOT NULL FOREIGN KEY REFERENCES DataProviderMaster(DataProviderID)
    PRIMARY KEY (EcanSiteID)
)

CREATE TABLE Hydro.dbo.SiteMaster (
       SiteID int NOT NULL,
       Status varchar(99),
       Hazards varchar(99),
       DateEstablished datetime,
       DateDecommissioned datetime,
       Owner varchar(299),
       StreetAddress varchar(299),
       Locality varchar(299),	
       NZTMX float not null,
       NZTMY float not null,
       Description varchar(699)
       PRIMARY KEY (SiteID)
)

CREATE TABLE Hydro.dbo.FeatureMaster (
       FeatureID int identity(1, 1) NOT NULL,
       FeatureLongName varchar(79) NOT NULL,
       FeatureShortName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (FeatureID),
       UNIQUE (FeatureLongName)
)

CREATE TABLE Hydro.dbo.MtypeMaster (
       MtypeID int identity(1, 1) NOT NULL,
       MtypeLongName varchar(79) NOT NULL,
       MtypeShortName varchar(79) NOT NULL,
       MtypeGroup varchar(29) NOT NULL,
       Units varchar(89) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (MtypeID),
       UNIQUE (MtypeLongName)
)

CREATE TABLE Hydro.dbo.MSourceMaster (
       MSourceID int identity(1, 1) NOT NULL,
       MSourceLongName varchar(79) NOT NULL,
       MSourceShortName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (MSourceID),
       UNIQUE (MSourceLongName)
)

CREATE TABLE Hydro.dbo.MtypeSecMaster (
       MtypeSecID int identity(1, 1) NOT NULL,
       MtypeSecLongName varchar(79) NOT NULL,
       MtypeSecShortName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (MtypeSecID),
       UNIQUE (MtypeSecLongName)
)
	
CREATE TABLE Hydro.dbo.LoggingMethodMaster (
       LoggingMethodID int identity(1, 1) NOT NULL,
       LoggingMethodName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (LoggingMethodID),
       UNIQUE (LoggingMethodName)
)	

CREATE TABLE Hydro.dbo.TSParamMaster (
       TSParamID int identity(1, 1) NOT NULL,
       TSParamName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (TSParamID),
       UNIQUE (TSParamName)
)
	
CREATE TABLE Hydro.dbo.FeatureAttrMaster (
       FeatureAttrID int identity(1, 1) NOT NULL,
       FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
       FeatureAttrName varchar(99) NOT NULL,
       Description varchar(99) NOT NULL,
       PRIMARY KEY (FeatureAttrID),
       UNIQUE (FeatureID, FeatureAttrName)
)	

CREATE TABLE Hydro.dbo.FeatureMtype (
       FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
       MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
       PRIMARY KEY (FeatureID, MtypeID),
)	

CREATE TABLE Hydro.dbo.SiteFeatureAttr (
	   SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
	   FeatureAttrID int NOT NULL FOREIGN KEY REFERENCES FeatureAttrMaster(FeatureAttrID),
	   Value varchar(299) NOT NULL,
	   PRIMARY KEY (SiteID, FeatureAttrID)
)	

CREATE TABLE Hydro.dbo.SiteFeature (
	   SiteFeatureID int identity(1, 1) NOT NULL,
	   SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
	   SiteFeatureNumber varchar(89) NOT NULL,
	   SiteFeatureName varchar(89) NOT NULL,
	   PRIMARY KEY (SiteFeatureID),
	   UNIQUE (SiteID, FeatureID)
)

CREATE TABLE Hydro.dbo.FeatureMtypeSource (
	   FeatureMtypeSourceID int identity(1, 1) NOT NULL,
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
	   MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
	   MSourceID int NOT NULL FOREIGN KEY REFERENCES MSourceMaster(MSourceID),
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES MtypeSecMaster(MtypeSecID),
	   LoggingMethodID int NOT NULL FOREIGN KEY REFERENCES LoggingMethodMaster(LoggingMethodID),
	   DataProviderID int NOT NULL FOREIGN KEY REFERENCES DataProviderMaster(DataProviderID),
	   PRIMARY KEY (FeatureMtypeSourceID),
	   UNIQUE (FeatureID, MtypeID, MSourceID, QualityStateID),
)

CREATE TABLE Hydro.dbo.TSDataSite (
	   TSDataSiteID int identity(1, 1) NOT NULL,
	   SiteFeatureID int NOT NULL FOREIGN KEY REFERENCES SiteFeature(SiteFeatureID),
	   MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
	   MeasurementSourceID int NOT NULL FOREIGN KEY REFERENCES MSourceMaster(MSourceID),
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES MtypeSecMaster(MtypeSecID),
	   TSGroup varchar(19) NOT NULL,
	   PRIMARY KEY (TSDataSiteID),
	   UNIQUE (SiteFeatureID, MtypeID, MeasurementSourceID, QualityStateID)
)


CREATE TABLE Hydro.dbo.TSDataPrimaryNumeric (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataPrimaryNumericDaily (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATE NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataPrimaryNumericHourly (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (TSDataSiteID, Time)
)


CREATE TABLE Hydro.dbo.TSDataPrimary (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   QualityCode smallint,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataSecondary (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   TSParamID int NOT NULL FOREIGN KEY REFERENCES TSParamMaster(TSParamID),
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   PRIMARY KEY (TSDataSiteID, TSParamID, Time)
)



create view vFeatureMtypeSourceShortNames as
select FeatureMtypeSourceID, FeatureShortName, MtypeShortName, MSourceShortName, MtypeSecShortName, LoggingMethodName, DataProvider, SystemID
from FeatureMtypeSource
inner join FeatureMaster on FeatureMtypeSource.FeatureID = FeatureMaster.FeatureID
INNER join MtypeMaster on FeatureMtypeSource.MtypeID = MtypeMaster.MtypeID
inner join MSourceMaster on FeatureMtypeSource.MSourceID = MSourceMaster.MSourceID
inner join MtypeSecMaster on FeatureMtypeSource.MtypeSecID = MtypeSecMaster.MtypeSecID
inner join LoggingMethodMaster on FeatureMtypeSource.LoggingMethodID = LoggingMethodMaster.LoggingMethodID
inner join DataProviderMaster on DataProviderMaster.DataProviderID = FeatureMtypeSource.DataProviderID

create view vFeatureMtypeSourceLongNames as
select FeatureMtypeSourceID, FeatureLongName, MtypeLongName, MSourceLongName, MtypeSecLongName, LoggingMethodName, DataProvider, SystemID
from FeatureMtypeSource
inner join FeatureMaster on FeatureMtypeSource.FeatureID = FeatureMaster.FeatureID
INNER join MtypeMaster on FeatureMtypeSource.MtypeID = MtypeMaster.MtypeID
inner join MSourceMaster on FeatureMtypeSource.MSourceID = MSourceMaster.MSourceID
inner join MtypeSecMaster on FeatureMtypeSource.MtypeSecID = MtypeSecMaster.MtypeSecID
inner join LoggingMethodMaster on FeatureMtypeSource.LoggingMethodID = LoggingMethodMaster.LoggingMethodID
inner join DataProviderMaster on DataProviderMaster.DataProviderID = FeatureMtypeSource.DataProviderID


CREATE TABLE Hydro.dbo.ExtractionLog (
	   RunTimeStart DATETIME NOT NULL,
	   RunTimeEnd DATETIME,
	   FromTime DATETIME,
	   HydroTable varchar(79) NOT NULL,
	   RunResult varchar(9) NOT NULL,
	   Comment varchar(299),
	   PRIMARY KEY (RunTimeStart, HydroTable)
)

--select distinct Variable from Hydro.dbo.NiwaAquaAtmosTSDataDaily

alter table ExtractionLog
add RunTimeEnd DATETIME;

alter table FeatureMtypeSource
alter column DataProviderID int NOT NULL 

alter table FeatureMtypeSource
add FOREIGN KEY (DataProviderID) REFERENCES DataProviderMaster(DataProviderID)



SELECT
     name, object_id, create_date, modify_date
FROM
     sys.tables

COMMIT






