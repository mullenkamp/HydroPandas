BEGIN TRANSACTION

CREATE TABLE Hydro.dbo.DataProviderMaster (
       DataProviderID int identity(1, 1) NOT NULL,
       DataProvider varchar(99) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (DataProviderID),
       UNIQUE (DataProvider)
)

CREATE TABLE Hydro.dbo.SiteMaster (
       SiteID int identity(1, 1) NOT NULL,
       Status varchar(79) NOT NULL,
       Hazards varchar(79),
       Owner varchar(99) NOT NULL,
       DataProviderID int NOT NULL FOREIGN KEY REFERENCES DataProviderMaster(DataProviderID),
       Description varchar(99)
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

CREATE TABLE Hydro.dbo.QualityStateMaster (
       QualityStateID int identity(1, 1) NOT NULL,
       QualityStateLongName varchar(79) NOT NULL,
       QualityStateShortName varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (QualityStateID),
       UNIQUE (QualityStateLongName)
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
       FeatureAttrName varchar(79) NOT NULL,
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
	   Value varchar(89) NOT NULL,
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
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES QualityStateMaster(QualityStateID),
	   LoggingMethodID int NOT NULL FOREIGN KEY REFERENCES LoggingMethodMaster(LoggingMethodID),
	   DataSource varchar(89) NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID),
	   UNIQUE (FeatureID, MtypeID, MSourceID, QualityStateID),
)

CREATE TABLE Hydro.dbo.TSDataSite (
	   TSDataSiteID int identity(1, 1) NOT NULL,
	   SiteFeatureID int NOT NULL FOREIGN KEY REFERENCES SiteFeature(SiteFeatureID),
	   MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
	   MeasurementSourceID int NOT NULL FOREIGN KEY REFERENCES MeasurementSourceMaster(MeasurementSourceID),
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES QualityStateMaster(QualityStateID),
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
	   ModDate DATE,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataSecondary (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   TSParamID int NOT NULL FOREIGN KEY REFERENCES TSParamMaster(TSParamID),
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   PRIMARY KEY (TSDataSiteID, TSParamID, Time)
)

/*
CREATE TABLE Hydro.dbo.TSDataModDate (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   ModDate DATE,
	   PRIMARY KEY (TSDataSiteID, Time)
)
*/

CREATE TABLE Hydro.dbo.HydstraTSDataDaily (
	   Site varchar(19) NOT NULL,
	   HydstraCode smallint NOT NULL,
	   Time DATE NOT NULL,
	   Value float,
	   QualityCode smallint,
	   ModDate DATE NOT NULL,
	   PRIMARY KEY (Site, HydstraCode, Time)
)

CREATE TABLE Hydro.dbo.HydstraTSDataHourly (
	   Site varchar(19) NOT NULL,
	   HydstraCode smallint NOT NULL,
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode smallint,
	   ModDate DATE NOT NULL,
	   PRIMARY KEY (Site, HydstraCode, Time)
)

CREATE TABLE Hydro.dbo.HydrotelTSDataDaily (
	   Site varchar(19) NOT NULL,
	   HydroID varchar(29) NOT NULL,
	   Time DATE NOT NULL,
	   Value float,
	   PRIMARY KEY (Site, HydroID, Time)
)

CREATE TABLE Hydro.dbo.WellsTSData (
	   Site varchar(19) NOT NULL,
	   HydroID varchar(29) NOT NULL,
	   Time DATETIME NOT NULL,
	   Value float,
	   PRIMARY KEY (Site, HydroID, Time)
)

CREATE TABLE Hydro.dbo.LowFlowSiteRestr (
	   site varchar(19) NOT NULL,
	   date date NOT NULL,
	   Waterway varchar(59) NOT NULL,
	   Location varchar(59) NOT NULL,
	   flow_method varchar(29) NOT NULL,
	   days_since_flow_est int,
	   flow numeric(10,3),
	   crc_count int NOT NULL,
	   min_trig numeric(10,3),
	   max_trig numeric(10,3),
	   restr_category varchar(9) NOT NULL,
	   PRIMARY KEY (site, date)
)

create view FeatureMtypeSourceNames as
select FeatureMtypeSourceID, FeatureShortName, MtypeShortName, MSourceShortName, QualityStateShortName, DataSource
from FeatureMtypeSource
inner join FeatureMaster on FeatureMtypeSource.FeatureID = FeatureMaster.FeatureID
INNER join MtypeMaster on FeatureMtypeSource.MtypeID = MtypeMaster.MtypeID
inner join MSourceMaster on FeatureMtypeSource.MSourceID = MSourceMaster.MSourceID
inner join QualityStateMaster on FeatureMtypeSource.QualityStateID = QualityStateMaster.QualityStateID


COMMIT






