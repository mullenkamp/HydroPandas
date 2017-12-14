BEGIN TRANSACTION
CREATE TABLE Hydro.dbo.SiteMaster (
       SiteID int identity(1, 1) NOT NULL,
       Status varchar(79),
       Hazards varchar(79),
       Description varchar(99)
       PRIMARY KEY (SiteID)
)
	
CREATE TABLE Hydro.dbo.FeatureMaster (
       FeatureID int identity(1, 1) NOT NULL,
       Feature varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (FeatureID),
       UNIQUE (Feature)
)

CREATE TABLE Hydro.dbo.MtypeMaster (
       MtypeID int identity(1, 1) NOT NULL,
       Mtype varchar(79) NOT NULL,
       MtypeGroup varchar(29) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (MtypeID),
       UNIQUE (Mtype)
)

CREATE TABLE Hydro.dbo.MeasurementSourceMaster (
       MeasurementSourceID int identity(1, 1) NOT NULL,
       MeasurementSource varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (MeasurementSourceID),
       UNIQUE (MeasurementSource)
)

CREATE TABLE Hydro.dbo.QualityStateMaster (
       QualityStateID int identity(1, 1) NOT NULL,
       QualityState varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (QualityStateID),
       UNIQUE (QualityState)
)
	
CREATE TABLE Hydro.dbo.LoggingMethodMaster (
       LoggingMethodID int identity(1, 1) NOT NULL,
       LoggingMethod varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (LoggingMethodID),
       UNIQUE (LoggingMethod)
)	

CREATE TABLE Hydro.dbo.TSParamMaster (
       TSParamID int identity(1, 1) NOT NULL,
       TSParam varchar(79) NOT NULL,
       Description varchar(99) NOT NULL
       PRIMARY KEY (TSParamID),
       UNIQUE (TSParam)
)
	
CREATE TABLE Hydro.dbo.FeatureAttrMaster (
       FeatureAttrID int identity(1, 1) NOT NULL,
       FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
       FeatureAttr varchar(79) NOT NULL,
       Description varchar(99) NOT NULL,
       PRIMARY KEY (FeatureAttrID),
       UNIQUE (FeatureID, FeatureAttr)
)	

CREATE TABLE Hydro.dbo.FeatureMtype (
       FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
       MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
       Units varchar(89) NOT NULL,
       PRIMARY KEY (FeatureID, MtypeID),
)	

CREATE TABLE Hydro.dbo.SiteFeatureAttr (
	   SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
	   FeatureAttrID int NOT NULL FOREIGN KEY REFERENCES FeatureAttrMaster(FeatureAttrID),
	   Value varchar(89) NOT NULL,
	   PRIMARY KEY (SiteID, FeatureAttrID)
)	

CREATE TABLE Hydro.dbo.SiteFeature (
	   SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
	   SiteFeatureNumber varchar(89) NOT NULL,
	   SiteFeatureName varchar(89) NOT NULL,
	   PRIMARY KEY (SiteID, FeatureID)
)

CREATE TABLE Hydro.dbo.FeatureMtypeSource (
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
	   MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
	   MeasurementSourceID int NOT NULL FOREIGN KEY REFERENCES MeasurementSourceMaster(MeasurementSourceID),
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES QualityStateMaster(QualityStateID),
	   DataSource varchar(89) NOT NULL,
	   PRIMARY KEY (FeatureID, MtypeID, MeasurementSourceID, QualityStateID),
)

CREATE TABLE Hydro.dbo.TSDataSite (
	   TSDataSiteID int identity(1, 1) NOT NULL,
	   SiteID int NOT NULL FOREIGN KEY REFERENCES SiteMaster(SiteID),
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES FeatureMaster(FeatureID),
	   MtypeID int NOT NULL FOREIGN KEY REFERENCES MtypeMaster(MtypeID),
	   MeasurementSourceID int NOT NULL FOREIGN KEY REFERENCES MeasurementSourceMaster(MeasurementSourceID),
	   QualityStateID int NOT NULL FOREIGN KEY REFERENCES QualityStateMaster(QualityStateID),
	   LoggingMethodID int NOT NULL FOREIGN KEY REFERENCES LoggingMethodMaster(LoggingMethodID),
	   TSGroup varchar(19) NOT NULL,
	   PRIMARY KEY (TSDataSiteID),
	   UNIQUE (SiteID, FeatureID, MtypeID, MeasurementSourceID, QualityStateID)
)

CREATE TABLE Hydro.dbo.TSDataPrimaryNumeric (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode int,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataPrimaryChar (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   QualityCode int,
	   PRIMARY KEY (TSDataSiteID, Time)
)

CREATE TABLE Hydro.dbo.TSDataSecondary (
	   TSDataSiteID int NOT NULL FOREIGN KEY REFERENCES TSDataSite(TSDataSiteID),
	   TSParamID int NOT NULL FOREIGN KEY REFERENCES TSParamMaster(TSParamID),
	   Time DATETIME NOT NULL,
	   Value varchar(99),
	   PRIMARY KEY (TSDataSiteID, TSParamID, Time)
)

	
COMMIT







