/* Dataset related base tables */

CREATE TABLE Feature (
       FeatureID int identity(1, 1) NOT NULL,
       FeatureLongName varchar(79) NOT NULL,
       FeatureShortName varchar(79) NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (FeatureID),
       UNIQUE (FeatureLongName)
)

CREATE TABLE LoggingMethod (
       LoggingMethodID int identity(1, 1) NOT NULL,
       LoggingMethodName varchar(79) NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (LoggingMethodID),
       UNIQUE (LoggingMethodName)
)	

CREATE TABLE MeasurementType (
       MTypeID int identity(1, 1) NOT NULL,
       MTypeLongName varchar(79) NOT NULL,
       MTypeShortName varchar(79) NOT NULL,
       MTypeGroup varchar(29) NOT NULL,
       Units varchar(89) NOT NULL,
       LoggingMethodID int not null FOREIGN KEY REFERENCES LoggingMethod(LoggingMethodID),
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (MTypeID),
       UNIQUE (MTypeLongName)
)

CREATE TABLE CollectionType (
       CTypeID int identity(1, 1) NOT NULL,
       CTypeLongName varchar(79) NOT NULL,
       CTypeShortName varchar(79) NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (CTypeID),
       UNIQUE (CTypeLongName)
)

CREATE TABLE DataCode (
       DataCodeID int identity(1, 1) NOT NULL,
       DataCode varchar(79) NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (DataCodeID),
       UNIQUE (DataCode)
)

CREATE TABLE DataProvider (
       DataProviderID int identity(1, 1) NOT NULL,
       DataProviderName varchar(79) NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (DataProviderID),
       UNIQUE (DataProviderName)
)

CREATE TABLE QualityCode (
       QualityCode int NOT NULL,
       Description varchar(99),
       ModDate datetime default getdate(),
       PRIMARY KEY (QualityCode)
)

CREATE TABLE DatasetType (
	   DatasetTypeID int identity(1, 1) NOT NULL,
	   FeatureID int NOT NULL FOREIGN KEY REFERENCES Feature(FeatureID),
	   MTypeID int NOT NULL FOREIGN KEY REFERENCES MeasurementType(MTypeID),
	   CTypeID int NOT NULL FOREIGN KEY REFERENCES CollectionType(CTypeID),
	   DataCodeID int NOT NULL FOREIGN KEY REFERENCES DataCode(DataCodeID),
	   DataProviderID int NOT NULL FOREIGN KEY REFERENCES DataProvider(DataProviderID),
	   PRIMARY KEY (DatasetTypeID),
	   UNIQUE (FeatureID, MtypeID, CTypeID, DataCodeID, DataProviderID)
)


/* External data input tables */

CREATE TABLE ExternalTSDataNumeric (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DateTime datetime not null,
       Value float,
       QualityCode int,
       ModDate datetime default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE ExternalTSDataNumericHourly (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DateTime datetime not null,
       Value float,
       QualityCode int,
       ModDate datetime default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE ExternalTSDataNumericDaily (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DateTime date not null,
       Value float,
       QualityCode int,
       ModDate datetime default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE ExternalTSDataChar (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DateTime datetime not null,
       Value varchar(99),
       QualityCode int,
       ModDate datetime default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE SiteDatasetLink (
	ExtSiteID varchar(29) NOT NULL,
	DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
    SiteID int NOT NULL,
    PRIMARY KEY (ExtSiteID, DatasetTypeID)
)

CREATE TABLE ExternalSite (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null FOREIGN KEY REFERENCES DatasetType(DatasetTypeID),
       DatasetZ float not null,
       SiteX float not null,
       SiteY float not null,
       SiteZ float,
       ModDate datetime default getdate(),
       PRIMARY KEY (ExtSiteID, DatasetTypeID)
)


/* Time series data tables */

CREATE TABLE TSDataNumeric (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime datetime not null,
       Value float,
       QualityCode int,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       foreign key (ExtSiteID, DatasetTypeID) references SiteDatasetLink (ExtSiteID, DatasetTypeID)
)

CREATE TABLE TSDataNumericHourly (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime datetime not null,
       Value float,
       QualityCode int,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       foreign key (ExtSiteID, DatasetTypeID) references SiteDatasetLink (ExtSiteID, DatasetTypeID)
)

CREATE TABLE TSDataNumericDaily (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime date not null,
       Value float,
       QualityCode int,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       foreign key (ExtSiteID, DatasetTypeID) references SiteDatasetLink (ExtSiteID, DatasetTypeID)
)

CREATE TABLE TSDataChar (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime datetime not null,
       Value varchar(99),
       QualityCode int,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       foreign key (ExtSiteID, DatasetTypeID) references SiteDatasetLink (ExtSiteID, DatasetTypeID)
)

/* Time series data summary tables */

CREATE TABLE TSDataNumericSumm (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime datetime not null,
	   Min float not null,
	   Mean float not null,
	   Max float not null,
	   Count float not null,
	   FromDate datetime not null,
	   ToDate datetime not null,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       FOREIGN KEY (ExtSiteID, DatasetTypeID, Datetime) REFERENCES TSDataNumeric(ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE TSDataNumericHourlySumm (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime datetime not null,
	   Min float not null,
	   Mean float not null,
	   Max float not null,
	   Count float not null,
	   FromDate datetime not null,
	   ToDate datetime not null,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       FOREIGN KEY (ExtSiteID, DatasetTypeID, Datetime) REFERENCES TSDataNumericHourly(ExtSiteID, DatasetTypeID, Datetime)
)

CREATE TABLE TSDataNumericDailySumm (
       ExtSiteID varchar(29) NOT NULL,
       DatasetTypeID int not null,
       DateTime date not null,
	   Min float not null,
	   Mean float not null,
	   Max float not null,
	   Count float not null,
	   FromDate datetime not null,
	   ToDate datetime not null,
       PRIMARY KEY (ExtSiteID, DatasetTypeID, Datetime),
       FOREIGN KEY (ExtSiteID, DatasetTypeID, Datetime) REFERENCES TSDataNumericDaily(ExtSiteID, DatasetTypeID, Datetime)
)

/* Log table */

CREATE TABLE ExtractionLog (
	   RunTimeStart DATETIME NOT NULL,
	   RunTimeEnd DATETIME default getdate(),
	   DataFromTime DATETIME,
	   HydstraArchiveTable varchar(79) NOT NULL,
	   RunResult varchar(9) NOT NULL,
	   Comment varchar(299),
	   PRIMARY KEY (RunTimeStart)
)

/* Views */


--create view vFeatureMtypeSourceShortNames as
--select DatasetTypeID, FeatureShortName, MTypeShortName, CTypeShortName, DataCode, DataProviderName
--from DatasetType
--inner join Feature on DatasetType.FeatureID = Feature.FeatureID
--INNER join MType on DatasetType.MTypeID = MType.MTypeID
--inner join CType on DatasetType.CTypeID = CType.CTypeID
--inner join DataCode on DatasetType.DataCodeID = DataCode.DataCodeID
--inner join DataProvider on DataProvider.DataProviderID = DatasetType.DataProviderID
--
--create view vFeatureMtypeSourceLongNames as
--select DatasetTypeID, FeatureLongName, MTypeLongName, CTypeLongName, DataCode, DataProviderName
--from DatasetType
--inner join Feature on DatasetType.FeatureID = Feature.FeatureID
--INNER join MType on DatasetType.MTypeID = MType.MTypeID
--inner join CType on DatasetType.CTypeID = CType.CTypeID
--inner join DataCode on DatasetType.DataCodeID = DataCode.DataCodeID
--inner join DataProvider on DataProvider.DataProviderID = DatasetType.DataProviderID


