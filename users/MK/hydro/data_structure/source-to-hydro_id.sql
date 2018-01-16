/* script to create the source-to-hydro_id tables */
BEGIN TRANSACTION


/* select distinct HydstraCode from Hydro.dbo.HydstraTSDataDaily */

CREATE TABLE Hydro.dbo.HydstraLink (
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   HydstraCode int NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID)
)

INSERT INTO Hydro.dbo.HydstraLink (FeatureMtypeSourceID, HydstraCode) VALUES
	(4, 100), (5, 140), (6, 450), (11, 110), (15, 10), (16, 130)


select HydstraLink.FeatureMtypeSourceID from HydstraTSDataDailyOld
inner join HydstraLink on HydstraTSDataDailyOld.HydstraCode = HydstraLink.HydstraCode


CREATE TABLE Hydro.dbo.HydstraTSDataDaily (
	   Site varchar(19) NOT NULL,
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   Time DATE NOT NULL,
	   Value float,
	   QualityCode smallint,
	   ModDate DATE NOT NULL,
	   PRIMARY KEY (Site, FeatureMtypeSourceID, Time)
)


insert into HydstraTSDataDaily (Site, FeatureMtypeSourceID, Time, Value, QualityCode, ModDate)
select Site, HydstraLink.FeatureMtypeSourceID, Time, Value, QualityCode, ModDate from HydstraTSDataDailyOld
inner join HydstraLink on HydstraTSDataDailyOld.HydstraCode = HydstraLink.HydstraCode



CREATE TABLE Hydro.dbo.HydstraTSDataHourly (
	   Site varchar(19) NOT NULL,
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode smallint,
	   ModDate DATE NOT NULL,
	   PRIMARY KEY (Site, FeatureMtypeSourceID, Time)
)


insert into HydstraTSDataHourly (Site, FeatureMtypeSourceID, Time, Value, QualityCode, ModDate)
select Site, HydstraLink.FeatureMtypeSourceID, Time, Value, QualityCode, ModDate from HydstraTSDataHourlyOld
inner join HydstraLink on HydstraTSDataHourlyOld.HydstraCode = HydstraLink.HydstraCode





