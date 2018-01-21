
CREATE TABLE Hydro.dbo.HydstraTSDataHourly (
	   Site varchar(19) NOT NULL,
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   Time DATETIME NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (Site, FeatureMtypeSourceID, Time)
)

CREATE TABLE Hydro.dbo.HydrotelTSDataDaily (
	   Site varchar(19) NOT NULL,
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   Time DATE NOT NULL,
	   Value float,
	   QualityCode smallint,
	   PRIMARY KEY (Site, FeatureMtypeSourceID, Time)
)

CREATE TABLE Hydro.dbo.HydstraQualCodeLink (
	   HydstraQualCode int NOT NULL,
	   NEMSCode int,
	   PRIMARY KEY (HydstraQualCode)
)

INSERT INTO Hydro.dbo.HydstraQualCodeLink VALUES
 (10, 600), (11, 600), (18, 520), (20, 500), (21, 500), (30, 400), (31, 400), (40, 300), (50, 200), (60, 100)

select Site, FeatureMtypeSourceID, Time, Value, NEMSCode as QualityCode into HydstraTSDataDaily
from HydstraTSDatadailyOld
inner join HydstraQualCodeLink on HydstraQualCodeLink.HydstraQualCode = HydstraTSDataDailyOld.QualityCode

alter table HydstraTSDataDaily
alter column Site varchar(29) not null

alter table HydstraTSDataDaily
add primary key (Site, FeatureMtypeSourceID, Time)

alter table HydstraTSDataDaily
ADD FOREIGN KEY (FeatureMtypeSourceID) REFERENCES FeatureMtypeSource(FeatureMtypeSourceID)

select Site, FeatureMtypeSourceID, Time, Value, NEMSCode as QualityCode into HydstraTSDataHourly
from HydstraTSDataHourlyOld
inner join HydstraQualCodeLink on HydstraQualCodeLink.HydstraQualCode = HydstraTSDataHourlyOld.QualityCode

alter table HydstraTSDataHourly
add primary key (Site, FeatureMtypeSourceID, Time)

alter table HydstraTSDataHourly
ADD FOREIGN KEY (FeatureMtypeSourceID) REFERENCES FeatureMtypeSource(FeatureMtypeSourceID)



select * into HydstraTSDataDailyTest1
from 
(select replace(Site, '_', '/') as Site, FeatureMtypeSourceID, Time, Value, QualityCode
from HydstraTSDataDaily) as temp

select distinct Site from HydstraTSDataDailyTest1
where FeatureMtypeSourceID=11

select * into HydstraTSDataHourlyTest1
from 
(select replace(Site, '_', '/') as Site, FeatureMtypeSourceID, Time, Value, QualityCode
from HydstraTSDataHourly) as temp

select distinct Site from HydstraTSDataHourlyTest1
where FeatureMtypeSourceID=11


alter table HydstraTSDataHourlyTest1
alter column Site varchar(29) not null

alter table HydstraTSDataHourlyTest1
add primary key (Site, FeatureMtypeSourceID, Time)

alter table HydstraTSDataHourlyTest1
ADD FOREIGN KEY (FeatureMtypeSourceID) REFERENCES FeatureMtypeSource(FeatureMtypeSourceID)

insert into HydstraTSDataHourly
select * from HydstraTSDataHourlyTest1

delete from HydstraTSDataDaily
where Site in ('BY19_0026', 'L36_0282', 'L37_0451')


