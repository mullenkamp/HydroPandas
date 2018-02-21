
--update NiwaAquaAtmosTSData
--set Origin = replace(Origin, 'original', '0') where Origin like '%original%';
--
--update NiwaAquaAtmosTSData
--set Origin = replace(Origin, 'out maxT', '') where Origin like '%out maxT%';
--
--update NiwaAquaAtmosTSData
--set Origin = replace(Origin, 'out  maxT', '') where Origin like '%out  maxT%';
--
--update NiwaAquaAtmosTSData
--set Origin = replace(Origin, 'out minT', '') where Origin like '%out minT%';

CREATE TABLE Hydro.dbo.NiwaAquaAtmosRecLink (
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   NiwaAquaAtmosCode varchar(9) NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID)
)

INSERT INTO Hydro.dbo.NiwaAquaAtmosRecLink (FeatureMtypeSourceID, NiwaAquaAtmosCode) VALUES
	(18, 'MaxT'), (20, 'MinT'), (22, 'NineT'), (24, 'PenmanET'), (38, 'Rain'), 
	(34, 'SolRad'), (28, 'VapourP'), (30, 'WindRun'), (32, 'NineWind');
	
CREATE TABLE Hydro.dbo.NiwaAquaAtmosSynthLink (
	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
	   NiwaAquaAtmosCode varchar(9) NOT NULL,
	   PRIMARY KEY (FeatureMtypeSourceID)
);

INSERT INTO Hydro.dbo.NiwaAquaAtmosSynthLink (FeatureMtypeSourceID, NiwaAquaAtmosCode) VALUES
	(19, 'MaxT'), (21, 'MinT'), (23, 'NineT'), (25, 'PenmanET'), (26, 'Rain'), 
	(27, 'SolRad'), (29, 'VapourP'), (31, 'WindRun'), (33, 'NineWind');
	
create view vNiwaAquaAtmosRecTSDataDaily AS
select Site, FeatureMtypeSourceID, Date as Time, Value, 200 as QualityCode
from Hydro.dbo.NiwaAquaAtmosTSDataBase 
inner join NiwaAquaAtmosRecLink on NiwaAquaAtmosTSDataBase.Variable = NiwaAquaAtmosRecLink.NiwaAquaAtmosCode
where Origin=0;

create view vNiwaAquaAtmosSynthTSDataDaily AS
select Site, FeatureMtypeSourceID, Date as Time, Value, 200 as QualityCode
from Hydro.dbo.NiwaAquaAtmosTSDataBase 
inner join NiwaAquaAtmosSynthLink on NiwaAquaAtmosTSDataBase.Variable = NiwaAquaAtmosSynthLink.NiwaAquaAtmosCode;

--create view vNiwaAquaAtmosTSDataDaily AS
--select * from vNiwaAquaAtmosRecTSDataDaily
--UNION
--select * from vNiwaAquaAtmosSynthTSDataDaily;

select *
into NiwaAquaAtmosRecTSDataDaily
from vNiwaAquaAtmosRecTSDataDaily;

select *
into NiwaAquaAtmosSynthTSDataDaily
from vNiwaAquaAtmosSynthTSDataDaily;

select *
into NiwaAquaAtmosTSDataDaily
from
(select * from NiwaAquaAtmosRecTSDataDaily
UNION
select * from NiwaAquaAtmosSynthTSDataDaily) as u;

alter table NiwaAquaAtmosTSDataDaily
alter column Site varchar(29) not null;

alter table NiwaAquaAtmosTSDataDaily
add primary key (Site, FeatureMtypeSourceID, Time);








alter table NiwaAquaAtmosSites
alter column Site int not null;

alter table NiwaAquaAtmosSites
ADD PRIMARY KEY (Site);

alter table NiwaAquaAtmosTSDataDaily
alter column Origin int not null;

alter table NiwaAquaAtmosTSDataDaily
alter column Date date not null;

alter table NiwaAquaAtmosTSDataDaily
alter column Site int not null;

alter table NiwaAquaAtmosTSDataDaily
alter column Variable varchar(19) not null;

alter table NiwaAquaAtmosTSDataDaily
ADD PRIMARY KEY (Date, Site, Variable);

alter table NiwaAquaAtmosTSDataDaily
ADD FOREIGN KEY (Site) references NiwaAquaAtmosSites(Site);



