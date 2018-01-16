
--CREATE TABLE Hydro.dbo.BgaugingLink (
--	   FeatureMtypeSourceID int NOT NULL FOREIGN KEY REFERENCES FeatureMtypeSource(FeatureMtypeSourceID),
--	   BgaugingCode varchar(9) NOT NULL,
--	   PRIMARY KEY (FeatureMtypeSourceID)
--)
--
--INSERT INTO Hydro.dbo.BgaugingLink (FeatureMtypeSourceID, BgaugingCode) VALUES
--	(8, 'Flow'), (7, 'Stage')


create view BgaugingTSData AS
select RiverSiteIndex AS Site, BgaugingLink.FeatureMtypeSourceID, Date as Time, Value
from Bgauging.dbo.BGauging
unpivot
(
Value for Type in (Stage, Flow)
) u
inner join BgaugingLink on u.Type = BgaugingLink.BgaugingCode




