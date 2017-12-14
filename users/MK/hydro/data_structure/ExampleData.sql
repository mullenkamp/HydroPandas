BEGIN TRANSACTION

INSERT INTO Hydro.dbo.SiteMaster (Status, Hazards, Description) VALUES
	('Opened', 'None', 'A nice site'), ('Opened', 'Big rocks', 'Another nice site')

INSERT INTO Hydro.dbo.FeatureMaster (Feature, Description) VALUES
	('River', 'A flowing freshwater surface water body'), ('Aquifer', 'A flowing freshwater groundwater water body'), 
	('Atmosphere', 'Water vapour above ground')

INSERT INTO Hydro.dbo.MtypeMaster (Mtype, MtypeGroup, Description) VALUES
	('Flow', 'Quantity', 'volume per time'), ('Water Level', 'Quanitity', 'The water level above an arbitrary datum'),
	('Nitrate Nitrogen', 'Quality', 'Nitrate nitrogen concentration')

INSERT INTO Hydro.dbo.MeasurementSourceMaster (MeasurementSource, Description) VALUES
	('Recorder', 'Automatic recording device in the field'), ('Manual field', 'Manually measured in the field'), 
	('Manual Lab', 'Manually collected in the field and measured in the lab')	
	
INSERT INTO Hydro.dbo.QualityStateMaster (QualityState, Description) VALUES
	('RAW', 'Raw data directly from measurement source'), ('Quality controlled', 'Quality controlled data')
	
INSERT INTO Hydro.dbo.FeatureAttrMaster (FeatureID, FeatureAttr, Description) VALUES
	(1, 'Minimum Flow Site', 'True'), (2, 'Well Depth', 'Well depth in meters'),
	(2, 'Well Diameter', 'Well diameter in cm')
	
INSERT INTO Hydro.dbo.SiteFeatureAttr (SiteID, FeatureAttrID, Value) VALUES
	(1, 1, 'True'), (2, 2, '10'), (2, 3, '20')	

INSERT INTO Hydro.dbo.LoggingMethodMaster (LoggingMethod, Description) VALUES
	('Discrete', 'A discrete instantaneous measurement'), ('Sum', 'An accumulated sum since the last measurement'), 
	('Mean', 'An aggregated mean since the last measurement')

INSERT INTO Hydro.dbo.TSParamMaster (TSParam, Description) VALUES
	('Lab Name', 'The lab name'), ('Lab Method', 'The lab method')
	
INSERT INTO Hydro.dbo.SiteFeature (SiteID, FeatureID, SiteFeatureNumber, SiteFeatureName) VALUES
	(1, 1, '69607', 'Big Stream at gorge'), (1, 2, 'M35/8765', 'Mistery Well'), (2, 3, '123456', 'Sky limit')		
	
INSERT INTO Hydro.dbo.FeatureMtype (FeatureID, MtypeID, Units) VALUES
	(1, 2, 'meter'), (1, 1, 'meter**3/second'), (1, 3, 'mg/l'), (2, 2, 'meter'), (2, 3, 'mg/l')

INSERT INTO Hydro.dbo.FeatureMtypeSource (FeatureID, MtypeID, MeasurementSourceID, QualityStateID, DataSource) VALUES
	(1, 1, 1, 1, 'Hydrotel'), (1, 1, 1, 2, 'Hydstra'), (1, 1, 2, 2, 'Bgauging'), (1, 3, 2, 2, 'Hilltop'), (2, 2, 2, 2, 'Wells'),
	(2, 3, 2, 2, 'Hilltop')
	
INSERT INTO Hydro.dbo.TSDataSite (SiteID, FeatureID, MtypeID, MeasurementSourceID, QualityStateID, LoggingMethodID, TSGroup) VALUES
	(1, 1, 1, 1, 1, 1, 'Numeric'), (1, 1, 1, 1, 2, 3, 'Numeric'), (1, 1, 1, 2, 2, 1, 'Numeric'), (1, 1, 3, 2, 2, 1, 'Character'), (1, 2, 2, 2, 2, 1, 'Numeric'),
	(1, 2, 3, 2, 2, 1, 'Character')	
	
	
COMMIT