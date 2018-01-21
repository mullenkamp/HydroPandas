
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
