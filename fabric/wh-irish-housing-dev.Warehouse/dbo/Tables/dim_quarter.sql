CREATE TABLE [dbo].[dim_quarter] (

	[QuarterKey] int NOT NULL, 
	[Quarter] varchar(10) NOT NULL, 
	[QuarterStartDate] date NOT NULL, 
	[Year] int NOT NULL, 
	[QuarterNumber] int NOT NULL
);