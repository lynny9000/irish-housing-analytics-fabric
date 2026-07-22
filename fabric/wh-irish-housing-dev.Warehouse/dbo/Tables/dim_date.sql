CREATE TABLE [dbo].[dim_date] (

	[DateKey] int NOT NULL, 
	[FullDate] date NOT NULL, 
	[Year] int NOT NULL, 
	[Quarter] int NOT NULL, 
	[Month] int NOT NULL, 
	[MonthName] varchar(20) NOT NULL
);