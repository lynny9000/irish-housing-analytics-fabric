CREATE TABLE [dbo].[fact_housing_vacancy] (

	[HousingVacancyKey] bigint NOT NULL, 
	[QuarterKey] int NOT NULL, 
	[CountyKey] int NOT NULL, 
	[VacantDwellings] bigint NULL, 
	[DwellingStock] bigint NULL, 
	[VacancyRate] decimal(5,1) NULL, 
	[SourceUpdatedTimestamp] datetime2(6) NULL, 
	[LoadDate] date NULL, 
	[LoadTimestamp] datetime2(6) NULL
);