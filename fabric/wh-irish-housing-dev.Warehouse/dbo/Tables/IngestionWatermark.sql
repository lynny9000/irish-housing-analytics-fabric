CREATE TABLE [dbo].[IngestionWatermark] (

	[WatermarkConfigID] int NOT NULL, 
	[DatasetName] varchar(100) NOT NULL, 
	[SourceTable] varchar(200) NOT NULL, 
	[WatermarkColumn] varchar(100) NOT NULL, 
	[LastWatermarkValue] datetime2(6) NOT NULL, 
	[IsActive] bit NOT NULL, 
	[UpdatedAt] datetime2(6) NULL
);