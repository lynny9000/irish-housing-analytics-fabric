CREATE TABLE [dbo].[fact_property_sales] (

	[PropertySaleKey] bigint NOT NULL, 
	[DateKey] int NOT NULL, 
	[CountyKey] int NOT NULL, 
	[PropertyTypeKey] int NOT NULL, 
	[SaleTypeKey] int NOT NULL, 
	[Address] varchar(500) NULL, 
	[Eircode] varchar(10) NULL, 
	[SalePrice] decimal(18,2) NOT NULL, 
	[IsFullMarketPrice] varchar(3) NULL, 
	[LoadTimestamp] datetime2(6) NULL
);