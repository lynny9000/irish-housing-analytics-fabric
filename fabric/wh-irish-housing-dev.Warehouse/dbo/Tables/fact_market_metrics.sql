CREATE TABLE [dbo].[fact_market_metrics] (

	[MarketMetricKey] bigint NOT NULL, 
	[DateKey] int NOT NULL, 
	[CountyKey] int NOT NULL, 
	[SalesCount] bigint NULL, 
	[AverageSalePrice] decimal(18,2) NULL, 
	[MinimumSalePrice] decimal(18,2) NULL, 
	[MaximumSalePrice] decimal(18,2) NULL
);