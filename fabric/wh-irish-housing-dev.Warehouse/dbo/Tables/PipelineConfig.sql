CREATE TABLE [dbo].[PipelineConfig] (

	[PipelineConfigID] int NOT NULL, 
	[DatasetName] varchar(100) NOT NULL, 
	[ChildPipelineName] varchar(200) NOT NULL, 
	[ExecutionOrder] int NOT NULL, 
	[IsActive] bit NOT NULL, 
	[ChildPipelineID] varchar(100) NULL
);