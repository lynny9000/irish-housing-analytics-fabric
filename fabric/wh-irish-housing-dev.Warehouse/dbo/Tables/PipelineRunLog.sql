CREATE TABLE [dbo].[PipelineRunLog] (

	[LogID] varchar(36) NOT NULL, 
	[ParentRunID] varchar(100) NOT NULL, 
	[PipelineConfigID] int NULL, 
	[DatasetName] varchar(200) NULL, 
	[ChildPipelineName] varchar(200) NULL, 
	[ExecutionOrder] int NULL, 
	[RunStatus] varchar(30) NOT NULL, 
	[StartTime] datetime2(6) NULL, 
	[EndTime] datetime2(6) NULL, 
	[DurationSeconds] int NULL, 
	[LoadDate] date NULL, 
	[ErrorMessage] varchar(8000) NULL
);