-- Auto Generated (Do not modify) 7163788E7BBD49471D1CA6F649F2A9DC828B72A5201BC093D9440A5C6E0D0854
/* 
SELECT *
FROM dbo.PipelineConfig;
*/

/*
SELECT
    COUNT(*) AS TotalRows,
    MIN(LoadTimestamp) AS EarliestLoadTimestamp,
    MAX(LoadTimestamp) AS LatestLoadTimestamp
FROM dbo.fact_property_sales;
*/

/*
SELECT 'dim_county' AS TableName, COUNT_BIG(*) AS TotalRows
FROM dbo.dim_county
UNION ALL
SELECT 'dim_date', COUNT_BIG(*)
FROM dbo.dim_date
UNION ALL
SELECT 'dim_property_type', COUNT_BIG(*)
FROM dbo.dim_property_type
UNION ALL
SELECT 'dim_sale_type', COUNT_BIG(*)
FROM dbo.dim_sale_type
UNION ALL
SELECT 'fact_property_sales', COUNT_BIG(*)
FROM dbo.fact_property_sales;
*/

/*
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%Pipeline%';
*/

/*SELECT
    PipelineConfigID,
    DatasetName,
    ChildPipelineName,
    ChildPipelineID,
    ExecutionOrder,
    IsActive
FROM dbo.PipelineConfig
ORDER BY ExecutionOrder;
*/

/*
-- Move the existing Warehouse load to execution order 2
UPDATE dbo.PipelineConfig
SET
    DatasetName = 'Property Price Register',
    ChildPipelineName = 'pl_load_ppr_warehouse',
    ChildPipelineID = '9a2a240f-7fe7-4538-b05c-f537b689abfc',
    ExecutionOrder = 2,
    IsActive = 1
WHERE PipelineConfigID = 1;

-- Add the ingestion and transformation pipeline as order 1

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.PipelineConfig
    WHERE ChildPipelineID = '299ad1e3-6e8a-46ed-8da4-eaf326fc5ae9'
)
BEGIN
    INSERT INTO dbo.PipelineConfig
    (
        PipelineConfigID,
        DatasetName,
        ChildPipelineName,
        ChildPipelineID,
        ExecutionOrder,
        IsActive
    )
    VALUES
    (
        2,
        'Property Price Register',
        'pl_ingest_ppr_csv',
        '299ad1e3-6e8a-46ed-8da4-eaf326fc5ae9',
        1,
        1
    );
END;
*/

/*
SELECT
    PipelineConfigID,
    DatasetName,
    ChildPipelineName,
    ChildPipelineID,
    ExecutionOrder,
    IsActive
FROM dbo.PipelineConfig
ORDER BY ExecutionOrder;
*/

/*
SELECT
    COUNT_BIG(*) AS TotalRows,
    MIN(LoadTimestamp) AS EarliestLoadTimestamp,
    MAX(LoadTimestamp) AS LatestLoadTimestamp
FROM dbo.fact_property_sales;
*/

-- SELECT * FROM dbo.PipelineConfig;

/*
CREATE TABLE dbo.PipelineRunLog
(
    LogID VARCHAR(36) NOT NULL,
    ParentRunID VARCHAR(100) NOT NULL,
    PipelineConfigID INT,
    DatasetName VARCHAR(200),
    ChildPipelineName VARCHAR(200),
    ExecutionOrder INT,
    RunStatus VARCHAR(30) NOT NULL,
    StartTime DATETIME2(6),
    EndTime DATETIME2(6),
    DurationSeconds INT,
    LoadDate DATE,
    ErrorMessage VARCHAR(8000)
);
*/

/*
SELECT
    ParentRunID,
    PipelineConfigID,
    DatasetName,
    ChildPipelineName,
    ExecutionOrder,
    RunStatus,
    StartTime,
    EndTime,
    DurationSeconds,
    LoadDate,
    ErrorMessage
FROM dbo.PipelineRunLog
ORDER BY ExecutionOrder;
*/

CREATE VIEW dbo.vw_pipeline_run_summary
AS
SELECT
    ParentRunID,
    MIN(StartTime) AS RunStartTime,
    MAX(EndTime) AS RunEndTime,

    DATEDIFF(
        SECOND,
        MIN(StartTime),
        MAX(EndTime)
    ) AS TotalDurationSeconds,

    CAST(
        DATEDIFF(
            SECOND,
            MIN(StartTime),
            MAX(EndTime)
        ) / 60.0
        AS DECIMAL(10,2)
    ) AS TotalDurationMinutes,

    COUNT(*) AS ChildPipelineCount,

    SUM(
        CASE WHEN RunStatus = 'Succeeded'
        THEN 1 ELSE 0 END
    ) AS SuccessfulChildCount,

    SUM(
        CASE WHEN RunStatus = 'Failed'
        THEN 1 ELSE 0 END
    ) AS FailedChildCount,

    CASE
        WHEN SUM(CASE WHEN RunStatus = 'Failed' THEN 1 ELSE 0 END) > 0
            THEN 'Failed'
        WHEN SUM(CASE WHEN RunStatus = 'Running' THEN 1 ELSE 0 END) > 0
            THEN 'Running'
        ELSE 'Succeeded'
    END AS OverallRunStatus,

    MAX(LoadDate) AS LoadDate
FROM dbo.PipelineRunLog
GROUP BY ParentRunID;