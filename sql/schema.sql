------------------------------------------------------------
-- Dimension: Police Forces
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dim_force' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_force (
        id NVARCHAR(100) NOT NULL PRIMARY KEY,
        name NVARCHAR(255) NOT NULL
    );
END;
GO

------------------------------------------------------------
-- Bronze Layer: raw JSON payloads
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'bronze_stop_search' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.bronze_stop_search (
        row_hash CHAR(64) NOT NULL PRIMARY KEY,          -- SHA256 hash of payload
        force_id NVARCHAR(100) NOT NULL,
        [month] DATE NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,                 -- full JSON text
        inserted_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_bronze_force_month ON dbo.bronze_stop_search(force_id, [month]);
END;
GO

------------------------------------------------------------
-- Silver Layer: typed fact table
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'fact_stop_search' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.fact_stop_search (
        row_hash CHAR(64) NOT NULL PRIMARY KEY,
        force_id NVARCHAR(100) NOT NULL,
        stop_datetime DATETIME2(0) NULL,
        stop_date DATE NULL,
        [type] NVARCHAR(200) NULL,
        involved_person BIT NULL,
        gender NVARCHAR(50) NULL,
        age_range NVARCHAR(50) NULL,
        self_defined_ethnicity NVARCHAR(200) NULL,
        officer_defined_ethnicity NVARCHAR(200) NULL,
        legislation NVARCHAR(400) NULL,
        object_of_search NVARCHAR(400) NULL,
        outcome NVARCHAR(200) NULL,
        outcome_linked_to_object_of_search BIT NULL,
        outcome_object_id NVARCHAR(100) NULL,
        outcome_object_name NVARCHAR(200) NULL,
        removal_more_than_outer_clothing BIT NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL,
        street_id BIGINT NULL,
        street_name NVARCHAR(300) NULL,
        [month] DATE NOT NULL,
        inserted_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_fact_force_month ON dbo.fact_stop_search(force_id, [month]);
    CREATE INDEX IX_fact_datetime ON dbo.fact_stop_search(stop_datetime);
    CREATE INDEX IX_fact_outcome ON dbo.fact_stop_search(outcome);
END;
GO

------------------------------------------------------------
-- Gold Layer: monthly outcomes aggregation (fixed PK)
------------------------------------------------------------
IF OBJECT_ID(N'dbo.gold_monthly_outcomes', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.gold_monthly_outcomes;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'gold_monthly_outcomes' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.gold_monthly_outcomes (
        force_id NVARCHAR(100) NOT NULL,
        [month]  DATE NOT NULL,
        outcome  NVARCHAR(200) NOT NULL CONSTRAINT DF_gold_outcome DEFAULT(''),
        [count]  INT NOT NULL,
        CONSTRAINT PK_gold PRIMARY KEY (force_id, [month], outcome)
    );
END;
GO
