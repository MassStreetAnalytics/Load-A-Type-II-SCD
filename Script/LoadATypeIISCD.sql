-- =============================================
-- Author: Bob Wakefield
-- Create date: 10Oct17
-- Description: Example of properly processing Type II SCDs with T-SQL
-- Create a database to work in or work in a database you're
-- comfortable breaking.
-- =============================================


BEGIN TRANSACTION

--Begin by clearing your work area.
IF OBJECT_ID('DimTickers') IS NOT NULL DROP TABLE DimTickers
IF OBJECT_ID('tempdb..#DimTickers') IS NOT NULL DROP TABLE #DimTickers
IF OBJECT_ID('DimTickersCM') IS NOT NULL DROP TABLE DimTickersCM
IF OBJECT_ID('TickersStage') IS NOT NULL DROP TABLE TickersStage

--Set up date variables.
--Every organization should have an arbitrary high and low date
DECLARE @LowDate AS DATETIME = '19000101'
DECLARE @HighDate AS DATETIME = '99991231'

--Now create all the tables we will need.
--Create our stage table
CREATE TABLE [TickersStage](
[ETLKey] [uniqueidentifier] NOT NULL,
[Symbol] [nvarchar](255) NULL,
[CompanyName] [nvarchar](255) NULL,
[UniqueDims] [varbinary](35) NULL,
[UniqueRows] [varbinary](16) NULL,
[SourceSystem] [nvarchar](255) NULL,
[ErrorRecord] [bit] NULL,
[Processed] [bit] NULL,
[RunDate] [datetime] NULL,
[RowHash]  AS (CONVERT([binary](16),hashbytes('MD5',[CompanyName]),0)) PERSISTED,
 CONSTRAINT [PK_DimTickersCM] PRIMARY KEY CLUSTERED 
(
[ETLKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
ALTER TABLE [TickersStage] ADD  CONSTRAINT [DF_DimTickersCM_ETLKey]  DEFAULT (newid()) FOR [ETLKey]

--Let's create our warehouse table.
--In the real world, this table would have a FK constraint
--as it is the parent to a fact table.
CREATE TABLE [DimTickers](
[TickersCK] [bigint] IDENTITY(1,1) NOT NULL,
[Symbol] [nvarchar](50) NULL,
[CompanyName] [nvarchar](100) NULL,
[CreatedBy] [nvarchar](50) NULL,
[CreatedOn] [datetime] NULL,
[UpdatedBy] [nvarchar](50) NULL,
[UpdatedOn] [datetime] NULL,
[SourceSystem] [nvarchar](100) NULL,
[SourceSystemKey] [nvarchar](100) NULL,
[EffectiveFrom] [datetime] NULL,
[EffectiveTo] [datetime] NULL,
[IsMostRecentRecord] [bit] NULL,
[RowHash]  AS (CONVERT([binary](16),hashbytes('MD5',[CompanyName]),0)) PERSISTED,
 CONSTRAINT [PK_tickers] PRIMARY KEY CLUSTERED 
(
[TickersCK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]


--Let's create our common model table
CREATE TABLE [DimTickersCM](
[TickersCK] [bigint] NULL,
[Symbol] [nvarchar](50) NULL,
[CompanyName] [nvarchar](100) NULL,
[CreatedBy] [nvarchar](50) NULL,
[CreatedOn] [datetime] NULL,
[UpdatedBy] [nvarchar](50) NULL,
[UpdatedOn] [datetime] NULL,
[SourceSystem] [nvarchar](100) NULL,
[SourceSystemKey] [nvarchar](100) NULL,
[EffectiveFrom] [datetime] NULL,
[EffectiveTo] [datetime] NULL,
[IsMostRecentRecord] [bit] NULL,
[RowHash]  AS (CONVERT([binary](16),hashbytes('MD5',[CompanyName]),0)) PERSISTED,
)


--Lets create our mirror temp table
CREATE TABLE #DimTickers(
[TickersCK] [bigint] NULL,
[Symbol] [nvarchar](50) NULL,
[CompanyName] [nvarchar](100) NULL,
[CreatedBy] [nvarchar](50) NULL,
[CreatedOn] [datetime] NULL,
[UpdatedBy] [nvarchar](50) NULL,
[UpdatedOn] [datetime] NULL,
[SourceSystem] [nvarchar](100) NULL,
[SourceSystemKey] [nvarchar](100) NULL,
[EffectiveFrom] [datetime] NULL,
[EffectiveTo] [datetime] NULL,
[IsMostRecentRecord] [bit] NULL,
[RowHash]  AS (CONVERT([binary](16),hashbytes('MD5',[CompanyName]),0)) PERSISTED
)



--Now we will insert our initial load. 
--Funny enough, I actually did find an error in my production data. 
--We will use that as an example of something messed up.
INSERT INTO TickersStage([Symbol],[CompanyName],[SourceSystem],[ErrorRecord],[Processed],[RunDate])
SELECT 'AAPL','Apple Inc','Yahoo',0,0,CURRENT_TIMESTAMP
UNION
SELECT 'UMBF','Ump Financial Corp','Yahoo',0,0,CURRENT_TIMESTAMP --This is an actual error I pulled from prod!
UNION
SELECT 'ACN','Accenture Plc','Yahoo',0,0,CURRENT_TIMESTAMP


--Now we will begin the process of actually loading the warehouse.
--Move data from staging to common model.
TRUNCATE TABLE DimTickersCM

INSERT INTO DimTickersCM(
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey]
)
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[Symbol] AS SourceSystemKey
FROM TickersStage
WHERE Processed = 0
AND ErrorRecord = 0


--Handle New Records
MERGE DimTickers AS target
USING (
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[Symbol] AS SourceSystemKey
FROM DimTickersCM
) AS source
ON target.[SourceSystemKey] = source.[SourceSystemKey]

WHEN NOT MATCHED THEN
INSERT (
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
EffectiveFrom,
EffectiveTo,
IsMostRecentRecord,
CreatedBy,
CreatedOn
)
VALUES (
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
@LowDate,
@HighDate,
1,
SYSTEM_USER,
CURRENT_TIMESTAMP
);

--Let’s check our work so far. You should get three records in DimTickers.
SELECT * FROM DimTickers


--That's our inital load. Let's go to day 2 and fix that
--embarrassing data entry error for UMB!
TRUNCATE TABLE TickersStage

INSERT INTO TickersStage([Symbol],[CompanyName],[SourceSystem],[ErrorRecord],[Processed],[RunDate])
SELECT 'UMBF','UMB Financial Corp','Yahoo',0,0,CURRENT_TIMESTAMP --Let's fix the company name so it's correct

--Now to fix things.
TRUNCATE TABLE DimTickersCM

INSERT INTO DimTickersCM(
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey]
)
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[Symbol] AS SourceSystemKey
FROM TickersStage
WHERE Processed = 0
AND ErrorRecord = 0



--Handle changed records

--Here is where things start to differ
--We are going to use a MERGE like normal, 
--but we are going to add an OUTPUT statement 
--and dump the results to a temporary table 
--that has no foreign key constraint
--which we'll later flush to prod.
INSERT INTO #DimTickers(
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
EffectiveFrom,
EffectiveTo,
IsMostRecentRecord,
CreatedBy,
CreatedOn
)
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
EffectiveFrom,
EffectiveTo,
IsMostRecentRecord,
CreatedBy,
CreatedOn 
FROM(--Here we're using the MERGE statement as a derived table.
MERGE DimTickers AS target
USING (
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[Symbol] AS SourceSystemKey, 
RowHash
FROM DimTickersCM
) AS source
ON target.[SourceSystemKey]  = source.[SourceSystemKey] 
WHEN MATCHED
AND source.RowHash <> target.RowHash --Look for changed data
AND target.IsMostRecentRecord = 1
THEN
UPDATE
SET
[UpdatedBy] = SYSTEM_USER,
[UpdatedOn] = CURRENT_TIMESTAMP,
EffectiveTo = DATEADD(ss,-1,CURRENT_TIMESTAMP), -- Make sure things don't overlap. There is some controversy doing it this way.
IsMostRecentRecord = 0
OUTPUT --Output columns need to match your select statement
$action Action_Out,
source.[Symbol],
source.[CompanyName],
source.[SourceSystem],
source.[SourceSystemKey],
CURRENT_TIMESTAMP AS EffectiveFrom,
@HighDate AS EffectiveTo,
1 AS IsMostRecentRecord,
SYSTEM_USER AS CreatedBy,
CURRENT_TIMESTAMP AS CreatedOn
) AS MERGE_OUT
WHERE MERGE_OUT.Action_Out = 'UPDATE'
;

--Now we flush the data in the temp table into prod.
INSERT INTO DimTickers(
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
EffectiveFrom,
EffectiveTo,
IsMostRecentRecord,
CreatedBy,
CreatedOn
)
SELECT
[Symbol],
[CompanyName],
[SourceSystem],
[SourceSystemKey],
EffectiveFrom,
EffectiveTo,
IsMostRecentRecord,
CreatedBy,
CreatedOn
FROM #DimTickers


--Go check DimTickers again. You should have four records now - three original and one extra for UMB. 
SELECT * FROM DimTickers ORDER BY Symbol, EffectiveTo 

COMMIT TRANSACTION

--Do not forget to clean up your environment.
DROP TABLE DimTickers
DROP TABLE DimTickersCM
DROP TABLE #DimTickers
DROP TABLE TickersStage
