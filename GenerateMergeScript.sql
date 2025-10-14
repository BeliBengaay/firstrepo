/*
  GenerateMergeScript.sql
  Polished stored procedure to generate a MERGE statement for a given table.

  Features:
  - Detects primary key columns and non-computed columns
  - Builds ON clause from PK columns
  - Builds INSERT (all non-computed columns)
  - Builds UPDATE with NULL-safe comparisons
    * For types where direct <> comparison is allowed, uses explicit NULL-safe <> checks
    * For types that may not support <> safely (xml, sql_variant, text/ntext), falls back to
      CAST/CONVERT to NVARCHAR(MAX) or to CHECKSUM semantics when necessary
  - Option to exclude columns (e.g. audit columns) via pattern
  - Option to include WHEN NOT MATCHED BY SOURCE THEN DELETE
  - Returns the generated MERGE statement as NVARCHAR(MAX) in a single-row resultset

  Usage example:
    EXEC dbo.usp_GenerateMergeScript
      @SchemaName = 'dbo',
      @TableName  = 'YourMasterTable',
      @IncludeDeletes = 1,
      @ExcludeColumnPattern = 'Audit%,Modified%';

  Notes / Safeguards:
  - This generator is intended for small-to-medium master tables. For very large tables, run-time
    and the size of the resulting MERGE may be problematic. Consider batching or using a staging table.
  - The generated MERGE uses a USING(SELECT * FROM schema.table) AS S pattern so it will produce a
    statement that re-reads the same table as source. Adjust as needed to point to a different source
    (VALUES list, temp table, external source) before executing the MERGE.
  - Validate the generated SQL before executing in a production environment.
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.usp_GenerateMergeScript', 'P') IS NOT NULL
  DROP PROCEDURE dbo.usp_GenerateMergeScript;
GO

CREATE PROCEDURE dbo.usp_GenerateMergeScript
(
    @SchemaName SYSNAME,
    @TableName  SYSNAME,
    @IncludeDeletes BIT = 1,
    @ExcludeColumnPattern NVARCHAR(4000) = NULL -- comma-separated wildcard patterns to exclude, e.g. 'Audit%,Modified%'
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FullTableName SYSNAME = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
    DECLARE @ObjectId INT = OBJECT_ID(@FullTableName);

    IF @ObjectId IS NULL
    BEGIN
        THROW 50001, 'Table ' + @FullTableName + ' does not exist or you do not have permission.', 1;
    END

    -- Get PK columns
    DECLARE @PKCols TABLE (colname SYSNAME, colordinal INT);

    INSERT INTO @PKCols (colname, colordinal)
    SELECT c.name, ic.key_ordinal
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.is_primary_key = 1
      AND i.object_id = @ObjectId
    ORDER BY ic.key_ordinal;

    IF NOT EXISTS(SELECT 1 FROM @PKCols)
    BEGIN
        THROW 50002, 'Table has no primary key. A primary key is required to build the MERGE ON clause.', 1;
    END

    -- Process exclude patterns into a table for faster checks
    DECLARE @Exclude TABLE (pattern NVARCHAR(4000));
    IF @ExcludeColumnPattern IS NOT NULL
    BEGIN
        INSERT INTO @Exclude (pattern)
        SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@ExcludeColumnPattern, ',') WHERE LTRIM(RTRIM(value)) <> '';
    END

    -- Gather column metadata (exclude computed columns)
    DECLARE @Cols TABLE (
        colname SYSNAME,
        column_id INT,
        is_pk BIT,
        is_computed BIT,
        system_type_name NVARCHAR(256),
        max_length INT
    );

    INSERT INTO @Cols (colname, column_id, is_pk, is_computed, system_type_name, max_length)
    SELECT c.name, c.column_id,
           CASE WHEN pk.colname IS NOT NULL THEN 1 ELSE 0 END,
           c.is_computed,
           TYPE_NAME(c.user_type_id) + CASE WHEN TYPE_NAME(c.user_type_id) IN ('varchar','nvarchar','varbinary')
                                            THEN '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST((CASE WHEN TYPE_NAME(c.user_type_id) = 'nvarchar' THEN c.max_length/2 ELSE c.max_length END) AS VARCHAR(10)) END + ')'
                                            ELSE '' END as system_type_name,
           c.max_length
    FROM sys.columns c
    LEFT JOIN (SELECT colname FROM @PKCols) pk ON pk.colname = c.name
    WHERE c.object_id = @ObjectId
      AND c.is_computed = 0
    ORDER BY c.column_id;

    -- Exclude columns matching patterns
    DELETE C
    FROM @Cols C
    WHERE EXISTS (
        SELECT 1 FROM @Exclude E WHERE C.colname LIKE E.pattern
    );

    -- Build string lists
    DECLARE @AllCols NVARCHAR(MAX);
    DECLARE @PKList NVARCHAR(MAX);
    DECLARE @InsertCols NVARCHAR(MAX);
    DECLARE @InsertVals NVARCHAR(MAX);
    DECLARE @OnClause NVARCHAR(MAX);
    DECLARE @UpdateSet NVARCHAR(MAX);
    DECLARE @UpdateCondition NVARCHAR(MAX);

    -- All non-computed columns (used for INSERT)
    SELECT @AllCols = STRING_AGG(QUOTENAME(colname), ', ') FROM @Cols;

    -- PK list
    SELECT @PKList = STRING_AGG(QUOTENAME(colname), ', ') FROM (SELECT colname FROM @Cols WHERE is_pk = 1 ORDER BY column_id) t;

    -- Insert cols and values
    SELECT @InsertCols = STRING_AGG(QUOTENAME(colname), ', '),
           @InsertVals = STRING_AGG('S.' + QUOTENAME(colname), ', ')
    FROM @Cols;

    -- ON clause using PK columns: S.[PK] = T.[PK] AND ...
    SELECT @OnClause = STRING_AGG('S.' + QUOTENAME(colname) + ' = T.' + QUOTENAME(colname), ' AND ')
    FROM (SELECT colname FROM @Cols WHERE is_pk = 1 ORDER BY column_id) t;

    -- Build UPDATE SET: exclude PK cols from update set
    SELECT @UpdateSet = STRING_AGG(QUOTENAME(colname) + ' = S.' + QUOTENAME(colname), ', ')
    FROM (SELECT colname FROM @Cols WHERE is_pk = 0 ORDER BY column_id) t;

    -- Build NULL-safe update condition across non-PK columns
    -- For columns where simple <> is allowed, use explicit checks; for other types fallback to CAST/CONVERT or CHECKSUM
    DECLARE @cond NVARCHAR(MAX) = N'';

    DECLARE curCols CURSOR LOCAL FAST_FORWARD FOR
    SELECT colname, system_type_name, max_length
    FROM @Cols
    WHERE is_pk = 0
    ORDER BY column_id;

    OPEN curCols;
    DECLARE @col SYSNAME; DECLARE @type NVARCHAR(256); DECLARE @maxlen INT;
    FETCH NEXT FROM curCols INTO @col, @type, @maxlen;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @expr NVARCHAR(2000);

        -- Decide comparison strategy based on type hints
        IF @type LIKE 'xml%' OR @type LIKE 'text%' OR @type LIKE 'ntext%'
        BEGIN
            -- cast XML / text types to NVARCHAR(MAX) before comparison
            SET @expr = '(' +
                        ' (S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR' +
                        ' (S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR' +
                        ' (CAST(S.' + QUOTENAME(@col) + ' AS NVARCHAR(MAX)) <> CAST(T.' + QUOTENAME(@col) + ' AS NVARCHAR(MAX)))' +
                        ' )';
        END
        ELSE IF @type LIKE 'sql_variant%'
        BEGIN
            -- use CHECKSUM on sql_variant - coarse but usable
            SET @expr = '(' +
                        ' (S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR' +
                        ' (S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR' +
                        ' (CHECKSUM(S.' + QUOTENAME(@col) + ') <> CHECKSUM(T.' + QUOTENAME(@col) + '))' +
                        ' )';
        END
        ELSE
        BEGIN
            -- default: use <> with NULL checks (works for numeric, char, varchar, datetime, binary, varbinary, uniqueidentifier, bit)
            SET @expr = '(' +
                        ' (S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR' +
                        ' (S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR' +
                        ' (S.' + QUOTENAME(@col) + ' <> T.' + QUOTENAME(@col) + ')' +
                        ' )';
        END

        IF LEN(@cond) = 0
            SET @cond = @expr;
        ELSE
            SET @cond = @cond + CHAR(13) + ' OR ' + @expr;

        FETCH NEXT FROM curCols INTO @col, @type, @maxlen;
    END
    CLOSE curCols; DEALLOCATE curCols;

    SET @UpdateCondition = @cond;

    -- Final safety checks
    IF @AllCols IS NULL OR @OnClause IS NULL OR @UpdateSet IS NULL
    BEGIN
        THROW 50003, 'Unable to construct column lists for the table. Possibly all columns are excluded or table only has computed columns.', 1;
    END

    -- Compose the MERGE statement
    DECLARE @MergeSQL NVARCHAR(MAX) = N'';

    SET @MergeSQL = N'-- MERGE generated by dbo.usp_GenerateMergeScript for ' + @FullTableName + CHAR(13) +
    N'MERGE INTO ' + @FullTableName + ' AS T' + CHAR(13) +
    N'USING (' + CHAR(13) +
    N'    SELECT ' + @AllCols + ' FROM ' + @FullTableName + CHAR(13) +
    N') AS S' + CHAR(13) +
    N'    ON ' + @OnClause + CHAR(13) +
    N'WHEN MATCHED AND (' + CHAR(13) + @UpdateCondition + CHAR(13) + N')' + CHAR(13) +
    N'    THEN UPDATE SET ' + @UpdateSet + CHAR(13) +
    N'WHEN NOT MATCHED BY TARGET' + CHAR(13) +
    N'    THEN INSERT (' + @InsertCols + ')' + CHAR(13) +
    N'         VALUES (' + @InsertVals + ')' + CHAR(13);

    IF @IncludeDeletes = 1
        SET @MergeSQL = @MergeSQL + N'WHEN NOT MATCHED BY SOURCE' + CHAR(13) + N'    THEN DELETE;' + CHAR(13);
    ELSE
        SET @MergeSQL = @MergeSQL + N';' + CHAR(13);

    SET @MergeSQL = @MergeSQL + N'-- End MERGE.' + CHAR(13);

    -- Return the generated script as a single NVARCHAR(MAX) row so caller can capture it
    SELECT @MergeSQL AS MergeStatement;
END;
GO

/*
Quick test (run on a test table):

CREATE TABLE dbo.TestMaster (
  Id INT PRIMARY KEY,
  Name NVARCHAR(200),
  Data XML,
  ModifiedDate DATETIME NULL,
  AuditUser NVARCHAR(50)
);

INSERT INTO dbo.TestMaster (Id, Name, Data, ModifiedDate, AuditUser)
VALUES (1, 'one', '<r>1</r>', GETDATE(), 'me'),
       (2, 'two', '<r>2</r>', GETDATE(), 'me');

-- Generate merge excluding AuditUser
EXEC dbo.usp_GenerateMergeScript 'dbo', 'TestMaster', 1, 'Audit%';

-- Copy output and inspect/execute on another database or after modifying source
*/
