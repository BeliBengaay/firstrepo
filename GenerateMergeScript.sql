SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('util.usp_GenerateMergeScript', 'P') IS NOT NULL
    DROP PROCEDURE util.usp_GenerateMergeScript;
GO

-- (Optional) Create a schema for utilities if not exists
IF SCHEMA_ID('util') IS NULL
    EXEC('CREATE SCHEMA util AUTHORIZATION dbo');
GO

CREATE PROCEDURE util.usp_GenerateMergeScript
(
    @SchemaName SYSNAME,
    @TableName SYSNAME,
    @IncludeDeletes BIT = 1,
    @ExcludeColumnPattern NVARCHAR(4000) = NULL  -- comma-separated patterns like 'Audit%,Modified%'
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @FullTableName NVARCHAR(512) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName),
        @ObjectId INT = OBJECT_ID(@FullTableName);

    IF @ObjectId IS NULL
    BEGIN
        DECLARE @msg NVARCHAR(4000) = 'Table ' + @FullTableName + ' does not exist or you do not have permission.';
        THROW 50001, @msg, 1;
    END

    -- Get PK columns
    DECLARE @PKCols TABLE (
        colname SYSNAME,
        key_ordinal INT
    );

    INSERT INTO @PKCols (colname, key_ordinal)
    SELECT c.name, ic.key_ordinal
    FROM sys.indexes i
    JOIN sys.index_columns ic
        ON i.object_id = ic.object_id 
        AND i.index_id = ic.index_id
    JOIN sys.columns c
        ON c.object_id = ic.object_id 
        AND c.column_id = ic.column_id
    WHERE i.is_primary_key = 1
      AND i.object_id = @ObjectId;

    IF NOT EXISTS (SELECT 1 FROM @PKCols)
    BEGIN
        THROW 50002, 'Table has no primary key. Cannot build MERGE ON clause.', 1;
    END

    -- Exclusion patterns parsing
    DECLARE @Exclude TABLE (pattern NVARCHAR(4000));
    IF @ExcludeColumnPattern IS NOT NULL
    BEGIN
        INSERT INTO @Exclude(pattern)
        SELECT LTRIM(RTRIM(value))
        FROM STRING_SPLIT(@ExcludeColumnPattern, ',')
        WHERE LTRIM(RTRIM(value)) <> '';
    END

    -- Gather columns metadata (non-computed)
    DECLARE @Cols TABLE (
        colname SYSNAME,
        column_id INT,
        is_pk BIT,
        system_type NVARCHAR(256),
        max_length INT
    );

    INSERT INTO @Cols (colname, column_id, is_pk, system_type, max_length)
    SELECT
        c.name,
        c.column_id,
        CASE WHEN pk.colname IS NOT NULL THEN 1 ELSE 0 END,
        TYPE_NAME(c.user_type_id) + 
            CASE 
                WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar', 'varbinary') 
                     THEN '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(
                            CASE 
                                WHEN TYPE_NAME(c.user_type_id) = 'nvarchar' THEN c.max_length/2 
                                ELSE c.max_length 
                            END AS VARCHAR(10)) END + ')'
                ELSE ''
            END,
        c.max_length
    FROM sys.columns c
    LEFT JOIN @PKCols pk
        ON pk.colname = c.name
    WHERE c.object_id = @ObjectId
      AND c.is_computed = 0;

    -- Remove excluded columns
    DELETE C
    FROM @Cols C
    WHERE EXISTS (
        SELECT 1
        FROM @Exclude E
        WHERE C.colname LIKE E.pattern
    );

    -- Build lists and clauses
    DECLARE 
        @AllCols NVARCHAR(MAX),
        @InsertCols NVARCHAR(MAX),
        @InsertVals NVARCHAR(MAX),
        @OnClause NVARCHAR(MAX),
        @UpdateSet NVARCHAR(MAX),
        @UpdateCondition NVARCHAR(MAX);

    -- All columns (for insert & source selection)
    SELECT @AllCols = STRING_AGG(QUOTENAME(colname), ', ')
    FROM @Cols;

    -- Insert column names / source references
    SELECT
        @InsertCols = STRING_AGG(QUOTENAME(colname), ', '),
        @InsertVals = STRING_AGG('S.' + QUOTENAME(colname), ', ')
    FROM @Cols;

    -- ON clause (PK equality)
    SELECT @OnClause = STRING_AGG('S.' + QUOTENAME(pk.colname) + ' = T.' + QUOTENAME(pk.colname), ' AND ')
    FROM @PKCols pk;

    -- UPDATE SET (only non-PK columns)
    SELECT @UpdateSet = STRING_AGG(QUOTENAME(colname) + ' = S.' + QUOTENAME(colname), ', ')
    FROM @Cols
    WHERE is_pk = 0;

    -- Build NULL-safe update condition (non-PK columns)
    DECLARE @cond NVARCHAR(MAX) = N'';
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT colname, system_type, max_length
        FROM @Cols
        WHERE is_pk = 0
        ORDER BY column_id;

    OPEN cur;
    DECLARE @col SYSNAME, @typ NVARCHAR(256); DECLARE @mlen INT;
    FETCH NEXT FROM cur INTO @col, @typ, @mlen;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @expr NVARCHAR(2000);

        IF @typ LIKE 'xml%' OR @typ LIKE 'text%' OR @typ LIKE 'ntext%'
        BEGIN
            SET @expr = N'(' +
                N'(S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR ' +
                N'(S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR ' +
                N'(CAST(S.' + QUOTENAME(@col) + ' AS NVARCHAR(MAX)) <> CAST(T.' + QUOTENAME(@col) + ' AS NVARCHAR(MAX)))' +
                N')';
        END
        ELSE IF @typ LIKE 'sql_variant%'
        BEGIN
            SET @expr = N'(' +
                N'(S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR ' +
                N'(S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR ' +
                N'(CHECKSUM(S.' + QUOTENAME(@col) + ') <> CHECKSUM(T.' + QUOTENAME(@col) + '))' +
                N')';
        END
        ELSE
        BEGIN
            SET @expr = N'(' +
                N'(S.' + QUOTENAME(@col) + ' IS NULL AND T.' + QUOTENAME(@col) + ' IS NOT NULL) OR ' +
                N'(S.' + QUOTENAME(@col) + ' IS NOT NULL AND T.' + QUOTENAME(@col) + ' IS NULL) OR ' +
                N'(S.' + QUOTENAME(@col) + ' <> T.' + QUOTENAME(@col) + ')' +
                N')';
        END

        IF LEN(@cond) = 0
            SET @cond = @expr;
        ELSE
            SET @cond = @cond + N' OR ' + @expr;

        FETCH NEXT FROM cur INTO @col, @typ, @mlen;
    END

    CLOSE cur;
    DEALLOCATE cur;

    SET @UpdateCondition = @cond;

    -- Safety checks: ensure non-empty lists
    IF @AllCols IS NULL OR @OnClause IS NULL OR @UpdateSet IS NULL
    BEGIN
        THROW 50003, 'Unable to construct column lists (maybe all columns excluded or only PKs exist).', 1;
    END

    -- Compose final MERGE statement
    DECLARE @MergeSQL NVARCHAR(MAX) = N'';

    SET @MergeSQL = 
        N'-- MERGE script generated by util.usp_GenerateMergeScript for ' + @FullTableName + CHAR(13) +
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

    SET @MergeSQL = @MergeSQL + N'-- End MERGE' + CHAR(13);

    -- Return as single-row result
    SELECT @MergeSQL AS MergeStatement;
END;
GO
