/*
    Unsupported options:
    * Filegroup types: Filestream, Memory-Optimized, Partitioned
    * Special index types: XML, HASH, Columnstore, Fulltext, Spatial
    * 2019+ new features - XML_COMPRESSION, OPTIMIZE_FOR_SEQUENTIAL_KEY, etc

    Considerations:
    * Add output message to ELSE for IF EXISTS option to indicate an action was skipped
    * If an index is disabled then generate create statement but within a multi-line comment
    * For Rebuild script, add option to check if disabled first? Or separate as an "EnableScript"
*/
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Options
DECLARE @ScriptExistsCheck bit     = 1,
        @EnableOnline      bit     = 0, --IIF(SERVERPROPERTY('EngineEdition') = 3, 1, 0),
        @BatchSeparator    bit     = 1,
        @FormatSQL         bit     = 1,
        @TrailingLineBreak bit     = 1,
        @AddOutputMessages bit     = 1,
        @MAXDOP            tinyint = 0; -- 0 = Default
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Other
DECLARE @crlf nchar(2) = NCHAR(13)+NCHAR(10),
        @tab  nchar(1) = NCHAR(9),
        @d    nchar(1) = NCHAR(9999), -- Delimeter to use for separating values in templates
        @q    nchar(1) = '''',        -- Single quote
        @qq   nchar(1) = '''''';      -- Double single quote
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Populate temp table with dependent infomation
------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#tmp_indexes', 'U') IS NOT NULL DROP TABLE #tmp_indexes; --SELECT * FROM #tmp_indexes
SELECT DatabaseName       = DB_NAME()
    , SchemaName          = SCHEMA_NAME(o.[schema_id])
    , ObjectName          = o.[name]
    , IndexName           = i.[name]
    , ObjectType          = o.[type_desc]
    , ObjectTypeCode      = o.[type] COLLATE DATABASE_DEFAULT
    , IndexType           = i.[type_desc] COLLATE DATABASE_DEFAULT
    , IsUnique            = i.is_unique
-- data_space_id
    , IgnoreDupKey        = i.[ignore_dup_key]
    , IsPrimaryKey        = i.is_primary_key
    , IsUniqueConstraint  = i.is_unique_constraint
    , [FillFactor]        = i.fill_factor
    , IsPadded            = i.is_padded
    , IsDisabled          = i.is_disabled
-- is_ignored_in_optimization
    , AllowRowLocks       = i.[allow_row_locks]
    , AllowPageLocks      = i.[allow_page_locks]
    , HasFilter           = i.has_filter
    , FilterDefinition    = i.filter_definition
-- compression_delay
-- suppress_dup_key_messages
    , StatNoRecompute     = st.no_recompute
    , StatIsIncremental   = st.is_incremental
    , DataCompressionType = p.[data_compression_desc] COLLATE DATABASE_DEFAULT
    , IndexFGName         = fg.[name]
    , IndexFGIsDefault    = fg.is_default
    , kc.KeyColsN, kc.KeyColsNQO, kc.InclColsNQ
INTO #tmp_indexes
FROM sys.indexes i
    JOIN sys.objects o ON o.[object_id] = i.[object_id]
    JOIN sys.stats st ON st.[object_id] = i.[object_id] AND st.stats_id = i.index_id
    JOIN sys.filegroups fg ON fg.data_space_id = i.data_space_id
    -- Disabled indexes do not have sys.partitions records
    LEFT HASH JOIN sys.partitions p ON p.[object_id] = i.[object_id] AND p.index_id = i.index_id
    JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
    CROSS APPLY (
        SELECT KeyColsN      = STRING_AGG(IIF(ic.is_included_column = 0, n.ColName          , NULL), @d) WITHIN GROUP (ORDER BY ic.key_ordinal, n.ColName)
            ,  KeyColsNQO    = STRING_AGG(IIF(ic.is_included_column = 0, t.ColNameQuoteOrder, NULL), @d) WITHIN GROUP (ORDER BY ic.key_ordinal, n.ColName)
            ,  InclColsNQ    = STRING_AGG(IIF(ic.is_included_column = 1, q.ColNameQuote     , NULL), @d) WITHIN GROUP (ORDER BY ic.key_ordinal, n.ColName)
        FROM sys.index_columns ic
            CROSS APPLY (SELECT ColName = COL_NAME(ic.[object_id], ic.column_id)) n
            CROSS APPLY (SELECT ColNameQuote = QUOTENAME(n.ColName)) q
            CROSS APPLY (SELECT ColNameQuoteOrder = CONCAT_WS(' ', q.ColNameQuote, IIF(ic.is_descending_key = 1, 'DESC', NULL))) t
        WHERE ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    ) kc
WHERE i.[type] > 0 -- Exclude heaps
    AND o.[type] IN ('U','V') -- Tables and views only - exclude functions/table types
    AND o.is_ms_shipped = 0
    -- Support limitations -->
    AND fg.[type] = 'FG'; -- FD (FILESTREAM), FX (Memory-Optimized), PS (Partition Scheme), etc - not supported
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Templates
DECLARE @SqlDrop     nvarchar(4000) = 'DROP INDEX IF EXISTS {{Index}} ON {{Schema}}.{{Object}};',
        @SqlRebuild  nvarchar(4000) = 'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} REBUILD',
        @SqlDisable  nvarchar(4000) = 'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} DISABLE;',
        @SqlDropPKUQ nvarchar(4000) = 'ALTER TABLE {{Schema}}.{{Object}} DROP CONSTRAINT {{Index}};';

IF OBJECT_ID('tempdb..#output', 'U') IS NOT NULL DROP TABLE #output; --SELECT * FROM #output
SELECT i.SchemaName, i.ObjectName, i.IndexName, i.ObjectType, i.ObjectTypeCode, i.IndexType, i.IsPrimaryKey, i.IsUniqueConstraint, i.IsDisabled, i.HasFilter
    , KeyCols              = REPLACE(i.KeyColsNQO, @d, ', ')
    , InclCols             = REPLACE(i.InclColsNQ, @d, ', ')
    , SuggestedName        = c.SuggestedName
    , MatchesSuggestedName = CONVERT(bit, IIF(i.IndexName = c.SuggestedName, 1, 0))
    , FQIN                 = CONCAT_WS('.', q.DatabaseName, q.SchemaName, q.ObjectName, q.IndexName)
    , CreateOn             = IIF(x.IsConstraint = 1, c.AlterTable+' '+c.AddConstraint, c.CreateBase+' '+c.OnObject)
    , CreateScript         = CONCAT_WS(@d,
								IIF(x.IsConstraint = 1, c.AlterTable+@d+c.AddConstraint, c.CreateBase+@d+c.OnObject)+' '+c.KeyCols
								, c.InclCols, c.Filtered, c.Options, c.FG
							 ) + ';'
    , DropScript           = IIF(x.IsConstraint = 1, s.DropPKUQScript, s.DropScript)
    , RebuildScript        = IIF(x.IsConstraint = 1, NULL, s.RebuildScript+' '+c.RebuildOptions+';')
    , DisableScript        = IIF(x.IsConstraint = 1, NULL, s.DisableScript)
	, c.Filtered
INTO #output
FROM #tmp_indexes i
    CROSS APPLY (
        SELECT DatabaseName  = QUOTENAME(i.DatabaseName)
            ,  SchemaName    = QUOTENAME(i.SchemaName)
            ,  ObjectName    = QUOTENAME(i.ObjectName)
            ,  IndexName     = QUOTENAME(i.IndexName)
            ,  IndexFGName   = QUOTENAME(i.IndexFGName)
    ) q
    -- Create the base scripts for each section
    CROSS APPLY (
        SELECT DropScript     = REPLACE(REPLACE(REPLACE(@SqlDrop    , '{{Schema}}', q.SchemaName), '{{Object}}', q.ObjectName), '{{Index}}', q.IndexName)
            ,  RebuildScript  = REPLACE(REPLACE(REPLACE(@SqlRebuild , '{{Schema}}', q.SchemaName), '{{Object}}', q.ObjectName), '{{Index}}', q.IndexName)
            ,  DisableScript  = REPLACE(REPLACE(REPLACE(@SqlDisable , '{{Schema}}', q.SchemaName), '{{Object}}', q.ObjectName), '{{Index}}', q.IndexName)
            ,  DropPKUQScript = REPLACE(REPLACE(REPLACE(@SqlDropPKUQ, '{{Schema}}', q.SchemaName), '{{Object}}', q.ObjectName), '{{Index}}', q.IndexName)
    ) s
    CROSS APPLY (
        SELECT CreateOptions = STRING_AGG(IIF(opt.IsBuildOption = 0, opt.n+' = '+opt.v, NULL), ', ')
            ,  BuildOptions  = STRING_AGG(IIF(opt.IsBuildOption = 1, opt.n+' = '+opt.v, NULL), ', ')
        FROM ( -- Default values that want to be excluded should return NULL
            VALUES 
                -- Index settings/configuration
                  (0, 'PAD_INDEX'                  , IIF(i.IsPadded = 1            , 'ON',   NULL))
                , (0, 'FILLFACTOR'                 , IIF(i.[FillFactor] NOT IN (0,100), CONVERT(nvarchar(3), i.[FillFactor]), NULL)) -- 0 means to use the default, which is 100
                , (0, 'IGNORE_DUP_KEY'             , IIF(i.IgnoreDupKey = 1        , 'ON',   NULL))
                , (0, 'STATISTICS_NORECOMPUTE'     , IIF(i.StatNoRecompute = 1     , 'ON',   NULL))
                , (0, 'STATISTICS_INCREMENTAL'     , IIF(i.StatIsIncremental = 1   , 'ON',   NULL))
                , (0, 'ALLOW_ROW_LOCKS'            , IIF(i.AllowRowLocks = 1       , NULL , 'OFF'))
                , (0, 'ALLOW_PAGE_LOCKS'           , IIF(i.AllowPageLocks = 1      , NULL , 'OFF'))
                , (1, 'OPTIMIZE_FOR_SEQUENTIAL_KEY', NULL) -- SQL Server 2019+ feature
                , (0, 'DATA_COMPRESSION'           , NULLIF(i.DataCompressionType, 'NONE')) -- Only PAGE/ROW/NONE supported - Partitioning not supported
                , (0, 'XML_COMPRESSION'            , NULL) -- SQL Server 2022+ feature
                -- Create options
                , (1, 'SORT_IN_TEMPDB'             , NULL)
                , (1, 'DROP_EXISTING'              , NULL)
                , (1, 'ONLINE'                     , IIF(@EnableOnline = 1         , 'ON',   NULL)) -- 3 = Eval/Dev/Enterprise - Only ON/OFF supported for now
                , (1, 'RESUMABLE'                  , NULL)
                , (1, 'MAX_DURATION'               , NULL)
                , (1, 'MAXDOP'                     , CONVERT(nvarchar(3), NULLIF(@MAXDOP, 0)))
        ) opt(IsBuildOption,n,v)
        WHERE opt.v IS NOT NULL -- Exclude default values
    ) o
    CROSS APPLY (
        SELECT IsConstraint       = IIF(i.IsPrimaryKey = 1 OR i.IsUniqueConstraint = 1, 1, 0)
			,  ConstraintType     = CASE WHEN i.IsPrimaryKey = 1 THEN 'PRIMARY KEY' WHEN i.IsUniqueConstraint = 1 THEN 'UNIQUE' ELSE NULL END
            ,  ConstraintTypeCode = CASE WHEN i.IsPrimaryKey = 1 THEN 'PK' WHEN i.IsUniqueConstraint = 1 THEN 'UQ' ELSE 'IX' END
			,  IndexType          = CONCAT_WS(' ', IIF(i.IsUnique = 1, 'UNIQUE', NULL), i.IndexType)
    ) x
    CROSS APPLY ( -- Optional parts should return NULL
        SELECT CreateBase     = 'CREATE '+x.IndexType+' INDEX '+q.IndexName                               -- CREATE UNIQUE NONCLUSTERED INDEX [IX_TableName]
            ,  OnObject       = 'ON '+q.SchemaName+'.'+q.ObjectName                                       -- ON [dbo].[TableName]
            ,  KeyCols        = '('+REPLACE(i.KeyColsNQO, @d, ', ')+')'                                   -- ([KeyCol1], [KeyCol2], [KeyCol3])
            --
            ,  AlterTable     = 'ALTER TABLE '+q.SchemaName+'.'+q.ObjectName                              -- ALTER TABLE [dbo].[TableName]
            ,  AddConstraint  = 'ADD CONSTRAINT '+q.IndexName+' '+x.ConstraintType                        -- ADD CONSTRAINT [PK_ConstraintName]
            -- Optional parts
            ,  InclCols       = 'INCLUDE ('+REPLACE(i.InclColsNQ, @d, ', ')+')'                           -- INCLUDE ([ColA], [ColB], [ColC])
            ,  Filtered       = 'WHERE '+i.FilterDefinition                                               -- WHERE ([ColA] = 123)
            ,  Options        = 'WITH ('+NULLIF(CONCAT_WS(', ', o.CreateOptions, o.BuildOptions), '')+')' -- WITH (PAD_INDEX=ON, FILLFACTOR=85, ONLINE=ON)
            ,  FG             = 'ON '+IIF(i.IndexFGIsDefault = 0, QUOTENAME(i.IndexFGName), NULL)         -- ON [Secondary]
            -- Other
            ,  RebuildOptions = 'WITH ('+o.BuildOptions+')'
            ,  SuggestedName  = LEFT(CONCAT_WS('_', x.ConstraintTypeCode, i.ObjectName, REPLACE(i.KeyColsN, @d, '_')), 128)
    ) c;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@FormatSQL = 1)
BEGIN;
    UPDATE o
    SET o.CreateScript = REPLACE(o.CreateScript, @d, @crlf + @tab)
    FROM #output o;
END;
ELSE
BEGIN;
    UPDATE o
    SET o.CreateScript = REPLACE(o.CreateScript, @d, ' ')
    FROM #output o;
END;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@AddOutputMessages = 1)
BEGIN;
    DECLARE @SqlOutputMessage nvarchar(4000) = 'RAISERROR(''Execute: {{Message}}'',0,1) WITH NOWAIT;';
    UPDATE o
    SET o.CreateScript    = REPLACE(@SqlOutputMessage, '{{Message}}', REPLACE(o.CreateOn     , @q, @qq)) + @crlf + o.CreateScript,
        o.DropScript      = REPLACE(@SqlOutputMessage, '{{Message}}', REPLACE(o.DropScript   , @q, @qq)) + @crlf + o.DropScript,
        o.RebuildScript   = REPLACE(@SqlOutputMessage, '{{Message}}', REPLACE(o.RebuildScript, @q, @qq)) + @crlf + o.RebuildScript,
        o.DisableScript   = REPLACE(@SqlOutputMessage, '{{Message}}', REPLACE(o.DisableScript, @q, @qq)) + @crlf + o.DisableScript
    FROM #Output o;
END;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@ScriptExistsCheck = 1)
BEGIN;
    DECLARE @SqlIfNotExists nvarchar(4000) = 'IF ((OBJECT_ID(N''{{Schema}}.{{Object}}'', ''{{ObjectTypeCode}}'') IS NOT NULL)' + @crlf
                                            + @tab + 'AND (INDEXPROPERTY(OBJECT_ID(N''{{Schema}}.{{Object}}''), N''{{Index}}'', ''IndexId'') IS NULL))' + @crlf
                                            + 'BEGIN;' + @crlf
                                            + @tab + '{{Script}}' + @crlf
                                            + 'END;',
            @SqlIfExists    nvarchar(4000) = 'IF (INDEXPROPERTY(OBJECT_ID(N''{{Schema}}.{{Object}}''), N''{{Index}}'', ''IndexId'') IS NOT NULL)' + @crlf
                                            + 'BEGIN;' + @crlf
                                            + @tab + '{{Script}}' + @crlf
                                            + 'END;';

    UPDATE o
    SET o.CreateScript  = REPLACE(x.IfNotExists, '{{Script}}', REPLACE(o.CreateScript , @crlf, @crlf + @tab)),
        o.DropScript    = REPLACE(x.IfExists   , '{{Script}}', REPLACE(o.DropScript   , @crlf, @crlf + @tab)),
        o.RebuildScript = REPLACE(x.IfExists   , '{{Script}}', REPLACE(o.RebuildScript, @crlf, @crlf + @tab)),
        o.DisableScript = REPLACE(x.IfExists   , '{{Script}}', REPLACE(o.DisableScript, @crlf, @crlf + @tab))
    FROM #output o
        CROSS APPLY (SELECT SchemaName = QUOTENAME(o.SchemaName), ObjectName = QUOTENAME(o.ObjectName)) q
        CROSS APPLY (
            SELECT IfNotExists = REPLACE(REPLACE(REPLACE(REPLACE(@SqlIfNotExists,'{{Schema}}',q.SchemaName),'{{Object}}',q.ObjectName),'{{Index}}',o.IndexName),'{{ObjectTypeCode}}',o.ObjectTypeCode)
                ,  IfExists    = REPLACE(REPLACE(REPLACE(REPLACE(@SqlIfExists   ,'{{Schema}}',q.SchemaName),'{{Object}}',q.ObjectName),'{{Index}}',o.IndexName),'{{ObjectTypeCode}}',o.ObjectTypeCode)
        ) x;
END;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@BatchSeparator = 1 OR @TrailingLineBreak = 1)
BEGIN;
    UPDATE o
    SET o.CreateScript  = o.CreateScript  + x.Sep,
        o.DropScript    = o.DropScript    + x.Sep,
        o.RebuildScript = o.RebuildScript + x.Sep,
        o.DisableScript = o.DisableScript + x.Sep
    FROM #output o
        CROSS APPLY (
            SELECT Sep = CONCAT(IIF(@BatchSeparator = 1, @crlf + 'GO', NULL), IIF(@TrailingLineBreak = 1, @crlf + @crlf, NULL))
        ) x;
END;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
SELECT *
FROM #output i
ORDER BY i.SchemaName, i.ObjectName, i.IndexName;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
