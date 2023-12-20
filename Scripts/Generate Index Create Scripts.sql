/*
    Considerations:
    * Add output message to ELSE for IF EXISTS option to indicate an action was skipped
    * If an index is disabled then generate create statement but within a multi-line comment
    * For Rebuild script, add option to check if disabled first? Or separate as an "EnableScript"
    * Move variables into CROSS APPLY or CTE so that entire script can be converted to a view or TVF
*/
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Populate temp table with dependent infomation
------------------------------------------------------------------------------
IF OBJECT_ID(N'tempdb..#tmp_indexes',N'U') IS NOT NULL DROP TABLE #tmp_indexes; --SELECT * FROM #tmp_indexes
SELECT ObjectID             = i.[object_id]
    , IndexID               = i.index_id
    , SchemaName            = SCHEMA_NAME(o.[schema_id])
    , ObjectName            = o.[name]
    , IndexName             = i.[name]
    , ObjectType            = o.[type_desc]
    , IndexType             = i.[type_desc] COLLATE DATABASE_DEFAULT
    , IsUnique              = i.is_unique
    , IgnoreDupKey          = i.[ignore_dup_key]
    , [FillFactor]          = i.fill_factor
    , IsPadded              = i.is_padded
    , IsDisabled            = i.is_disabled
    , AllowRowLocks         = i.[allow_row_locks]
    , AllowPageLocks        = i.[allow_page_locks]
    , HasFilter             = i.has_filter
    , FilterDefinition      = i.filter_definition
    , StatNoRecompute       = st.no_recompute
    , StatIsIncremental     = st.is_incremental
    , DataCompressionType   = p.[data_compression_desc] COLLATE DATABASE_DEFAULT
    , IndexFGName           = FILEGROUP_NAME(i.data_space_id)
    , IndexFGIsDefault      = FILEGROUPPROPERTY(FILEGROUP_NAME(i.data_space_id), 'IsDefault')
    , kc.KeyColsN, kc.KeyColsNQO, kc.InclColsNQ
INTO #tmp_indexes
FROM sys.indexes i
    JOIN sys.objects o ON o.[object_id] = i.[object_id]
    JOIN sys.stats st ON st.[object_id] = i.[object_id] AND st.stats_id = i.index_id
    -- Disabled indexes do not have sys.partitions records
    LEFT HASH JOIN sys.partitions p ON p.[object_id] = i.[object_id] AND p.index_id = i.index_id AND p.partition_number = 1 -- Partitioning not yet supported
    JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
    CROSS APPLY (
        SELECT KeyColsN      = STRING_AGG(IIF(ic.is_included_column = 0, n.ColName          , NULL), N'{{delim}}') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id)
            ,  KeyColsNQO    = STRING_AGG(IIF(ic.is_included_column = 0, t.ColNameQuoteOrder, NULL), N'{{delim}}') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id)
            ,  InclColsNQ    = STRING_AGG(IIF(ic.is_included_column = 1, q.ColNameQuote     , NULL), N'{{delim}}') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id)
        FROM sys.index_columns ic
            CROSS APPLY (SELECT ColName = COL_NAME(ic.[object_id], ic.column_id)) n
            CROSS APPLY (SELECT ColNameQuote = QUOTENAME(n.ColName)) q
            CROSS APPLY (SELECT ColNameQuoteOrder = CONCAT_WS(N' ', q.ColNameQuote, IIF(ic.is_descending_key = 1, N'DESC', NULL))) t
        WHERE ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    ) kc
WHERE i.[type] > 0 -- Exclude heaps
    AND o.[type] IN ('U','V') -- Tables and views only - exclude functions/table types
    AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 -- PK's and Unique constraints have their own syntax
    AND o.is_ms_shipped = 0
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Options
DECLARE @ScriptIfNotExists  bit            = 1,
        @ScriptIfExists     bit            = 1,
        @EnableOnline       bit            = IIF(SERVERPROPERTY('EngineEdition') = 3, 1, 0),
        @BatchSeparator     bit            = 1,
        @FormatSQL          bit            = 1,
        @TrailingLineBreak  bit            = 1,
        @AddOutputMessages  bit            = 1,
        @MAXDOP             tinyint        = 0; -- 0 = Default
-- Other
DECLARE @crlf               nchar(2)       = NCHAR(13)+NCHAR(10),
        @tab                nchar(1)       = NCHAR(9);
-- Templates
DECLARE @SqlIfNotExists     nvarchar(4000) = N'IF (INDEXPROPERTY(OBJECT_ID(N''{{Schema}}.{{Object}}''), N''{{Index}}'', ''IndexId'') IS NULL)',
        @SqlIfExists        nvarchar(4000) = N'IF (INDEXPROPERTY(OBJECT_ID(N''{{Schema}}.{{Object}}''), N''{{Index}}'', ''IndexId'') IS NOT NULL)',
        @SqlDrop            nvarchar(4000) = N'DROP INDEX IF EXISTS {{Index}} ON {{Schema}}.{{Object}};',
        @SqlRebuild         nvarchar(4000) = N'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} REBUILD',
        @SqlDisable         nvarchar(4000) = N'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} DISABLE;',
        @SqlOutputMessage   nvarchar(4000) = N'RAISERROR(''Execute: {{Message}}'',0,1) WITH NOWAIT;',
        @SqlErrorMessage    nvarchar(4000) = N'RAISERROR(''ERROR: {{Message}}'',11,1) WITH NOWAIT;';
------------------------------------------------------------------------------

------------------------------------------------------------------------------
SELECT i.SchemaName, i.ObjectName, i.IndexName, i.ObjectType, i.IndexType, i.IsDisabled, i.HasFilter
    , KeyCols              = REPLACE(i.KeyColsNQO , N'{{delim}}',N', ')
	, InclCols             = REPLACE(i.InclColsNQ, N'{{delim}}',N', ')
    , SuggestedName        = c.SuggestedName
	, MatchesSuggestedName = CONVERT(bit, IIF(i.IndexName = c.SuggestedName, 1, 0))
    , CreateScript         = REPLACE(REPLACE(y.IfNotExists, N'{{Message}}', REPLACE(c.CreateBase   ,N'''',N'''''')), N'{{Script}}', z.CompleteCreate)
    , DropScript           = REPLACE(REPLACE(y.IfExists   , N'{{Message}}', REPLACE(s.DropScript   ,N'''',N'''''')), N'{{Script}}', s.DropScript)
    , RebuildScript        = REPLACE(REPLACE(y.IfExists   , N'{{Message}}', REPLACE(s.RebuildScript,N'''',N'''''')), N'{{Script}}', CONCAT_WS(N' ', s.RebuildScript, c.BuildOptions) + N';')
    , DisableScript        = REPLACE(REPLACE(y.IfExists   , N'{{Message}}', REPLACE(s.DisableScript,N'''',N'''''')), N'{{Script}}', s.DisableScript)
    , VerifyDrop           = s.IfExists + @crlf + N'BEGIN;' + @crlf + @tab + REPLACE(@SqlErrorMessage , N'{{Message}}', REPLACE(s.DropScript   ,N'''',N'''''')) + @crlf + N'END;' + c.BatchSeparator
FROM #tmp_indexes i
    CROSS APPLY (SELECT SchemaName = QUOTENAME(i.SchemaName), ObjectName = QUOTENAME(i.ObjectName), IndexName = QUOTENAME(i.IndexName)) qn
    -- Create the base scripts for each section
    CROSS APPLY (
        SELECT IfNotExists   = REPLACE(REPLACE(REPLACE(@SqlIfNotExists, N'{{Schema}}', qn.SchemaName), N'{{Object}}', qn.ObjectName), N'{{Index}}', i.IndexName)
            ,  IfExists      = REPLACE(REPLACE(REPLACE(@SqlIfExists   , N'{{Schema}}', qn.SchemaName), N'{{Object}}', qn.ObjectName), N'{{Index}}', i.IndexName)
            ,  DisableScript = REPLACE(REPLACE(REPLACE(@SqlDisable    , N'{{Schema}}', qn.SchemaName), N'{{Object}}', qn.ObjectName), N'{{Index}}', qn.IndexName)
            ,  RebuildScript = REPLACE(REPLACE(REPLACE(@SqlRebuild    , N'{{Schema}}', qn.SchemaName), N'{{Object}}', qn.ObjectName), N'{{Index}}', qn.IndexName)
            ,  DropScript    = REPLACE(REPLACE(REPLACE(@SqlDrop       , N'{{Schema}}', qn.SchemaName), N'{{Object}}', qn.ObjectName), N'{{Index}}', qn.IndexName)
    ) s
    CROSS APPLY (
        SELECT CreateOptions = STRING_AGG(IIF(opt.IsBuildOption = 0, CONCAT(opt.n, N'=', opt.v), NULL), N', ')
            ,  BuildOptions  = STRING_AGG(IIF(opt.IsBuildOption = 1, CONCAT(opt.n, N'=', opt.v), NULL), N', ')
        FROM (
            VALUES (0, N'PAD_INDEX'             , IIF(i.IsPadded = 1            , N'ON',   NULL))
                ,  (0, N'FILLFACTOR'            , CONVERT(nvarchar(3), NULLIF(i.[FillFactor], 0)))
                ,  (0, N'IGNORE_DUP_KEY'        , IIF(i.IgnoreDupKey = 1        , N'ON',   NULL))
                ,  (0, N'STATISTICS_NORECOMPUTE', IIF(i.StatNoRecompute = 1     , N'ON',   NULL))
                ,  (0, N'STATISTICS_INCREMENTAL', IIF(i.StatIsIncremental = 1   , N'ON',   NULL))
                ,  (0, N'ALLOW_ROW_LOCKS'       , IIF(i.AllowRowLocks = 1       , NULL , N'OFF'))
                ,  (0, N'ALLOW_PAGE_LOCKS'      , IIF(i.AllowPageLocks = 1      , NULL , N'OFF'))
                ,  (0, N'DATA_COMPRESSION'      , NULLIF(i.DataCompressionType, N'NONE')) -- Only works for non-partitioned tables
                ,  (0, N'XML_COMPRESSION'       , NULL) -- Haven't figured it out yet
                -- Create options
                ,  (1, N'ONLINE'                , IIF(@EnableOnline = 1         , N'ON',   NULL)) -- 3 = Eval/Dev/Enterprise
                ,  (1, N'MAXDOP'                , CONVERT(nvarchar(3), NULLIF(@MAXDOP, 0)))
        ) opt(IsBuildOption, n,v)
        WHERE opt.v IS NOT NULL -- Exclude default values
    ) x
    CROSS APPLY (
        SELECT CreateBase     = CONCAT_WS(N' ', N'CREATE', IIF(i.IsUnique = 1, N'UNIQUE', NULL), i.IndexType, N'INDEX', qn.IndexName) -- CREATE UNIQUE INDEX [IX_TableName]
            ,  CreateOn       = CONCAT_WS(N' ', N'ON', qn.SchemaName+N'.'+qn.ObjectName)                                              -- ON [dbo].[TableName]
            ,  Cols           = N'('+REPLACE(i.KeyColsNQO,N'{{delim}}',N', ')+N')'                                                    -- ([KeyCol1], [KeyCol2], [KeyCol3])
            ,  InclCols       = N'INCLUDE ('+REPLACE(i.InclColsNQ,N'{{delim}}',N', ')+N')'                                            -- INCLUDE ([ColA], [ColB], [ColC])
            ,  Filtered       = N'WHERE '+i.FilterDefinition                                                                          -- WHERE ([ColA] = 123)
            ,  CreateOptions  = N'WITH ('+NULLIF(CONCAT_WS(N', ', x.CreateOptions, x.BuildOptions),N'')+N')'                          -- WITH (PAD_INDEX=ON, FILLFACTOR=85, ONLINE=ON)
            ,  DataSpace      = N'ON '+IIF(i.IndexFGIsDefault = 0, QUOTENAME(i.IndexFGName), NULL)                                    -- ON [Secondary]

            ,  BuildOptions   = N'WITH ('+x.BuildOptions+N')'
            ,  BatchSeparator = IIF(@BatchSeparator = 1, @crlf + N'GO', N'') + IIF(@TrailingLineBreak = 1, @crlf+@crlf, N'')
            ,  SuggestedName  = LEFT(CONCAT(N'IX_', i.ObjectName, N'_', REPLACE(i.KeyColsN,N'{{delim}}',N'_')), 128)
    ) c
    CROSS APPLY (
        SELECT IfExists    = IIF(@ScriptIfExists    = 1, s.IfExists    + @crlf + N'BEGIN;' + @crlf, N'')
                           + IIF(@AddOutputMessages = 1, IIF(@ScriptIfExists    = 1, @tab, N'') + @SqlOutputMessage + @crlf, N'')
                           + IIF(@ScriptIfExists    = 1, @tab, N'') + N'{{Script}}'
                           + IIF(@ScriptIfExists    = 1, @crlf + N'END;', N'')
                           + c.BatchSeparator

            ,  IfNotExists = IIF(@ScriptIfNotExists = 1, s.IfNotExists + @crlf + N'BEGIN;' + @crlf, N'')
                           + IIF(@AddOutputMessages = 1, IIF(@ScriptIfNotExists = 1, @tab, N'') + @SqlOutputMessage + @crlf, N'')
                           + IIF(@ScriptIfNotExists = 1, @tab, N'') + N'{{Script}}'
                           + IIF(@ScriptIfNotExists = 1, @crlf + N'END;', N'')
                           + c.BatchSeparator
    ) y
    CROSS APPLY (
        SELECT CompleteCreate = CONCAT(c.CreateBase
                                , IIF(@FormatSQL = 1, @crlf + IIF(@ScriptIfNotExists = 1, @tab+@tab, @tab), N' ') + c.CreateOn , N' ' , c.Cols
                                , IIF(@FormatSQL = 1, @crlf + IIF(@ScriptIfNotExists = 1, @tab+@tab, @tab), N' ') + c.InclCols
                                , IIF(@FormatSQL = 1, @crlf + IIF(@ScriptIfNotExists = 1, @tab+@tab, @tab), N' ') + c.Filtered
                                , IIF(@FormatSQL = 1, @crlf + IIF(@ScriptIfNotExists = 1, @tab+@tab, @tab), N' ') + c.CreateOptions
                                , IIF(@FormatSQL = 1, @crlf + IIF(@ScriptIfNotExists = 1, @tab+@tab, @tab), N' ') + c.DataSpace
                                , N';')
    ) z
ORDER BY i.SchemaName, i.ObjectName, i.IndexName;