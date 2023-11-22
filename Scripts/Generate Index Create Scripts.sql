/*
    Considerations:
    * Add output message to ELSE for IF EXISTS option to indicate an action was skipped
    * If an index is disabled then generate create statement but within a multi-line comment
    * For Rebuild script, add option to check if disabled first? Or separate as an "EnableScript"
    * Move variables into CROSS APPLY or CTE so that entire script can be converted to a view or TVF
*/

-- Options
DECLARE @ScriptIfNotExists  bit           = 1,
        @ScriptIfExists     bit           = 1,
        @EnableOnline       bit           = IIF(SERVERPROPERTY('EngineEdition') = 3, 1, 0),
        @BatchSeparator     bit           = 0,
        @TrailingLineBreak  bit           = 1,
        @AddOutputMessages  bit           = 1,
        @MAXDOP             tinyint       = 0; -- 0 = Default
-- Other
DECLARE @crlf               char(2)       = CHAR(13)+CHAR(10),
        @tab                char(1)       = CHAR(9);
-- Templates
DECLARE @SqlIfNotExists     nvarchar(MAX) = N'IF (INDEXPROPERTY(OBJECT_ID(''{{Schema}}.{{Object}}''), ''{{Index}}'', ''IndexId'') IS NULL)',
        @SqlIfExists        nvarchar(MAX) = N'IF (INDEXPROPERTY(OBJECT_ID(''{{Schema}}.{{Object}}''), ''{{Index}}'', ''IndexId'') IS NOT NULL)',
        @SqlDrop            nvarchar(MAX) = N'DROP INDEX IF EXISTS {{Index}} ON {{Schema}}.{{Object}};',
        @SqlRebuild         nvarchar(MAX) = N'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} REBUILD',
        @SqlDisable         nvarchar(MAX) = N'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} DISABLE;',
        @SqlOutputMessage   nvarchar(MAX) = N'RAISERROR(''Execute: {{Message}}'',0,1) WITH NOWAIT;',
        @SqlErrorMessage    nvarchar(MAX) = N'RAISERROR(''ERROR: {{Message}}'',11,1) WITH NOWAIT;';

SELECT n.SchemaName, n.ObjectName, n.IndexName, i.is_disabled
    , SuggestedName = CONCAT('IX_', n.ObjectName, '_', kc.KeyColsName)
    , CreateScript  = REPLACE(REPLACE(y.IfNotExists, '{{Message}}', REPLACE(c.CreateBase      ,'''','''''')), '{{Script}}', CONCAT_WS(' ', c.CreateBase, c.Cols, c.Features) + ';')
    , DropScript    = REPLACE(REPLACE(y.IfExists   , '{{Message}}', REPLACE(s.DropScript      ,'''','''''')), '{{Script}}', s.DropScript)
    , RebuildScript = REPLACE(REPLACE(y.IfExists   , '{{Message}}', REPLACE(s.RebuildScript   ,'''','''''')), '{{Script}}', CONCAT_WS(' ', s.RebuildScript, c.BuildOptions) + ';')
    , DisableScript = REPLACE(REPLACE(y.IfExists   , '{{Message}}', REPLACE(s.DisableScript   ,'''','''''')), '{{Script}}', s.DisableScript)
    , VerifyDrop    = s.IfExists + @crlf + 'BEGIN;' + @crlf + @tab + REPLACE(@SqlErrorMessage , '{{Message}}', REPLACE(s.DropScript   ,'''','''''')) + @crlf + 'END;' + c.BatchSeparator
FROM sys.indexes i
    JOIN sys.objects o ON o.[object_id] = i.[object_id]
    JOIN sys.stats st ON st.[object_id] = i.[object_id] AND st.stats_id = i.index_id
    -- Disabled indexes do not have sys.partitions records
    LEFT HASH JOIN sys.partitions p ON p.[object_id] = i.[object_id] AND p.index_id = i.index_id AND p.partition_number = 1 -- Partitioning not yet supported
    JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
    CROSS APPLY (SELECT SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]) n
    CROSS APPLY (SELECT SchemaName = QUOTENAME(n.SchemaName), ObjectName = QUOTENAME(n.ObjectName), IndexName = QUOTENAME(n.IndexName)) qn
    -- Create the base scripts for each section
    CROSS APPLY (
        SELECT IfNotExists   = REPLACE(REPLACE(REPLACE(@SqlIfNotExists, '{{Schema}}', qn.SchemaName), '{{Object}}', qn.ObjectName), '{{Index}}', n.IndexName)
            ,  IfExists      = REPLACE(REPLACE(REPLACE(@SqlIfExists   , '{{Schema}}', qn.SchemaName), '{{Object}}', qn.ObjectName), '{{Index}}', n.IndexName)
            ,  DisableScript = REPLACE(REPLACE(REPLACE(@SqlDisable    , '{{Schema}}', qn.SchemaName), '{{Object}}', qn.ObjectName), '{{Index}}', qn.IndexName)
            ,  RebuildScript = REPLACE(REPLACE(REPLACE(@SqlRebuild    , '{{Schema}}', qn.SchemaName), '{{Object}}', qn.ObjectName), '{{Index}}', qn.IndexName)
            ,  DropScript    = REPLACE(REPLACE(REPLACE(@SqlDrop       , '{{Schema}}', qn.SchemaName), '{{Object}}', qn.ObjectName), '{{Index}}', qn.IndexName)
    ) s
    CROSS APPLY (
        SELECT KeyCols       = STRING_AGG(IIF(ic.is_included_column = 0, t.ColText, NULL), ', ') WITHIN GROUP (ORDER BY ic.index_column_id)
            ,  KeyColsName   = STRING_AGG(IIF(ic.is_included_column = 0, n.ColName, NULL), '_')	 WITHIN GROUP (ORDER BY ic.index_column_id)
            ,  InclCols      = STRING_AGG(IIF(ic.is_included_column = 1, t.ColText, NULL), ', ') WITHIN GROUP (ORDER BY ic.index_column_id)
        FROM sys.index_columns ic
            CROSS APPLY (SELECT ColName = COL_NAME(ic.[object_id], ic.column_id)) n
            CROSS APPLY (SELECT ColText = CONCAT_WS(' ', QUOTENAME(n.ColName), IIF(ic.is_descending_key = 1, 'DESC', NULL))) t
        WHERE ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    ) kc
    CROSS APPLY (
        SELECT CreateOptions = STRING_AGG(IIF(opt.IsBuildOption = 0, CONCAT(opt.n, '=', opt.v), NULL), ', ')
            ,  BuildOptions  = STRING_AGG(IIF(opt.IsBuildOption = 1, CONCAT(opt.n, '=', opt.v), NULL), ', ')
        FROM (
            VALUES (0, 'PAD_INDEX'             , IIF(i.is_padded = 1           , 'ON', NULL))
                ,  (0, 'FILLFACTOR'            , CONVERT(varchar(3), NULLIF(i.fill_factor, 0)))
                ,  (0, 'IGNORE_DUP_KEY'        , IIF(i.[ignore_dup_key] = 1    , 'ON', NULL))
                ,  (0, 'STATISTICS_NORECOMPUTE', IIF(st.no_recompute = 1       , 'ON', NULL))
                ,  (0, 'STATISTICS_INCREMENTAL', IIF(st.is_incremental = 1     , 'ON', NULL))
                ,  (0, 'ALLOW_ROW_LOCKS'       , IIF(i.[allow_row_locks] = 1   , NULL, 'OFF'))
                ,  (0, 'ALLOW_PAGE_LOCKS'      , IIF(i.[allow_page_locks] = 1  , NULL, 'OFF'))
                ,  (0, 'DATA_COMPRESSION'      , NULLIF(p.[data_compression_desc] COLLATE DATABASE_DEFAULT, 'NONE')) -- Only works for non-partitioned tables
                ,  (0, 'XML_COMPRESSION'       , NULL) -- Haven't figured it out yet
                -- Create options
                ,  (1, 'ONLINE'                , IIF(@EnableOnline = 1         , 'ON', NULL)) -- 3 = Eval/Dev/Enterprise
                ,  (1, 'MAXDOP'                , CONVERT(varchar(3), NULLIF(@MAXDOP, 0)))
        ) opt(IsBuildOption, n,v)
        WHERE opt.v IS NOT NULL -- Exclude default values
    ) x
    CROSS APPLY (
        SELECT CreateBase     = CONCAT_WS(' ', 'CREATE', IIF(i.is_unique = 1, 'UNIQUE', NULL), i.[type_desc] COLLATE DATABASE_DEFAULT, 'INDEX', qn.IndexName, 'ON', qn.SchemaName+'.'+qn.ObjectName)
            ,  Cols           = CONCAT_WS(' ', '('+kc.KeyCols+')', 'INCLUDE ('+kc.InclCols+')')
            ,  Features       = CONCAT_WS(' '
                                    , 'WHERE '+i.filter_definition
                                    , 'WITH ('+NULLIF(CONCAT_WS(', ', x.CreateOptions, x.BuildOptions),'')+')'
                                    , 'ON '+IIF(ds.is_default = 0, QUOTENAME(ds.[name]), NULL)
                                )
            ,  BuildOptions   = 'WITH ('+x.BuildOptions+')'
            ,  BatchSeparator = IIF(@BatchSeparator = 1, @crlf + 'GO', '') + IIF(@TrailingLineBreak = 1, @crlf+@crlf, '')
    ) c
    CROSS APPLY (
        SELECT IfExists    = IIF(@ScriptIfExists    = 1, s.IfExists    + @crlf + 'BEGIN;' + @crlf, '')
                           + IIF(@ScriptIfExists    = 1, @tab, '') + IIF(@AddOutputMessages = 1, @SqlOutputMessage + @crlf, '')
                           + IIF(@ScriptIfExists    = 1, @tab, '') + '{{Script}}'
                           + IIF(@ScriptIfExists    = 1, @crlf + 'END;', '')
                           + c.BatchSeparator

            ,  IfNotExists = IIF(@ScriptIfNotExists = 1, s.IfNotExists + @crlf + 'BEGIN;' + @crlf, '')
                           + IIF(@ScriptIfNotExists = 1, @tab, '') + IIF(@AddOutputMessages = 1, @SqlOutputMessage + @crlf, '')
                           + IIF(@ScriptIfNotExists = 1, @tab, '') + '{{Script}}'
                           + IIF(@ScriptIfNotExists = 1, @crlf + 'END;', '')
                           + c.BatchSeparator
    ) y
WHERE i.[type] > 0 -- Exclude heaps
    AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 -- PK's and Unique constraints have their own syntax
    AND o.is_ms_shipped = 0
ORDER BY n.SchemaName, n.ObjectName, n.IndexName;
