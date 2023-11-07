DECLARE @ScriptIfNotExists	bit	= 1,
		@EnableOnline		bit = IIF(SERVERPROPERTY('EngineEdition') = 3, 1, 0);
DECLARE	@SqlIfNotExists		nvarchar(MAX) = N'IF (INDEXPROPERTY(OBJECT_ID(''{{Schema}}.{{Object}}''), ''{{Index}}'', ''IndexId'') IS NULL)',
		@SqlDisable			nvarchar(MAX) = 'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} DISABLE;',
		@SqlRebuild			nvarchar(MAX) = 'ALTER INDEX {{Index}} ON {{Schema}}.{{Object}} REBUILD' + IIF(@EnableOnline = 1, ' WITH (ONLINE=ON)', ''),
		@SqlDrop			nvarchar(MAX) = 'DROP INDEX IF EXISTS {{Index}} ON {{Schema}}.{{Object}};';

SELECT n.SchemaName, n.ObjectName, n.IndexName, i.is_disabled
	, CreateScript = IIF(@ScriptIfNotExists = 1, s.IfNotExists+CHAR(13)+CHAR(10)+CHAR(9), '') + CONCAT_WS(' '
		, 'CREATE', IIF(i.is_unique = 1, 'UNIQUE', NULL), i.[type_desc] COLLATE DATABASE_DEFAULT, 'INDEX', qn.IndexName		-- CREATE UNIQUE CLUSTERED INDEX [IX_Foo_Bar_Baz]
		, 'ON', qn.SchemaName+'.'+qn.ObjectName, '('+kc.KeyCols+')', 'INCLUDE ('+kc.InclCols+')'							-- ON [dbo].[Foo] ([Bar]) INCLUDE ([Baz])
		, 'WHERE '+i.filter_definition, 'WITH ('+x.Options+')', IIF(ds.is_default = 0, 'ON '+QUOTENAME(ds.[name]), NULL)	-- WHERE (Val >= 100) WITH (ONLINE=ON) ON [Secondary]
	--	, 'FILESTREAM_ON {{FilestreamGroup|PartitionName|"NULL"}}'
	)+';'
	, s.DisableScript, s.RebuildScript, s.DropScript
FROM sys.indexes i
	JOIN sys.objects o ON o.[object_id] = i.[object_id]
	JOIN sys.stats st ON st.[object_id] = i.[object_id] AND st.stats_id = i.index_id
	-- Disabled indexes do not have sys.partitions records
	LEFT HASH JOIN sys.partitions p ON p.[object_id] = i.[object_id] AND p.index_id = i.index_id AND p.partition_number = 1 -- Partitioning not yet supported
	JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
	CROSS APPLY (SELECT SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]) n
	CROSS APPLY (SELECT SchemaName = QUOTENAME(n.SchemaName), ObjectName = QUOTENAME(n.ObjectName), IndexName = QUOTENAME(n.IndexName)) qn
	CROSS APPLY (
		SELECT IfNotExists		= REPLACE(REPLACE(REPLACE(@SqlIfNotExists	,'{{Schema}}',qn.SchemaName),'{{Object}}',qn.ObjectName),'{{Index}}',n.IndexName)
			,  DisableScript	= REPLACE(REPLACE(REPLACE(@SqlDisable		,'{{Schema}}',qn.SchemaName),'{{Object}}',qn.ObjectName),'{{Index}}',qn.IndexName)
			,  RebuildScript	= REPLACE(REPLACE(REPLACE(@SqlRebuild		,'{{Schema}}',qn.SchemaName),'{{Object}}',qn.ObjectName),'{{Index}}',qn.IndexName)
			,  DropScript		= REPLACE(REPLACE(REPLACE(@SqlDrop			,'{{Schema}}',qn.SchemaName),'{{Object}}',qn.ObjectName),'{{Index}}',qn.IndexName)
	) s
	CROSS APPLY (
		SELECT KeyCols = STRING_AGG(IIF(ic.is_included_column = 0, x.Col, NULL), ', ')
			, InclCols = STRING_AGG(IIF(ic.is_included_column = 1, x.Col, NULL), ', ')
		FROM sys.index_columns ic
			CROSS APPLY (SELECT Col = CONCAT_WS(' ', QUOTENAME(COL_NAME(ic.[object_id], ic.column_id)), IIF(ic.is_descending_key = 1, 'DESC', NULL))) x
		WHERE ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
	) kc
	CROSS APPLY (
		SELECT Options = STRING_AGG(CONCAT(opt.n, '=', opt.v), ', ')
		FROM (
			VALUES ('PAD_INDEX'				, IIF(i.is_padded = 1			, 'ON', NULL))
				,  ('FILLFACTOR'			, CONVERT(varchar(3), NULLIF(i.fill_factor, 0)))
				,  ('IGNORE_DUP_KEY'		, IIF(i.[ignore_dup_key] = 1	, 'ON', NULL))
				,  ('STATISTICS_NORECOMPUTE', IIF(st.no_recompute = 1		, 'ON', NULL))
				,  ('STATISTICS_INCREMENTAL', IIF(st.is_incremental = 1		, 'ON', NULL))
				,  ('ALLOW_ROW_LOCKS'		, IIF(i.[allow_row_locks] = 1	, NULL, 'OFF'))
				,  ('ALLOW_PAGE_LOCKS'		, IIF(i.[allow_page_locks] = 1	, NULL, 'OFF'))
				,  ('DATA_COMPRESSION'		, NULLIF(p.[data_compression_desc] COLLATE DATABASE_DEFAULT, 'NONE')) -- Only works for non-partitioned tables
				,  ('XML_COMPRESSION'		, NULL) -- Haven't figured it out yet
				-- Create options
				,  ('ONLINE'				, IIF(@EnableOnline = 1			, 'ON', NULL)) -- 3 = Eval/Dev/Enterprise
		) opt(n,v)
		WHERE opt.v IS NOT NULL -- Exclude default values
	) x
WHERE i.[type] > 0 -- Exclude heaps
	AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 -- PK's and Unique constraints have their own syntax
	AND o.is_ms_shipped = 0
ORDER BY n.SchemaName, n.ObjectName, n.IndexName
