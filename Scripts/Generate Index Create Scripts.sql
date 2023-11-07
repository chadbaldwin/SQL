SELECT n.SchemaName, n.ObjectName, n.IndexName, IndexDefinition = CONCAT_WS(' '
		, 'CREATE', IIF(i.is_unique = 1, 'UNIQUE', NULL), i.[type_desc] COLLATE DATABASE_DEFAULT, 'INDEX', QUOTENAME(n.IndexName)
		, 'ON', CONCAT(QUOTENAME(n.SchemaName),'.', QUOTENAME(n.ObjectName))
		, '('+kc.KeyCols+')', 'INCLUDE ('+kc.InclCols+')'
		, 'WHERE ' + i.filter_definition, 'WITH ('+x.Options+')'
		, IIF(ds.is_default = 0, 'ON '+QUOTENAME(ds.[name]), NULL)
	--	, 'FILESTREAM_ON {{FilestreamGroup|PartitionName|"NULL"}}'
	)+';'
FROM sys.indexes i
	JOIN sys.objects o ON o.[object_id] = i.[object_id]
	JOIN sys.stats st ON st.[object_id] = i.[object_id] AND st.stats_id = i.index_id
	JOIN sys.partitions p ON p.[object_id] = i.[object_id] AND p.index_id = i.index_id AND p.partition_number = 1 -- Partitioning not yet supported
	JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
	CROSS APPLY (SELECT SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]) n
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
				,  ('FILLFACTOR'			, IIF(i.fill_factor > 0			, CONVERT(varchar(3), i.fill_factor), NULL))
				,  ('IGNORE_DUP_KEY'		, IIF(i.[ignore_dup_key] = 1	, 'ON', NULL))
				,  ('STATISTICS_NORECOMPUTE', IIF(st.no_recompute = 1		, 'ON', NULL))
				,  ('STATISTICS_INCREMENTAL', IIF(st.is_incremental = 1		, 'ON', NULL))
				,  ('ALLOW_ROW_LOCKS'		, IIF(i.[allow_row_locks] = 1	, NULL, 'OFF'))
				,  ('ALLOW_PAGE_LOCKS'		, IIF(i.[allow_page_locks] = 1	, NULL, 'OFF'))
				,  ('DATA_COMPRESSION'		, NULLIF(p.[data_compression_desc] COLLATE DATABASE_DEFAULT, 'NONE'))
			--	,  ('XML_COMPRESSION'		, NULL) -- Haven't figured it out yet
				,  ('ONLINE'				, IIF(SERVERPROPERTY('EngineEdition') = 3, 'ON', NULL)) -- 3 = Eval/Dev/Enterprise
		) opt(n,v)
		WHERE opt.v IS NOT NULL -- Exclude default values
	) x
WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0 -- PK's and Unique constraints have their own syntax
	AND o.is_ms_shipped = 0
