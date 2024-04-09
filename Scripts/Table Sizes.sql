SELECT x.TableName, x.SchemaName, x.CompressionType
	, RowCounts			= RIGHT(SPACE(16) + FORMAT(x.RowCounts		,'N0'), 16)
	, TotalKB			= RIGHT(SPACE(16) + FORMAT(x.TotalKB		,'N0'), 16)
	, TotalMB			= RIGHT(SPACE(16) + FORMAT(x.TotalMB		,'N2'), 16)
	, TotalGB			= RIGHT(SPACE(16) + FORMAT(x.TotalGB		,'N2'), 16)
	, UsedKB			= RIGHT(SPACE(16) + FORMAT(x.UsedKB			,'N0'), 16)
	, UsedMB			= RIGHT(SPACE(16) + FORMAT(x.UsedMB			,'N2'), 16)
	, UsedGB			= RIGHT(SPACE(16) + FORMAT(x.UsedGB			,'N2'), 16)
	, UnusedKB			= RIGHT(SPACE(16) + FORMAT(x.UnusedKB		,'N0'), 16)
	, UnusedMB			= RIGHT(SPACE(16) + FORMAT(x.UnusedMB		,'N2'), 16)
	, UnusedGB			= RIGHT(SPACE(16) + FORMAT(x.UnusedGB		,'N2'), 16)
	, AvgRecordBytes	= RIGHT(SPACE(16) + FORMAT(x.AvgRecordBytes	,'N2'), 16)
FROM (
	SELECT x.TableName, x.SchemaName, x.RowCounts, x.CompressionType
		, TotalKB			= x.TotalKB
		, TotalMB			= x.TotalKB  / 1024.0
		, TotalGB			= x.TotalKB  / 1024.0 / 1024.0
		, UsedKB			= x.UsedKB
		, UsedMB			= x.UsedKB   / 1024.0
		, UsedGB			= x.UsedKB   / 1024.0 / 1024.0
		, UnusedKB			= x.UnusedKB
		, UnusedMB			= x.UnusedKB / 1024.0
		, UnusedGB			= x.UnusedKB / 1024.0 / 1024.0
		, AvgRecordBytes	= COALESCE(x.TotalKB / NULLIF(x.RowCounts, 0) * 1024, 0)
	FROM (
		SELECT TableName		= t.[name]
			, SchemaName		= SCHEMA_NAME(t.[schema_id])
			, RowCounts			= p.[rows]
			, TotalKB			= SUM(a.total_pages) * 8.0
			, UsedKB			= SUM(a.used_pages) * 8.0
			, UnusedKB			= (SUM(a.total_pages) - SUM(a.used_pages)) * 8.0
			, CompressionType	= STRING_AGG(CONCAT(a.[type_desc], ': ', p.data_compression_desc), ', ') WITHIN GROUP (ORDER BY a.[type_desc])
		FROM sys.tables t
			JOIN sys.indexes i ON t.[object_id] = i.[object_id]
			JOIN sys.partitions p ON i.[object_id] = p.[object_id] AND i.index_id = p.index_id
			JOIN sys.allocation_units a ON p.[partition_id] = a.container_id
		WHERE t.is_ms_shipped = 0
			AND i.[object_id] > 255
			AND i.index_id <= 1 -- Heaps and clustered indexes only
			AND a.total_pages > 0
		GROUP BY t.[name], SCHEMA_NAME(t.[schema_id]), p.[rows]
	) x
) x
