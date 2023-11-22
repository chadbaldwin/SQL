DECLARE @2 bigint = 2;

IF OBJECT_ID('tempdb..#tmp_idx','U') IS NOT NULL DROP TABLE #tmp_idx; --SELECT * FROM #tmp_idx
SELECT i.[object_id], i.index_id
	, SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]
	, ObjectType = o.[type_desc], IndexType = i.[type_desc]
	, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.is_disabled, i.is_hypothetical, i.has_filter, filter_definition = COALESCE(i.filter_definition,'')
	, x.KeyBitmap1, x.KeyBitmap2, x.InclBitmap1, x.InclBitmap2
INTO #tmp_idx
FROM sys.indexes i
	JOIN sys.objects o ON o.[object_id] = i.[object_id]
	CROSS APPLY (
		SELECT KeyBitmap1  = CONVERT(bigint, SUM(IIF(ic.is_included_column = 0 AND ic.column_id >  0 AND ic.column_id <=  62, POWER(@2, ic.column_id     ), 0)))
			,  KeyBitmap2  = CONVERT(bigint, SUM(IIF(ic.is_included_column = 0 AND ic.column_id > 62 AND ic.column_id <= 126, POWER(@2, ic.column_id - 62), 0)))
			,  InclBitmap1 = CONVERT(bigint, SUM(IIF(ic.is_included_column = 1 AND ic.column_id >  0 AND ic.column_id <=  62, POWER(@2, ic.column_id     ), 0)))
			,  InclBitmap2 = CONVERT(bigint, SUM(IIF(ic.is_included_column = 1 AND ic.column_id > 62 AND ic.column_id <= 126, POWER(@2, ic.column_id - 62), 0)))
		FROM sys.index_columns ic
		WHERE ic.[object_id] = i.[object_id]
			AND ic.index_id = i.index_id
	) x
WHERE i.index_id >= 2
	AND o.is_ms_shipped = 0
	AND i.is_primary_key = 0
	AND i.is_disabled = 0

SELECT x.[object_id], x.index_id
	, x.SchemaName, x.ObjectName, x.IndexName, x.ObjectType, x.IndexType
	, x.is_unique, x.is_primary_key, x.is_unique_constraint, x.is_disabled, x.is_hypothetical, x.has_filter
	, N'█' [██], x.KeyBitmap1, x.KeyBitmap2, x.InclBitmap1, x.InclBitmap2
	, N'█' [██], y.IndexName, y.KeyBitmap1, y.KeyBitmap2, y.InclBitmap1, y.InclBitmap2
	, N'█' [██], x.KeyBitmap1 & y.KeyBitmap1
FROM #tmp_idx x
	CROSS APPLY (
		SELECT i.IndexName, i.KeyBitmap1, i.KeyBitmap2, i.InclBitmap1, i.InclBitmap2
		FROM #tmp_idx i
		WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id
			AND i.is_unique = x.is_unique AND i.has_filter = x.has_filter AND i.filter_definition = x.filter_definition
			AND i.KeyBitmap1  & x.KeyBitmap1  = i.KeyBitmap1
			AND i.KeyBitmap2  & x.KeyBitmap2  = i.KeyBitmap2
			AND i.InclBitmap1 & x.InclBitmap1 = i.InclBitmap1
			AND i.InclBitmap2 & x.InclBitmap2 = i.InclBitmap2
	) y
WHERE 1=1
ORDER BY x.SchemaName, x.ObjectName, x.IndexName
