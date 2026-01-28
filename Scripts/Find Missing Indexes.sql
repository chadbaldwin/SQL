GO
/*
https://learn.microsoft.com/en-us/sql/relational-databases/indexes/tune-nonclustered-missing-index-suggestions
https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-missing-index-group-stats-transact-sql

| Column name         | Description                                                                                                        |
|---------------------|--------------------------------------------------------------------------------------------------------------------|
| unique_compiles     | Number of compilations and recompilations that would benefit from this missing index group                         |
| user_seeks          | Number of seeks caused by user queries that the recommended index in the group could have been used for            |
| user_scans          | Number of scans caused by user queries that the recommended index in the group could have been used for            |
| last_user_seek      | Date and time of last seek caused by user queries that the recommended index in the group could have been used for |
| last_user_scan      | Date and time of last scan caused by user queries that the recommended index in the group could have been used for |
| avg_total_user_cost | Average cost of the user queries that could be reduced by the index in the group                                   |
| avg_user_impact     | Average percentage benefit that user queries could experience if this missing index group was implemented          |

SELECT * FROM sys.dm_db_missing_index_columns(1164)
SELECT * FROM sys.dm_db_missing_index_details
SELECT * FROM sys.dm_db_missing_index_group_stats
SELECT * FROM sys.dm_db_missing_index_group_stats_query
SELECT * FROM sys.dm_db_missing_index_groups
*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #mid;
SELECT index_handle, [object_id]
	, [database_name]	= PARSENAME(mid.[statement], 3)
	, [schema_name]		= PARSENAME(mid.[statement], 2)
	, [object_name]		= PARSENAME(mid.[statement], 1)
	, object_type		= OBJECTPROPERTYEX(mid.[object_id], 'BaseType')
INTO #mid
FROM sys.dm_db_missing_index_details mid
WHERE mid.database_id = DB_ID();
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #mig;
SELECT mig.index_group_handle, mig.index_handle
INTO #mig
FROM sys.dm_db_missing_index_groups mig
WHERE EXISTS (SELECT * FROM #mid mid WHERE mid.index_handle = mig.index_handle);
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #migs;
SELECT group_handle, unique_compiles, user_seeks, user_scans, last_user_seek, last_user_scan, avg_total_user_cost, avg_user_impact
INTO #migs
FROM sys.dm_db_missing_index_group_stats migs
WHERE EXISTS (SELECT * FROM #mig mig WHERE mig.index_group_handle = migs.group_handle);
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #cols;
CREATE TABLE #cols (
	[object_id] int NOT NULL,
	column_id int NOT NULL,
	[name] nvarchar(128) NOT NULL,
	[type_desc] nvarchar(256) NULL,
	INDEX cix CLUSTERED ([object_id], column_id),
);

INSERT #cols ([object_id], column_id, [name], [type_desc])
SELECT [object_id], column_id, [name], dt.[type_desc]
FROM sys.columns c
	CROSS APPLY (SELECT [type_name] = COALESCE(TYPE_NAME(c.system_type_id), TYPE_NAME(c.user_type_id))) n
	CROSS APPLY (
		SELECT [type_desc] = CONCAT(n.[type_name]
				,   CASE
						WHEN n.[type_name] IN ('datetime2','time','datetimeoffset')   THEN IIF(c.scale = 7, NULL, CONCAT('(', c.scale, ')')) --scale of (7) is the default so it can be ignored; (0) is a valid value
						WHEN n.[type_name] IN ('decimal','numeric')                   THEN CONCAT('(', c.[precision], ',', c.scale,')')
						WHEN n.[type_name] IN ('nchar','nvarchar')                    THEN IIF(c.max_length = -1, '(MAX)', CONCAT('(', c.max_length/2, ')'))
						WHEN n.[type_name] IN ('char','varchar','binary','varbinary') THEN IIF(c.max_length = -1, '(MAX)', CONCAT('(', c.max_length, ')'))
						-- Including for the sake of clarity so I know they've been covered
						WHEN n.[type_name] IN ('real','float')                        THEN NULL -- real and float are odd because float(1-24) = real(24); float(25-53) = float(53); real(N) = real(24); so we can just pass these through with no extra info
						WHEN n.[type_name] IN ('bit','tinyint','smallint','int','bigint','money','date','datetime','smalldatetime','geometry','sql_variant','uniqueidentifier','xml','hierarchyid','image','text','ntext','timestamp') THEN NULL
						ELSE '{{UNRECOGNIZED}}'
					END
			)
	) dt
WHERE EXISTS (SELECT * FROM #mid d WHERE d.[object_id] = c.[object_id]);
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #idx_cols;
SELECT mid.index_handle
	, Cols_EQ	= STRING_AGG(IIF(mic.column_usage = 'EQUALITY'	, QUOTENAME(mic.column_name) + ' ' + c.[type_desc], NULL), ', ') WITHIN GROUP (ORDER BY mic.column_name)
	, Cols_INC	= STRING_AGG(IIF(mic.column_usage = 'INCLUDE'	, QUOTENAME(mic.column_name) + ' ' + c.[type_desc], NULL), ', ') WITHIN GROUP (ORDER BY mic.column_name)
	, Cols_IEQ	= STRING_AGG(IIF(mic.column_usage = 'INEQUALITY', QUOTENAME(mic.column_name) + ' ' + c.[type_desc], NULL), ', ') WITHIN GROUP (ORDER BY mic.column_name)
INTO #idx_cols
FROM #mid mid
	CROSS APPLY sys.dm_db_missing_index_columns(mid.index_handle) mic
	JOIN #cols c ON c.[object_id] = mid.[object_id] AND c.column_id = mic.column_id
GROUP BY mid.index_handle;
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #migsq;
SELECT q.group_handle, q.query_hash, q.query_plan_hash, q.last_sql_handle, q.last_statement_start_offset, q.last_statement_end_offset, q.last_statement_sql_handle
	, q.user_seeks, q.user_scans, q.last_user_seek, q.last_user_scan, q.avg_total_user_cost, q.avg_user_impact
	, [database_name] = DB_NAME(st.[dbid])
	, [schema_name] = OBJECT_SCHEMA_NAME(st.objectid, st.[dbid])
	, [object_name] = OBJECT_NAME(st.objectid, st.[dbid])
	, batch_text = st.[text]
	, query_text = SUBSTRING(st.[text], q.last_statement_start_offset/2+1, IIF(q.last_statement_end_offset = -1, DATALENGTH(st.[text]), (q.last_statement_end_offset-q.last_statement_start_offset)/2+1))
INTO #migsq
FROM sys.dm_db_missing_index_group_stats_query q
	OUTER APPLY sys.dm_exec_sql_text(q.last_sql_handle) st
WHERE EXISTS (SELECT * FROM #mig mig WHERE mig.index_group_handle = q.group_handle);
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #missing_index_recs;
WITH idx_cols AS (
	SELECT ic.[object_id], ic.index_id
		, KeyCols = STRING_AGG(x.KeyColName, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, c.[name])
	FROM sys.index_columns ic
		JOIN #cols c ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
		OUTER APPLY (SELECT KeyColName = IIF(ic.is_included_column = 0, CONCAT_WS(' ', QUOTENAME(c.[name]), c.[type_desc], IIF(ic.is_descending_key = 1, 'DESC', NULL)), NULL)) x
	WHERE ic.index_id = 1
	GROUP BY ic.[object_id], ic.index_id
)
SELECT IndexGroupHandle = mig.index_group_handle
	, IndexHandle		= mig.index_handle
	, ObjectID          = mid.[object_id]
	, SchemaName		= mid.[schema_name]
	, ObjectName		= mid.[object_name]
	, ObjectType		= mid.object_type
	, ObjectRowCount	= CONVERT(bigint, OBJECTPROPERTYEX(mid.[object_id], 'Cardinality'))
	, ObjNCIdxCount		= x.NCIndexCount
	, ClstIdxCols		= cic.KeyCols
	, EQDupCount		= COUNT(*) OVER (PARTITION BY mid.[object_id], ic.Cols_EQ)
	, EQDupTotalCompiles= SUM(migs.unique_compiles)					OVER (PARTITION BY mid.[object_id], ic.Cols_EQ)
	, EQDupTotalReads	= SUM(migs.user_seeks + migs.user_scans)	OVER (PARTITION BY mid.[object_id], ic.Cols_EQ)
	, ic.Cols_EQ, ic.Cols_IEQ, ic.Cols_INC
	, QueryCount		= qc.QueryCount
	, MaxQueryCost		= qc.MaxCost
	, AvgCost			= migs.avg_total_user_cost
	, AvgImpactPct		= migs.avg_user_impact/100.0
	, Compiles			= migs.unique_compiles
	, Seeks				= migs.user_seeks
	, Scans				= migs.user_scans
	, LasSeekTime		= migs.last_user_seek
	, LastSeekMS		= l.LastSeekMS
	, CompileRatio		= migs.unique_compiles / NULLIF(((migs.user_seeks + migs.user_scans) * 1.0), 0)
	, Ranking			= CUME_DIST() OVER (ORDER BY migs.unique_compiles)              * 0.25
						+ CUME_DIST() OVER (ORDER BY migs.user_seeks + migs.user_scans) * 0.25
						+ CUME_DIST() OVER (ORDER BY migs.avg_total_user_cost)          * 0.25
						+ CUME_DIST() OVER (ORDER BY migs.avg_user_impact)              * 0.25
INTO #missing_index_recs
FROM #mid mid
	JOIN #mig mig ON mig.index_handle = mid.index_handle
	LEFT JOIN #migs migs ON migs.group_handle = mig.index_group_handle
	LEFT JOIN idx_cols cic ON cic.[object_id] = mid.[object_id]
	LEFT JOIN #idx_cols ic ON ic.index_handle = mid.index_handle
	OUTER APPLY (SELECT NCIndexCount = COUNT(*) FROM sys.indexes i WHERE i.[object_id] = mid.[object_id] AND i.index_id > 1) x
	CROSS APPLY (SELECT LastSeekMS = DATEDIFF(MILLISECOND, migs.last_user_seek, GETDATE())) l
	CROSS APPLY (SELECT QueryCount = COUNT(*), MaxCost = MAX(avg_total_user_cost) FROM #migsq q WHERE q.group_handle = mig.index_group_handle) qc;
------------------------------------------------------------

------------------------------------------------------------
-- Yes it's ugly, if you don't like it, build a UI for it 😅
SELECT IndexGroupHandle, IndexHandle, SchemaName, ObjectName, ObjectType
	, N'█' [█]
		, ObjectRowCount= RIGHT(SPACE(50)+FORMAT(ObjectRowCount		,'N0'), GREATEST(LEN('ObjectRowCount')		, MAX(LEN(FORMAT(ObjectRowCount		,'N0'))) OVER ()))
		, ObjNCIdxCount
		, ClstIdxCols
	, N'█ EQ Dup Totals ->' [█]
		, [Count]		= RIGHT(SPACE(50)+FORMAT(EQDupCount			,'N0'), GREATEST(LEN('EQDupCount')			, MAX(LEN(FORMAT(EQDupCount			,'N0'))) OVER ()))
		, Compiles		= RIGHT(SPACE(50)+FORMAT(EQDupTotalCompiles	,'N0'), GREATEST(LEN('EQDupTotalCompiles')	, MAX(LEN(FORMAT(EQDupTotalCompiles	,'N0'))) OVER ()))
		, Reads			= RIGHT(SPACE(50)+FORMAT(EQDupTotalReads	,'N0'), GREATEST(LEN('EQDupTotalReads')		, MAX(LEN(FORMAT(EQDupTotalReads	,'N0'))) OVER ()))
	, N'█' [█], Cols_EQ, Cols_IEQ, Cols_INC
	, N'█' [█]
		, QueryCount
		, MaxQueryCost	= RIGHT(SPACE(50)+FORMAT(MaxQueryCost		,'N2'), GREATEST(LEN('MaxQueryCost')		, MAX(LEN(FORMAT(MaxQueryCost		,'N2'))) OVER ()))
		, AvgCost		= RIGHT(SPACE(50)+FORMAT(AvgCost			,'N2'), GREATEST(LEN('AvgCost')				, MAX(LEN(FORMAT(AvgCost			,'N2'))) OVER ()))
		, AvgImpactPct	= RIGHT(SPACE(50)+FORMAT(AvgImpactPct		,'P2'), GREATEST(LEN('AvgImpactPct')		, MAX(LEN(FORMAT(AvgImpactPct		,'P2'))) OVER ()))
		, Compiles		= RIGHT(SPACE(50)+FORMAT(Compiles			,'N0'), GREATEST(LEN('Compiles')			, MAX(LEN(FORMAT(Compiles			,'N0'))) OVER ()))
		, Seeks			= RIGHT(SPACE(50)+FORMAT(Seeks				,'N0'), GREATEST(LEN('Seeks')				, MAX(LEN(FORMAT(Seeks				,'N0'))) OVER ()))
		, Scans			= RIGHT(SPACE(50)+FORMAT(Scans				,'N0'), GREATEST(LEN('Scans')				, MAX(LEN(FORMAT(Scans				,'N0'))) OVER ()))
		, LasSeekTime
		, LastSeek		= CONCAT(FORMAT(LastSeekMS / 1000.0, 'N0'), ' secs ago')
		, CompileRatio	= RIGHT(SPACE(50)+FORMAT(CompileRatio		,'P2'), GREATEST(LEN('CompileRatio')		, MAX(LEN(FORMAT(CompileRatio		,'P2'))) OVER ()))
		, Ranking		= RIGHT(SPACE(50)+FORMAT(Ranking			,'N2'), GREATEST(LEN('Ranking')				, MAX(LEN(FORMAT(Ranking			,'N2'))) OVER ()))
FROM #missing_index_recs
ORDER BY Ranking DESC;
------------------------------------------------------------

------------------------------------------------------------

RETURN;