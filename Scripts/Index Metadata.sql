/* Recommend using NOLOCK; it tends to deadlock if a rebuilds are running */
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
GO
WITH cte_indexes AS (
	/* Get list of indexes we want to export */
	SELECT o.[object_id], i.index_id
		/* Name info */
		, ServerName			= @@SERVERNAME
		, DatabaseName			= DB_NAME()
		, SchemaName			= s.[name]
		, ObjectName			= o.[name]
		, IndexName				= IIF(i.[type_desc] = 'HEAP' AND i.[name] IS NULL, '<<HEAP>>', i.[name]) /* Heaps have NULL index names */
		/* Object Metadata */
		, ObjectType			= o.[type_desc]
		, ObjectCreateDate		= o.create_date
		/* Index Metadata */
		, IndexType				= i.[type_desc]
		, IsUnique				= i.is_unique
		, IsPrimaryKey			= i.is_primary_key
		, IsUniqueConstraint	= i.is_unique_constraint
		, IsDisabled			= i.is_disabled
		, IsHypothetical		= i.is_hypothetical
		, HasFilter				= i.has_filter
	FROM sys.indexes i
		JOIN sys.objects o ON o.[object_id] = i.[object_id]
		JOIN sys.schemas s ON s.[schema_id] = o.[schema_id]
	WHERE s.[name] <> 'sys' AND o.is_ms_shipped = 0 /* Exclude internal/system stuff */
		AND o.[type] IN ('U','V') /* Only tables and views - otherwise it includes indexes associated with functions */
), cte_index_size AS (
	/* Returns rows for all indexes where is_disabled = 0 */
	SELECT s.[object_id], s.index_id
		, EstimatedIndexSizeKB	= SUM(s.used_page_count) * 8 /* would probably be easier to store value directly, but KB is more human to read */
		, IndexRowCount			= SUM(s.row_count) 
	FROM sys.dm_db_partition_stats s
	GROUP BY s.[object_id], s.index_id
), cte_fk_index_counts AS (
	/* FKs dont have to reference a PK, they can reference any unique index */
	SELECT [object_id]			= fk.referenced_object_id
		, index_id				= fk.key_index_id
		, FKReferenceCount		= COUNT(*)
	FROM sys.foreign_keys fk
	GROUP BY fk.referenced_object_id, fk.key_index_id
)
SELECT i.[object_id], i.index_id, StatsCollectionTimeUTC = GETUTCDATE(), x.SQLServerStartTimeUTC
	/* Name info */			, i.ServerName, i.DatabaseName, i.SchemaName, i.ObjectName, i.IndexName
	/* Object Metadata */	, i.ObjectType, x.ObjectCreateDate
	/* Index Metadata */	, i.IndexType, i.IsUnique, i.IsPrimaryKey, i.IsUniqueConstraint, i.IsDisabled, i.IsHypothetical, i.HasFilter
							, FKReferenceCount = COALESCE(fkc.FKReferenceCount, 0)
	/* Stats */				, x.SeekCount, x.ScanCount, x.LookupCount, x.UpdateCount, ReadCount = x.SeekCount + x.ScanCount + x.LookupCount
							, x.LastSeek, x.LastScan, x.LastLookup, x.LastUpdate, l.LastRead
							, x.EstimatedIndexSizeKB, x.IndexRowCount
FROM cte_indexes i
	/*	Need to be LEFT JOIN in order to include ALL indexes in the results.
		Default plan tries to use a nested loop join taking 26 seconds to run.
		HASH joins won testing; Reducing runtime to ~3 seconds and significantly reducing logical reads and CPU time. */
	LEFT HASH JOIN cte_index_size ixs ON ixs.[object_id] = i.[object_id] AND ixs.index_id = i.index_id
	LEFT HASH JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.[object_id] = i.[object_id] AND ius.index_id = i.index_id
	LEFT JOIN cte_fk_index_counts fkc ON fkc.[object_id] = i.[object_id] AND fkc.index_id = i.index_id
	CROSS APPLY (SELECT SQLServerStartTime = osi.sqlserver_start_time FROM sys.dm_os_sys_info osi) ss
	CROSS APPLY (SELECT OffsetMinutes = DATEPART(TZOFFSET, SYSDATETIMEOFFSET())) tz /* Get the system TZ offset */
	CROSS APPLY (
		SELECT SeekCount				= COALESCE(ius.user_seeks, 0)
			,  ScanCount				= COALESCE(ius.user_scans, 0)
			,  LookupCount				= COALESCE(ius.user_lookups, 0)			/* Lookups only occur on clustered indexes */
			,  UpdateCount				= COALESCE(ius.user_updates, 0)
			,  EstimatedIndexSizeKB		= COALESCE(ixs.EstimatedIndexSizeKB, 0) /* Disabled indexes have NULL size */
			,  IndexRowCount			= COALESCE(ixs.IndexRowCount, 0)		/* Disabled indexes have NULL row count */
			/* Unfortunately, SQL Server stores everything with server local time - converting everything to UTC */
			,  LastSeek					= DATEADD(MINUTE, -tz.OffsetMinutes, ius.last_user_seek)
			,  LastScan					= DATEADD(MINUTE, -tz.OffsetMinutes, ius.last_user_scan)
			,  LastLookup				= DATEADD(MINUTE, -tz.OffsetMinutes, ius.last_user_lookup)
			,  LastUpdate				= DATEADD(MINUTE, -tz.OffsetMinutes, ius.last_user_update)
			,  ObjectCreateDate			= DATEADD(MINUTE, -tz.OffsetMinutes, i.ObjectCreateDate)
			,  SQLServerStartTimeUTC	= DATEADD(MINUTE, -tz.OffsetMinutes, ss.SQLServerStartTime)
	) x
	CROSS APPLY (SELECT LastRead = MAX(x.LastRead) FROM (VALUES (x.LastSeek), (x.LastScan), (x.LastLookup)) x(LastRead)) l;
