SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
DECLARE @debug bit = 0;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1)
BEGIN;
	DROP TABLE IF EXISTS #tmp_object;
	DROP TABLE IF EXISTS #tmp_part;
	DROP TABLE IF EXISTS #tmp_fk;
	DROP TABLE IF EXISTS #tmp_idx_op_stat;
	DROP TABLE IF EXISTS #tmp_idx_col_squash;
END;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) SET STATISTICS IO, TIME ON;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Declare variables',0,1) WITH NOWAIT;
DECLARE @LocalTZ            nvarchar(128),
        @CollectionTime     datetime2       = SYSUTCDATETIME(),
        @InstanceName       nvarchar(257)   = CONCAT_WS('\', @@SERVERNAME, CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName'))),
        @DatabaseName       nvarchar(128)   = DB_NAME(),
        @DBIsPrimaryReplica bit             = sys.fn_hadr_is_primary_replica(DB_NAME()), /* 0 = secondary, 1 = primary, NULL = N/A */
        @SQLServerStartTime datetime2,
        @DBLastRestoreTime  datetime2;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get timezone info',0,1) WITH NOWAIT;
/*  Get instance time zone from registry
    2019 (v15) and under, registry is the only way to get it
    2022 (v16) and up we can use the new CURRENT_TIMEZONE_ID() function
*/
DECLARE @tzsql nvarchar(MAX);
IF (CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) < 16)
BEGIN;
    SELECT @tzsql = N'EXEC [master].dbo.xp_regread @rootkey = ''HKEY_LOCAL_MACHINE'', @key = ''SYSTEM\CurrentControlSet\Control\TimeZoneInformation'', @value_name = ''TimeZoneKeyName'', @value = @LocalTZ OUT;'
END;
ELSE
BEGIN;
    SELECT @tzsql = N'SELECT @LocalTZ = CURRENT_TIMEZONE_ID()'
END;

EXEC sys.sp_executesql @stmt = @tzsql, @params = N'@LocalTZ nvarchar(128) OUT', @LocalTZ = @LocalTZ OUT
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get SQL Server start time',0,1) WITH NOWAIT;
SELECT @SQLServerStartTime = sqlserver_start_time AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC'
FROM sys.dm_os_sys_info;

/* DB Restores don't require a service restart, but they do clear the restored database's stats DMVs */
IF (@debug = 1) RAISERROR('Get database last restore time',0,1) WITH NOWAIT;
SELECT @DBLastRestoreTime = MAX(restore_date) AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC'
FROM msdb.dbo.restorehistory WHERE destination_database_name = DB_NAME();
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get base set of objects to pull',0,1) WITH NOWAIT;
SELECT o.[schema_id], o.[object_id], o.[name], o.[type_desc], o.create_date
INTO #tmp_object
FROM sys.objects o
WHERE o.[type] IN ('U','V')
    -- Exclude SQL Server system objects
    AND o.is_ms_shipped = 0
    -- Exclude SSMS created system objects, like sysdiagrams
    AND NOT EXISTS (
        SELECT *
        FROM sys.extended_properties ep
        WHERE ep.major_id = o.[object_id]
            AND ep.class = 1 -- Objects/Columns
            AND ep.minor_id = 0 -- Exclude columns
            AND ep.[name] = N'microsoft_database_tools_support'
    );
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.dm_db_partition_stats',0,1) WITH NOWAIT;
/* Table is aggregated in order to rollup partitions */
/* Returns rows for all indexes where is_disabled = 0 */
SELECT s.[object_id], s.index_id
    , EstimatedIndexSizeKB  = SUM(s.used_page_count) * 8 /* would probably be easier to store value directly, but KB is more human to read */
    , IndexRowCount         = SUM(s.row_count)
    , CompressionType       = MAX(p.data_compression_desc COLLATE DATABASE_DEFAULT) /* Technically not accurate for partitioned indexes since each partition can have different compression types */
    , PartitionCount        = COUNT(*)
INTO #tmp_part
FROM sys.dm_db_partition_stats s
    JOIN sys.partitions p ON p.[object_id] = s.[object_id] AND p.index_id = s.index_id AND p.partition_number = s.partition_number
WHERE EXISTS (SELECT * FROM #tmp_object o WHERE o.[object_id] = s.[object_id])
GROUP BY s.[object_id], s.index_id
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get foreign key counts',0,1) WITH NOWAIT;
/* FKs dont have to reference a PK, they can reference any unique index */
SELECT [object_id]          = fk.referenced_object_id
    , index_id              = fk.key_index_id
    , FKReferenceCount      = COUNT(*)
INTO #tmp_fk
FROM sys.foreign_keys fk
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = fk.referenced_object_id AND p.index_id = fk.key_index_id)
GROUP BY fk.referenced_object_id, fk.key_index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;
/* Table is aggregated in order to rollup partitions */
SELECT x.[object_id], x.index_id
    , leaf_insert_count                  = SUM(x.leaf_insert_count)
    , leaf_delete_count                  = SUM(x.leaf_delete_count)
    , leaf_update_count                  = SUM(x.leaf_update_count)
--  , leaf_ghost_count                   = SUM(x.leaf_ghost_count)
--  , nonleaf_insert_count               = SUM(x.nonleaf_insert_count)
--  , nonleaf_delete_count               = SUM(x.nonleaf_delete_count)
--  , nonleaf_update_count               = SUM(x.nonleaf_update_count)
--  , leaf_allocation_count              = SUM(x.leaf_allocation_count)
--  , nonleaf_allocation_count           = SUM(x.nonleaf_allocation_count)
    , leaf_page_merge_count              = SUM(x.leaf_page_merge_count)
--  , nonleaf_page_merge_count           = SUM(x.nonleaf_page_merge_count)
    , range_scan_count                   = SUM(x.range_scan_count)
    , singleton_lookup_count             = SUM(x.singleton_lookup_count)
--  , forwarded_fetch_count              = SUM(x.forwarded_fetch_count)
--  , lob_fetch_in_pages                 = SUM(x.lob_fetch_in_pages)
--  , lob_fetch_in_bytes                 = SUM(x.lob_fetch_in_bytes)
--  , lob_orphan_create_count            = SUM(x.lob_orphan_create_count)
--  , lob_orphan_insert_count            = SUM(x.lob_orphan_insert_count)
--  , row_overflow_fetch_in_pages        = SUM(x.row_overflow_fetch_in_pages)
--  , row_overflow_fetch_in_bytes        = SUM(x.row_overflow_fetch_in_bytes)
--  , column_value_push_off_row_count    = SUM(x.column_value_push_off_row_count)
--  , column_value_pull_in_row_count     = SUM(x.column_value_pull_in_row_count)
    , row_lock_count                     = SUM(x.row_lock_count)
    , row_lock_wait_count                = SUM(x.row_lock_wait_count)
    , row_lock_wait_in_ms                = SUM(x.row_lock_wait_in_ms)
    , page_lock_count                    = SUM(x.page_lock_count)
    , page_lock_wait_count               = SUM(x.page_lock_wait_count)
    , page_lock_wait_in_ms               = SUM(x.page_lock_wait_in_ms)
    , index_lock_promotion_attempt_count = SUM(x.index_lock_promotion_attempt_count)
    , index_lock_promotion_count         = SUM(x.index_lock_promotion_count)
    , page_latch_wait_count              = SUM(x.page_latch_wait_count)
    , page_latch_wait_in_ms              = SUM(x.page_latch_wait_in_ms)
    , page_io_latch_wait_count           = SUM(x.page_io_latch_wait_count)
    , page_io_latch_wait_in_ms           = SUM(x.page_io_latch_wait_in_ms)
--  , tree_page_latch_wait_count         = SUM(x.tree_page_latch_wait_count)
--  , tree_page_latch_wait_in_ms         = SUM(x.tree_page_latch_wait_in_ms)
--  , tree_page_io_latch_wait_count      = SUM(x.tree_page_io_latch_wait_count)
--  , tree_page_io_latch_wait_in_ms      = SUM(x.tree_page_io_latch_wait_in_ms)
--  , page_compression_attempt_count     = SUM(x.page_compression_attempt_count)
--  , page_compression_success_count     = SUM(x.page_compression_success_count)
INTO #tmp_idx_op_stat
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) x
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = x.[object_id] AND p.index_id = x.index_id)
GROUP BY x.database_id, x.[object_id], x.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.index_columns',0,1) WITH NOWAIT;
SELECT ic.[object_id], ic.index_id 
    , KeyCols  = STRING_AGG(x.KeyColName , ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, c.[name])
    , InclCols = STRING_AGG(x.InclColName, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, c.[name])
INTO #tmp_idx_col_squash
FROM sys.index_columns ic
    JOIN sys.columns c ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
    CROSS APPLY (
        SELECT KeyColName  = IIF(ic.is_included_column = 0, CONCAT_WS(' ', QUOTENAME(c.[name]), IIF(ic.is_descending_key = 1, 'DESC', NULL)), NULL)
            ,  InclColName = IIF(ic.is_included_column = 1, QUOTENAME(c.[name]), NULL)
    ) x
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = ic.[object_id] AND p.index_id = ic.index_id)
GROUP BY ic.[object_id], ic.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Assemble and return output',0,1) WITH NOWAIT;
--IF OBJECT_ID('tempdb..#output','U') IS NOT NULL DROP TABLE #output; --SELECT * FROM #output
SELECT i.[object_id], i.index_id
    , EstimatedStatsBeginTime   = r.BeginDate
    , StatsEndTime              = @CollectionTime
    , SQLServerStartTime        = @SQLServerStartTime
    , InstanceName              = @InstanceName
    , DatabaseName              = @DatabaseName
    , SchemaName                = SCHEMA_NAME(o.[schema_id])
    , ObjectName                = o.[name]
    , IndexName                 = COALESCE(i.[name], N'<<HEAP>>') /* Heaps have NULL index names */
    , DBIsPrimaryReplica        = @DBIsPrimaryReplica
    , ObjectType                = o.[type_desc]
    , ObjectCreateDate          = tz.ObjectCreateDate
    , IndexType                 = i.[type_desc]
    , IndexCreateDate           = tz.ConstraintCreateDate /* Only available on unique indexes since they have constraint objects.
                                                             Adding a non-unique clustered index does not reset the object create date.
                                                             Therefore, we cannot rely on the object "create_date" value in those cases  */
    , IndexKeyCols              = cs.KeyCols
    , IndexIncludecols          = cs.InclCols

    /* sys.indexes passthru */
    , IsUnique                  = i.is_unique
    , IndexDataSpaceID          = i.data_space_id
    , IgnoreDupKey              = i.[ignore_dup_key]
    , IsPrimaryKey              = i.is_primary_key
    , IsUniqueConstraint        = i.is_unique_constraint
    , [FillFactor]              = i.fill_factor
    , IsPadded                  = i.is_padded
    , IsDisabled                = i.is_disabled
    , IsHypothetical            = i.is_hypothetical
--  , IsIgnoredInOptimization   = i.is_ignored_in_optimization /* Undocumented column */
    , AllowRowLocks             = i.[allow_row_locks]
    , AllowPageLocks            = i.[allow_page_locks]
    , HasFilter                 = i.has_filter
    , FilterDefinition          = i.filter_definition
--  , CompressionDelay          = i.compression_delay
--  , SuppressDupKeyMessages    = i.suppress_dup_key_messages
--  , AutoCreated               = i.auto_created

    /*  Heaps don't have stats, so we'll just default these to zero so we can make the
        column not null, and make queries against these columns easier. */
    , StatNoRecompute           = CONVERT(bit, COALESCE(s.no_recompute, 0))
    , StatIsIncremental         = CONVERT(bit, COALESCE(s.is_incremental, 0))

    /*  For some reason joining to sys.filegroups causes this to take 4x as long to run. 
        Even reading sys.filegroups into a temp table and playing with join hints did nothing. */
    , IndexFileGroupName        = FILEGROUP_NAME(i.data_space_id)
    , IndexFileGroupIsDefault   = FILEGROUPPROPERTY(FILEGROUP_NAME(i.data_space_id), 'IsDefault')

    , HasFKReferences           = CONVERT(bit, IIF(fkc.FKReferenceCount > 0, 1, 0))
    , FKReferenceCount          = COALESCE(fkc.FKReferenceCount, 0)
    , CompressionType           = ixs.CompressionType /* Disabled indexes have NULL compression type */
    , StatsAgeMS                = DATEDIFF_BIG(MILLISECOND, r.BeginDate, @CollectionTime)
    , x.SeekCount, x.ScanCount, x.LookupCount, x.UpdateCount, x.ReadCount

    /* Stats from: sys.dm_db_index_operational_stats */
    , x.LeafInsertCount, x.LeafDeleteCount, x.LeafUpdateCount, x.LeafPageMergeCount, x.RangeScanCount, x.SingletonLookupCount
    , x.RowLockCount, x.RowLockWaitCount, x.RowLockWaitInMS
    , x.PageLockCount, x.PageLockWaitCount, x.PageLockWaitInMS
    , x.TotalLockWaitInMS
    , x.IndexLockPromotionAttemptCount, x.IndexLockPromotionCount
    , x.PageLatchWaitCount, x.PageLatchWaitInMS
    , x.PageIOLatchWaitCount, x.PageIOLatchWaitInMS

    , tz.LastSeek, tz.LastScan, tz.LastLookup, tz.LastUpdate, l.LastRead, tz.StatsDate
    , x.EstimatedIndexSizeKB, x.IndexRowCount
FROM sys.indexes i
    JOIN #tmp_object o ON o.[object_id] = i.[object_id]
    LEFT JOIN sys.stats s ON s.[object_id] = i.[object_id] AND s.stats_id = i.index_id
    LEFT JOIN #tmp_part ixs ON ixs.[object_id] = i.[object_id] AND ixs.index_id = i.index_id
    LEFT JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.[object_id] = i.[object_id] AND ius.index_id = i.index_id
    LEFT JOIN #tmp_fk fkc ON fkc.[object_id] = i.[object_id] AND fkc.index_id = i.index_id
    LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.[object_id] AND kc.unique_index_id = i.index_id
    LEFT JOIN #tmp_idx_op_stat os ON os.[object_id] = i.[object_id] AND os.index_id = i.index_id
    LEFT JOIN #tmp_idx_col_squash cs ON cs.[object_id] = i.[object_id] AND cs.index_id = i.index_id
    CROSS APPLY (
        SELECT SeekCount                      = COALESCE(ius.user_seeks, 0)
            ,  ScanCount                      = COALESCE(ius.user_scans, 0)
            ,  LookupCount                    = COALESCE(ius.user_lookups, 0)         /* Lookups only occur on clustered indexes */
            ,  UpdateCount                    = COALESCE(ius.user_updates, 0)
            ,  ReadCount                      = COALESCE(ius.user_seeks + ius.user_scans + ius.user_lookups, 0)

            ,  LeafInsertCount                = COALESCE(os.leaf_insert_count, 0)
            ,  LeafDeleteCount                = COALESCE(os.leaf_delete_count, 0)
            ,  LeafUpdateCount                = COALESCE(os.leaf_update_count, 0)
            ,  LeafPageMergeCount             = COALESCE(os.leaf_page_merge_count, 0)
            ,  RangeScanCount                 = COALESCE(os.range_scan_count, 0)
            ,  SingletonLookupCount           = COALESCE(os.singleton_lookup_count, 0)

            ,  RowLockCount                   = COALESCE(os.row_lock_count, 0)
            ,  RowLockWaitCount               = COALESCE(os.row_lock_wait_count, 0)
            ,  RowLockWaitInMS                = COALESCE(os.row_lock_wait_in_ms, 0)

            ,  PageLockCount                  = COALESCE(os.page_lock_count, 0)
            ,  PageLockWaitCount              = COALESCE(os.page_lock_wait_count, 0)
            ,  PageLockWaitInMS               = COALESCE(os.page_lock_wait_in_ms, 0)

            ,  TotalLockWaitInMS              = COALESCE(os.row_lock_wait_in_ms + os.page_lock_wait_in_ms, 0)

            ,  IndexLockPromotionAttemptCount = COALESCE(os.index_lock_promotion_attempt_count, 0)
            ,  IndexLockPromotionCount        = COALESCE(os.index_lock_promotion_count, 0)

            ,  PageLatchWaitCount             = COALESCE(os.page_latch_wait_count, 0)
            ,  PageLatchWaitInMS              = COALESCE(os.page_latch_wait_in_ms, 0)

            ,  PageIOLatchWaitCount           = COALESCE(os.page_io_latch_wait_count, 0)
            ,  PageIOLatchWaitInMS            = COALESCE(os.page_io_latch_wait_in_ms, 0)

            ,  EstimatedIndexSizeKB           = COALESCE(ixs.EstimatedIndexSizeKB, 0) /* Disabled indexes have NULL size */
            ,  IndexRowCount                  = COALESCE(ixs.IndexRowCount, 0)        /* Disabled indexes have NULL row count */
    ) x
    CROSS APPLY (
        /*  Unfortunately, SQL Server stores everything using system time, rather than UTC. Need to convert to UTC for historical storage */
        /*  First set the values to the local time zone (no shift), then convert to UTC (with shift) */
        SELECT ObjectCreateDate     = CONVERT(datetime2, o.create_date                         AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  ConstraintCreateDate = CONVERT(datetime2, kc.create_date                        AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  StatsDate            = CONVERT(datetime2, STATS_DATE(i.[object_id], i.index_id) AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  LastSeek             = CONVERT(datetime2, ius.last_user_seek                    AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  LastScan             = CONVERT(datetime2, ius.last_user_scan                    AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  LastLookup           = CONVERT(datetime2, ius.last_user_lookup                  AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
            ,  LastUpdate           = CONVERT(datetime2, ius.last_user_update                  AT TIME ZONE @LocalTZ AT TIME ZONE 'UTC')
    ) tz
    CROSS APPLY (SELECT LastRead = MAX(x.LastRead) FROM (VALUES (tz.LastSeek), (tz.LastScan), (tz.LastLookup)) x(LastRead)) l
    /*  This is a best attempt determination of when the stats snapshot we're currently taking likely began.

        Restarting SQL Server and restoring a database clears out the stats tables. However, if an index is
        dropped and recreated, then the age of the snapshot is as of the index create date instead. Since
        SQL Server does not provide an index create date in all cases, we then have to rely on the object
        create date instead.

        So the best solution (without creating some other process) is to take the max of all those dates.
    */
    CROSS APPLY (SELECT BeginDate = MAX(x.BeginDate) FROM (VALUES (@SQLServerStartTime), (@DBLastRestoreTime), (tz.ObjectCreateDate), (tz.ConstraintCreateDate)) x(BeginDate)) r
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = i.[object_id] AND p.index_id = i.index_id);
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) SET STATISTICS IO, TIME OFF;
