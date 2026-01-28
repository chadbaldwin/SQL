SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
DECLARE @debug bit = 0;
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

DROP TABLE IF EXISTS #tmp_object;
CREATE TABLE #tmp_object (
    [name]      nvarchar(128)   NOT NULL,
    [object_id] int             NOT NULL,
    [schema_id] int             NOT NULL,
    [type_desc] nvarchar(60)    NOT NULL,
    create_date datetime        NOT NULL,
    PRIMARY KEY CLUSTERED ([object_id])
);

INSERT #tmp_object
SELECT o.[name], o.[object_id], o.[schema_id], o.[type_desc], o.create_date
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

DROP TABLE IF EXISTS #tmp_part;
CREATE TABLE #tmp_part (
    [object_id]             int             NOT NULL,
    index_id                int             NOT NULL,
    EstimatedIndexSizeKB    bigint          NOT NULL,
    IndexRowCount           bigint          NOT NULL,
    CompressionType         nvarchar(60)    NOT NULL,
    PartitionCount          int             NOT NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

/* Table is aggregated in order to rollup partitions */
/* Returns rows for all indexes where is_disabled = 0 */
INSERT #tmp_part
SELECT s.[object_id], s.index_id
    , EstimatedIndexSizeKB  = SUM(s.used_page_count) * 8 /* would probably be easier to store value directly, but KB is more human to read */
    , IndexRowCount         = SUM(s.row_count)
    , CompressionType       = IIF(COUNT(DISTINCT p.data_compression_desc) > 1, '<<MIXED>>', MAX(p.data_compression_desc COLLATE DATABASE_DEFAULT)) /* Technically not accurate for partitioned indexes since each partition can have different compression types */
    , PartitionCount        = COUNT(*)
FROM sys.partitions p
    JOIN sys.dm_db_partition_stats s ON s.[partition_id] = p.[partition_id]
WHERE EXISTS (SELECT * FROM #tmp_object o WHERE o.[object_id] = s.[object_id])
GROUP BY s.[object_id], s.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.indexes',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_indexes;
CREATE TABLE #tmp_indexes (
    [object_id]          int           NOT NULL,
    [name]               nvarchar(128)     NULL,
    index_id             int           NOT NULL,
    [type_desc]          nvarchar(60)  NOT NULL,
    is_unique            bit           NOT NULL,
    data_space_id        int           NOT NULL,
    [ignore_dup_key]     bit           NOT NULL,
    is_primary_key       bit           NOT NULL,
    is_unique_constraint bit           NOT NULL,
    fill_factor          tinyint       NOT NULL,
    is_padded            bit           NOT NULL,
    is_disabled          bit           NOT NULL,
    is_hypothetical      bit           NOT NULL,
    [allow_row_locks]    bit           NOT NULL,
    [allow_page_locks]   bit           NOT NULL,
    has_filter           bit           NOT NULL,
    filter_definition    nvarchar(max) NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

INSERT #tmp_indexes
SELECT i.[object_id]
    , i.[name]
    , i.index_id
--  , i.[type]
    , i.[type_desc]
    , i.is_unique
    , i.data_space_id
    , i.[ignore_dup_key]
    , i.is_primary_key
    , i.is_unique_constraint
    , i.fill_factor
    , i.is_padded
    , i.is_disabled
    , i.is_hypothetical
--  , i.is_ignored_in_optimization /* Undocumented column */
    , i.[allow_row_locks]
    , i.[allow_page_locks]
    , i.has_filter
    , i.filter_definition
--  , i.[compression_delay]
--  , i.suppress_dup_key_messages
--  , i.auto_created
--  , i.[optimize_for_sequential_key]
FROM sys.indexes i
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = i.[object_id] AND p.index_id = i.index_id);
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.stats',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_stats;
CREATE TABLE #tmp_stats (
    [object_id]		int	NOT NULL,
    stats_id		int	NOT NULL,
    no_recompute	bit	NOT NULL,
    is_incremental	bit	NOT NULL,
    PRIMARY KEY CLUSTERED ([object_id], stats_id)
);

INSERT #tmp_stats
SELECT s.[object_id]
--  , s.[name]
    , s.stats_id
--  , s.auto_created
--  , s.user_created
    , s.no_recompute
--  , s.has_filter
--  , s.filter_definition
--  , s.is_temporary
    , s.is_incremental
--  , s.has_persisted_sample
--  , s.stats_generation_method
--  , s.stats_generation_method_desc
--  , s.auto_drop
FROM sys.stats s
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = s.[object_id] AND p.index_id = s.stats_id);
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get foreign key counts',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_fk;
CREATE TABLE #tmp_fk (
    [object_id]         int NOT NULL,
    index_id            int NOT NULL,
    FKReferenceCount    int NOT NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

/* FKs dont have to reference a PK, they can reference any unique index */
INSERT #tmp_fk ([object_id], index_id, FKReferenceCount)
SELECT [object_id]          = fk.referenced_object_id
    , index_id              = fk.key_index_id
    , FKReferenceCount      = COUNT(*)
FROM sys.foreign_keys fk
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = fk.referenced_object_id AND p.index_id = fk.key_index_id)
GROUP BY fk.referenced_object_id, fk.key_index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug =1) RAISERROR('Get sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_idx_op_stat;
CREATE TABLE #tmp_idx_op_stat (
    [object_id]                        int    NOT NULL,
    index_id                           int    NOT NULL,
    leaf_insert_count                  bigint NOT NULL,
    leaf_delete_count                  bigint NOT NULL,
    leaf_update_count                  bigint NOT NULL,
    leaf_allocation_count              bigint NOT NULL,
    leaf_page_merge_count              bigint NOT NULL,
    range_scan_count                   bigint NOT NULL,
    singleton_lookup_count             bigint NOT NULL,
    forwarded_fetch_count              bigint NOT NULL,
    row_lock_count                     bigint NOT NULL,
    row_lock_wait_count                bigint NOT NULL,
    row_lock_wait_in_ms                bigint NOT NULL,
    page_lock_count                    bigint NOT NULL,
    page_lock_wait_count               bigint NOT NULL,
    page_lock_wait_in_ms               bigint NOT NULL,
    index_lock_promotion_attempt_count bigint NOT NULL,
    index_lock_promotion_count         bigint NOT NULL,
    page_latch_wait_count              bigint NOT NULL,
    page_latch_wait_in_ms              bigint NOT NULL,
    page_io_latch_wait_count           bigint NOT NULL,
    page_io_latch_wait_in_ms           bigint NOT NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

/* Table is aggregated in order to rollup partitions */
INSERT #tmp_idx_op_stat
SELECT x.[object_id], x.index_id
    , leaf_insert_count                  = SUM(x.leaf_insert_count)
    , leaf_delete_count                  = SUM(x.leaf_delete_count)
    , leaf_update_count                  = SUM(x.leaf_update_count)
--  , leaf_ghost_count                   = SUM(x.leaf_ghost_count)
--  , nonleaf_insert_count               = SUM(x.nonleaf_insert_count)
--  , nonleaf_delete_count               = SUM(x.nonleaf_delete_count)
--  , nonleaf_update_count               = SUM(x.nonleaf_update_count)
    , leaf_allocation_count              = SUM(x.leaf_allocation_count)
--  , nonleaf_allocation_count           = SUM(x.nonleaf_allocation_count)
    , leaf_page_merge_count              = SUM(x.leaf_page_merge_count)
--  , nonleaf_page_merge_count           = SUM(x.nonleaf_page_merge_count)
    , range_scan_count                   = SUM(x.range_scan_count)
    , singleton_lookup_count             = SUM(x.singleton_lookup_count)
    , forwarded_fetch_count              = SUM(x.forwarded_fetch_count)
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
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) x
WHERE EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = x.[object_id] AND p.index_id = x.index_id)
GROUP BY x.database_id, x.[object_id], x.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF (@debug = 1) RAISERROR('Get sys.index_columns',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_idx_col_squash;
CREATE TABLE #tmp_idx_col_squash (
    [object_id] int             NOT NULL,
    index_id    int             NOT NULL,
    KeyCols     nvarchar(4000)  NOT NULL,
    InclCols    nvarchar(4000)      NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

INSERT #tmp_idx_col_squash
SELECT ic.[object_id], ic.index_id 
    , KeyCols  = STRING_AGG(x.KeyColName , ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, c.[name])
    , InclCols = STRING_AGG(x.InclColName, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, c.[name])
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
IF (@debug = 1) RAISERROR('Get sys.dm_db_index_usage_stats',0,1) WITH NOWAIT;

DROP TABLE IF EXISTS #tmp_dm_db_index_usage_stats;
CREATE TABLE #tmp_dm_db_index_usage_stats (
    [object_id]         int         NOT NULL,
    index_id            int         NOT NULL,
    user_seeks          bigint      NOT NULL,
    user_scans          bigint      NOT NULL,
    user_lookups        bigint      NOT NULL,
    user_updates        bigint      NOT NULL,
    last_user_seek      datetime        NULL,
    last_user_scan      datetime        NULL,
    last_user_lookup    datetime        NULL,
    last_user_update    datetime        NULL,
    PRIMARY KEY CLUSTERED ([object_id], index_id)
);

INSERT #tmp_dm_db_index_usage_stats
SELECT ius.[object_id], ius.index_id
    , user_seeks, user_scans, user_lookups, user_updates
    , ius.last_user_seek, ius.last_user_scan, ius.last_user_lookup, ius.last_user_update
FROM sys.dm_db_index_usage_stats ius
WHERE ius.database_id = DB_ID()
    AND EXISTS (SELECT * FROM #tmp_part p WHERE p.[object_id] = ius.[object_id] AND p.index_id = ius.index_id);
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

    , IndexDataSpaceName        = ds.[name]
    , IndexDataSpaceIsDefault   = ds.is_default
    , IndexDataSpaceType        = ds.[type_desc]

    , HasFKReferences           = CONVERT(bit, IIF(fkc.FKReferenceCount > 0, 1, 0))
    , FKReferenceCount          = COALESCE(fkc.FKReferenceCount, 0)
    , CompressionType           = ixs.CompressionType /* Disabled indexes have NULL compression type */
    , PartitionCount            = ixs.PartitionCount
    , StatsAgeMS                = DATEDIFF_BIG(MILLISECOND, r.BeginDate, @CollectionTime)

    , x.SeekCount, x.ScanCount, x.LookupCount, x.UpdateCount, x.ReadCount

    /* Stats from: sys.dm_db_index_operational_stats */
    , x.LeafInsertCount, x.LeafDeleteCount, x.LeafUpdateCount, x.LeafAllocationCount, x.LeafPageMergeCount, x.RangeScanCount, x.SingletonLookupCount, x.ForwardedFetchCount
    , x.RowLockCount, x.RowLockWaitCount, x.RowLockWaitInMS
    , x.PageLockCount, x.PageLockWaitCount, x.PageLockWaitInMS
    , x.TotalLockWaitInMS
    , x.IndexLockPromotionAttemptCount, x.IndexLockPromotionCount
    , x.PageLatchWaitCount, x.PageLatchWaitInMS
    , x.PageIOLatchWaitCount, x.PageIOLatchWaitInMS

    , tz.LastSeek, tz.LastScan, tz.LastLookup, tz.LastUpdate, l.LastRead, tz.StatsDate
    , x.EstimatedIndexSizeKB, x.IndexRowCount
FROM #tmp_indexes i
    JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
    JOIN #tmp_object o ON o.[object_id] = i.[object_id]
    JOIN #tmp_part ixs ON ixs.[object_id] = i.[object_id] AND ixs.index_id = i.index_id

    -- Yes, all of these need to be left joins for various reasons - heaps, disabled indexes, no constraints, etc
    LEFT JOIN #tmp_stats s ON s.[object_id] = i.[object_id] AND s.stats_id = i.index_id
    LEFT JOIN #tmp_dm_db_index_usage_stats ius ON ius.[object_id] = i.[object_id] AND ius.index_id = i.index_id
    LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.[object_id] AND kc.unique_index_id = i.index_id
    LEFT JOIN #tmp_fk fkc ON fkc.[object_id] = i.[object_id] AND fkc.index_id = i.index_id
    LEFT JOIN #tmp_idx_op_stat os ON os.[object_id] = i.[object_id] AND os.index_id = i.index_id
    LEFT JOIN #tmp_idx_col_squash cs ON cs.[object_id] = i.[object_id] AND cs.index_id = i.index_id

    -- Handle NULLs
    CROSS APPLY (
        SELECT SeekCount                      = COALESCE(ius.user_seeks, 0)
            ,  ScanCount                      = COALESCE(ius.user_scans, 0)
            ,  LookupCount                    = COALESCE(ius.user_lookups, 0)         /* Lookups only occur on clustered indexes */
            ,  UpdateCount                    = COALESCE(ius.user_updates, 0)
            ,  ReadCount                      = COALESCE(ius.user_seeks + ius.user_scans + ius.user_lookups, 0)

            ,  LeafInsertCount                = COALESCE(os.leaf_insert_count, 0)
            ,  LeafDeleteCount                = COALESCE(os.leaf_delete_count, 0)
            ,  LeafUpdateCount                = COALESCE(os.leaf_update_count, 0)
            ,  LeafAllocationCount            = COALESCE(os.leaf_allocation_count, 0)
            ,  LeafPageMergeCount             = COALESCE(os.leaf_page_merge_count, 0)
            ,  RangeScanCount                 = COALESCE(os.range_scan_count, 0)
            ,  SingletonLookupCount           = COALESCE(os.singleton_lookup_count, 0)
            ,  ForwardedFetchCount            = COALESCE(os.forwarded_fetch_count, 0)

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
    -- Handle time zone conversions
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
