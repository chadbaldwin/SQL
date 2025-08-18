/* WORK IN PROGRESS - Set filters on main search query. 
   The script will loop every 5 seconds until a matching session is identified.
   It will pick the longest running session and then grab just about every
   piece of information you can for that session.

  Only runs on 2019+ due to some new system DMVs.
*/

-- Set session to use target DB you are most likely wanting to identify something running in
-- USE FooBar;
------------------------------------------------------------
GO  
------------------------------------------------------------
SET DEADLOCK_PRIORITY LOW;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
------------------------------------------------------------
GO
------------------------------------------------------------
DROP TABLE IF EXISTS  #dm_exec_connections
                    , #dm_exec_sessions
                    , #dm_exec_requests
                    , #dm_exec_input_buffer
                    , #dm_exec_plan_and_text
                    , #dm_exec_query_profiles
                    , #query_store_query
                    , #query_store_plan
                    , #query_store_runtime_stats
                    , #query_store_wait_stats
                    , #query_store_plan_feedback
                    , #dm_exec_query_stats
                    , #stats_tables
                    , #dm_db_session_space_usage
                    , #dm_db_task_space_usage
                    , #dm_cdc_log_scan_sessions
                    , #dm_exec_query_memory_grants
                    , #dm_os_tasks
                    , #dm_os_waiting_tasks
                    , #dm_exec_cursors
                    , #dm_exec_xml_handles
                    , #dm_tran_session_transactions
                    , #dm_tran_active_transactions
                    , #dm_tran_database_transactions
                    , #dm_tran_active_snapshot_database_transactions
                    , #dm_db_page_info
                    , #dm_exec_session_wait_stats
                    , #dm_tran_locks;
------------------------------------------------------------
GO
------------------------------------------------------------
DECLARE @session_id int,
        @request_id int,
        @plan_handle varbinary(64),
        @sql_handle varbinary(64),
        @stmt_sql_handle varbinary(64),
        @stmt_start int,
        @stmt_end int,
        @batch_text nvarchar(MAX),
        @stmt_text nvarchar(MAX),
        @ib_text nvarchar(MAX),
        @txid bigint,
        @page_resource varbinary(8),
        @query_hash binary(8),
        @plan_hash binary(8),
        @db_id int,
        @object_id int,
        @start_time datetime,
        @blocking_session_id int;

WHILE (@session_id IS NULL)
BEGIN;
    WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
    SELECT TOP(1) @session_id = c.[session_id]
        , @request_id = r.request_id
        , @plan_handle = NULLIF(r.plan_handle,0x0)
        , @sql_handle = NULLIF(r.[sql_handle],0x0)
        , @stmt_sql_handle = r.statement_sql_handle
        , @stmt_start = r.statement_start_offset
        , @stmt_end = r.statement_end_offset
        , @batch_text = COALESCE(st_ph.[text], st_sh.[text])
        , @stmt_text = SUBSTRING(COALESCE(st_ph.[text], st_sh.[text]), r.statement_start_offset/2+1, IIF(r.statement_end_offset = -1, DATALENGTH(COALESCE(st_ph.[text], st_sh.[text])), (r.statement_end_offset-r.statement_start_offset)/2+1))
        , @txid = r.transaction_id
        , @ib_text = ib.event_info
        , @page_resource = r.page_resource
        , @query_hash = r.query_hash
        , @plan_hash = r.query_plan_hash
        , @db_id = COALESCE(st_ph.[dbid], st_sh.[dbid], tqp.[dbid])
        , @object_id = COALESCE(st_ph.objectid, st_sh.objectid, tqp.objectid)
        , @start_time = r.start_time
        , @blocking_session_id = NULLIF(r.blocking_session_id, 0)
    FROM sys.dm_exec_connections c
        JOIN sys.dm_exec_sessions s ON s.[session_id] = c.[session_id]
        LEFT JOIN sys.dm_exec_requests r ON r.[session_id] = c.[session_id]
        OUTER APPLY sys.dm_exec_sql_text(r.plan_handle) st_ph
        OUTER APPLY sys.dm_exec_sql_text(r.[sql_handle]) st_sh
        OUTER APPLY sys.dm_exec_input_buffer(r.[session_id], r.request_id) ib
        OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle, r.statement_start_offset, r.statement_end_offset) tqp
        CROSS APPLY (SELECT query_plan = CONVERT(xml, tqp.query_plan)) x
    WHERE 1=1
        AND c.[session_id] <> @@SPID
        AND s.[status] NOT IN ('background','sleeping')
        AND r.[status] NOT IN ('background','sleeping')
--
-- Customize filters to identify the session you want to target
    AND s.login_name NOT IN ('NT AUTHORITY\SYSTEM','sa')
    --  AND COALESCE(st_ph.[text], st_sh.[text]) NOT IN ('xp_cmdshell','xp_backup_log')
    --  AND r.[command] NOT IN ('BACKUP LOG')
    --  AND c.[session_id] = 2839
    --  AND r.command LIKE 'UPDATE%' -- command/action is an update statement
        AND r.total_elapsed_time > 15000 -- running for at least N seconds
    --  AND r.blocking_session_id <> 0 -- is blocked
        AND r.blocking_session_id = 0 -- not blocked
        AND EXISTS (SELECT * FROM sys.dm_exec_requests br WHERE br.blocking_session_id = r.[session_id]) -- is blocking
    --  AND x.query_plan.exist('//Update/Object[lower-case(string(@Table))=("[table_name_1]","[table_name_2]")]') = 1 -- work in progress, doesn't _always_ work
    ORDER BY r.total_elapsed_time DESC;
    -------------------------------------------------------------------------------------------------

    -------------------------------------------------------------------------------------------------
    IF (@session_id IS NULL)
    BEGIN;
        RAISERROR('.',0,1) WITH NOWAIT;
        WAITFOR DELAY '00:00:05.000';
        CONTINUE;
    END;

    RAISERROR(N'███████████████████████████████ - Input buffer',0,1) WITH NOWAIT;
    RAISERROR('%s',0,1,@ib_text) WITH NOWAIT;
    RAISERROR(N'███████████████████████████████ - Statement text',0,1) WITH NOWAIT;
    RAISERROR('%s',0,1,@stmt_text) WITH NOWAIT;
    RAISERROR(N'███████████████████████████████ - Batch text',0,1) WITH NOWAIT;
    RAISERROR('%s',0,1,@batch_text) WITH NOWAIT;
    RAISERROR(N'███████████████████████████████',0,1) WITH NOWAIT;

    SELECT run_time = CONCAT(FORMAT(DATEDIFF(DAY, @start_time, GETDATE()),'0#'), ' ', FORMAT(DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @start_time, GETDATE()), 0),'HH:mm:ss.fff'))
        , [session_id] = @session_id
        , request_id = @request_id
        , transaction_id = @txid
        , stmt_start = @stmt_start
        , stmt_end = @stmt_end
        , page_resource = @page_resource;
    SELECT 'Handles and hashes'
        , [sql_handle] = @sql_handle
        , plan_handle = @plan_handle
        , stmt_sql_handle = @stmt_sql_handle
        , query_hash = @query_hash
        , plan_hash = @plan_hash;
    SELECT 'Text'
        , input_buffer = @ib_text
        , batch_text = @batch_text
        , statement_text = @stmt_text;
    SELECT 'Object info'
        , [db_id] = @db_id
        , [object_id] = @object_id
        , [database_name] = DB_NAME(@db_id)
        , [schema_name] = OBJECT_SCHEMA_NAME(@object_id, @db_id)
        , [object_name] = OBJECT_NAME(@object_id, @db_id);
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Top level stats - Connection/Session/Request
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'  Top level info'   , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #dm_exec_connections FROM sys.dm_exec_connections WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_connections) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_connections')+N'█', * FROM #dm_exec_connections; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_sessions FROM sys.dm_exec_sessions WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_sessions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_sessions')+N'█', * FROM #dm_exec_sessions; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_requests FROM sys.dm_exec_requests WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_requests) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests')+N'█', * FROM #dm_exec_requests; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        IF (@blocking_session_id IS NOT NULL) --AND 1=0
        BEGIN;
            SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests - blocked by')+N'█', x.*, y.*, N'█' [█]
                , r.*
            FROM sys.dm_exec_requests r
                OUTER APPLY (
                    SELECT DatabaseID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 3))
                        , FileID = NULL
                        , PageID = NULL
                        , ObjectID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 2))
                        , HOBTID = NULL
                        , IndexID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 1))
                    WHERE r.wait_resource LIKE 'OBJECT:%'
                    UNION ALL
                    SELECT x.database_id, x.[file_id], x.page_id, x.[object_id], NULL, x.index_id
                    FROM sys.fn_PageResCracker(r.page_resource) c
                        CROSS APPLY sys.dm_db_page_info(c.[db_id], c.[file_id], c.page_id, 'LIMITED') x
                    WHERE r.wait_resource LIKE 'PAGE:%'
                    UNION ALL
                    SELECT DatabaseID = CONVERT(int, PARSENAME(REPLACE(SUBSTRING(r.wait_resource, 1, CHARINDEX(' (', r.wait_resource)),':','.'), 2))
                        , FileID = NULL
                        , PageID = NULL
                        , ObjectID = NULL
                        , HOBTID = PARSENAME(REPLACE(SUBSTRING(r.wait_resource, 1, CHARINDEX(' (', r.wait_resource)),':','.'), 1)
                        , IndexID = NULL
                    WHERE r.wait_resource LIKE 'KEY:%'
                ) x
                CROSS APPLY (
                    SELECT DatabaseName = DB_NAME(x.DatabaseID)
                        , SchemaName = OBJECT_SCHEMA_NAME(x.ObjectID, x.DatabaseID)
                        , ObjectName =  CASE
                                            WHEN DB_NAME(x.DatabaseID) = 'tempdb' THEN SUBSTRING(OBJECT_NAME(x.ObjectID, x.DatabaseID), 1, CHARINDEX('_____', OBJECT_NAME(x.ObjectID, x.DatabaseID)))
                                            ELSE OBJECT_NAME(x.ObjectID, x.DatabaseID)
                                        END
                ) y
            WHERE r.[session_id] = @blocking_session_id;
            RAISERROR('.',0,1) WITH NOWAIT;
        END;

        IF EXISTS (SELECT * FROM sys.dm_exec_requests WHERE blocking_session_id = @session_id) --AND 1=0
        BEGIN;
            SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests - blocking')+N'█', x.*, y.*, N'█' [█]
                , r.*
            FROM sys.dm_exec_requests r
                OUTER APPLY (
                    SELECT DatabaseID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 3))
                        , FileID = NULL
                        , PageID = NULL
                        , ObjectID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 2))
                        , HOBTID = NULL
                        , IndexID = CONVERT(int, PARSENAME(REPLACE(r.wait_resource,':','.'), 1))
                    WHERE r.wait_resource LIKE 'OBJECT:%'
                    UNION ALL
                    SELECT x.database_id, x.[file_id], x.page_id, x.[object_id], NULL, x.index_id
                    FROM sys.fn_PageResCracker(r.page_resource) c
                        CROSS APPLY sys.dm_db_page_info(c.[db_id], c.[file_id], c.page_id, 'LIMITED') x
                    WHERE r.wait_resource LIKE 'PAGE:%'
                    UNION ALL
                    SELECT DatabaseID = CONVERT(int, PARSENAME(REPLACE(SUBSTRING(r.wait_resource, 1, CHARINDEX(' (', r.wait_resource)),':','.'), 2))
                        , FileID = NULL
                        , PageID = NULL
                        , ObjectID = NULL
                        , HOBTID = PARSENAME(REPLACE(SUBSTRING(r.wait_resource, 1, CHARINDEX(' (', r.wait_resource)),':','.'), 1)
                        , IndexID = NULL
                    WHERE r.wait_resource LIKE 'KEY:%'
                ) x
                CROSS APPLY (
                    SELECT DatabaseName = DB_NAME(x.DatabaseID)
                        , SchemaName = OBJECT_SCHEMA_NAME(x.ObjectID, x.DatabaseID)
                        , ObjectName =  CASE
                                            WHEN DB_NAME(x.DatabaseID) = 'tempdb' THEN SUBSTRING(OBJECT_NAME(x.ObjectID, x.DatabaseID), 1, CHARINDEX('_____', OBJECT_NAME(x.ObjectID, x.DatabaseID)))
                                            ELSE OBJECT_NAME(x.ObjectID, x.DatabaseID)
                                        END
                ) y
            WHERE r.blocking_session_id = @session_id;
            RAISERROR('.',0,1) WITH NOWAIT;
        END;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Query plan and text
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'Query plan and text', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #dm_exec_input_buffer FROM sys.dm_exec_input_buffer(@session_id, @request_id);
        IF EXISTS (SELECT * FROM #dm_exec_input_buffer) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_input_buffer')+N'█', * FROM #dm_exec_input_buffer; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
        SELECT x.dmv, x.[dbid], x.objectid, x.[encrypted], x.sql_text, y.query_plan_xml, x.query_plan_text, x.[description]
            , plan_stmt_count = y.query_plan_xml.value('count(/ShowPlanXML/BatchSequence/Batch/Statements/*)', 'int')
            , missing_idx_count = y.query_plan_xml.value('count(/ShowPlanXML/BatchSequence/Batch/Statements/*//MissingIndex)', 'int')
            , statement_text = SUBSTRING(x.sql_text, @stmt_start/2+1, IIF(@stmt_end = -1, DATALENGTH(x.sql_text), (@stmt_end-@stmt_start)/2+1))
        INTO #dm_exec_plan_and_text
        FROM (
                  SELECT dmv = 'sys.dm_exec_sql_text(@plan_handle)'                 , st.[dbid], st.objectid, st.[encrypted], sql_text = st.[text]  , query_plan_xml = NULL                                 , query_plan_text = NULL            , [description] = 'Returns the text of the SQL batch that is identified by the specified sql_handle.' FROM sys.dm_exec_sql_text(@plan_handle) st
            UNION SELECT dmv = 'sys.dm_exec_sql_text(@sql_handle)'                  , st.[dbid], st.objectid, st.[encrypted], sql_text = st.[text]  , query_plan_xml = NULL                                 , query_plan_text = NULL            , [description] = 'Returns the text of the SQL batch that is identified by the specified sql_handle.' FROM sys.dm_exec_sql_text(@sql_handle) st
            UNION SELECT dmv = 'sys.dm_exec_query_plan(@plan_handle)'               , st.[dbid], st.objectid, st.[encrypted], sql_text = NULL       , query_plan_xml = CONVERT(nvarchar(MAX), st.query_plan), query_plan_text = NULL            , [description] = 'Returns the Showplan in XML format for the batch specified by the plan handle.' FROM sys.dm_exec_query_plan(@plan_handle) st
            UNION SELECT dmv = 'sys.dm_exec_query_plan_stats(@plan_handle)'         , st.[dbid], st.objectid, st.[encrypted], sql_text = NULL       , query_plan_xml = CONVERT(nvarchar(MAX), st.query_plan), query_plan_text = NULL            , [description] = 'Returns the equivalent of the last known actual execution plan for a previously cached query plan.' FROM sys.dm_exec_query_plan_stats(@plan_handle) st
            UNION SELECT dmv = 'sys.dm_exec_text_query_plan(@plan_handle)'          , st.[dbid], st.objectid, st.[encrypted], sql_text = NULL       , query_plan_xml = st.query_plan                        , query_plan_text = st.query_plan   , [description] = 'Returns the Showplan in text format for a Transact-SQL batch or for a specific statement within the batch.' FROM sys.dm_exec_text_query_plan(@plan_handle, @stmt_start, @stmt_end) st
            UNION SELECT dmv = 'sys.dm_exec_text_query_plan(@plan_handle, 0, -1)'   , st.[dbid], st.objectid, st.[encrypted], sql_text = NULL       , query_plan_xml = st.query_plan                        , query_plan_text = st.query_plan   , [description] = 'Returns the Showplan in text format for a Transact-SQL batch or for a specific statement within the batch.' FROM sys.dm_exec_text_query_plan(@plan_handle, 0, -1) st
            UNION SELECT dmv = 'sys.dm_exec_query_statistics_xml(@session_id)'      , NULL     , NULL       , NULL          , sql_text = NULL       , query_plan_xml = CONVERT(nvarchar(MAX), st.query_plan), query_plan_text = NULL            , [description] = 'Returns query execution plan for in-flight requests.' FROM sys.dm_exec_query_statistics_xml(@session_id) st
        ) x
            CROSS APPLY (SELECT query_plan_xml = TRY_CONVERT(xml, x.query_plan_xml)) y;

        SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
            , x.[dbid], x.objectid, x.[encrypted], x.sql_text, x.query_plan_xml, x.query_plan_text, x.[description]
            , N'█' [█], x.plan_stmt_count, x.missing_idx_count, x.statement_text
        FROM #dm_exec_plan_and_text x
        ORDER BY x.dmv;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_query_profiles FROM sys.dm_exec_query_profiles WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_query_profiles)
        BEGIN;
            SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_profiles')+N'█'
                , ce_accuracy_pct = FORMAT(row_count / NULLIF((estimate_row_count * 1.0), 0), 'P0')
                , row_count = FORMAT(row_count, 'N0')
                , N'█' [█], *
            FROM #dm_exec_query_profiles
            ORDER BY node_id, thread_id;
        END;
        RAISERROR('.',0,1) WITH NOWAIT;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Query Store
    ------------------------------------------------------------
    DECLARE @qs_query_id bigint, @qs_plan_id bigint;
    SELECT @qs_query_id = query_id FROM sys.query_store_query WHERE batch_sql_handle = @sql_handle AND query_hash = @query_hash;
    SELECT @qs_plan_id = plan_id FROM sys.query_store_plan WHERE query_id = @qs_query_id AND query_plan_hash = @plan_hash;

    IF (@qs_query_id IS NOT NULL AND @qs_plan_id IS NOT NULL)
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Query Store'    , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #query_store_query FROM sys.query_store_query WHERE query_id = @qs_query_id;
        IF EXISTS (SELECT * FROM #query_store_query) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_query - by query_id')+N'█', * FROM #query_store_query; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #query_store_plan FROM sys.query_store_plan WHERE plan_id = @qs_plan_id;
        IF EXISTS (SELECT * FROM #query_store_plan) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_plan - by plan_id')+N'█', * FROM #query_store_plan; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #query_store_runtime_stats FROM sys.query_store_runtime_stats WHERE plan_id = @qs_plan_id;
        IF EXISTS (SELECT * FROM #query_store_runtime_stats) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_runtime_stats - by plan_id')+N'█', * FROM #query_store_runtime_stats ORDER BY last_execution_time DESC; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #query_store_wait_stats FROM sys.query_store_wait_stats WHERE plan_id = @qs_plan_id;
        IF EXISTS (SELECT * FROM #query_store_wait_stats) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_wait_stats - by plan_id')+N'█', * FROM #query_store_wait_stats ORDER BY runtime_stats_interval_id, wait_category; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #query_store_plan_feedback FROM sys.query_store_plan_feedback WHERE plan_id = @qs_plan_id;
        IF EXISTS (SELECT * FROM #query_store_plan_feedback) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_plan_feedback - by plan_id')+N'█', * FROM #query_store_plan_feedback; END;
        RAISERROR('.',0,1) WITH NOWAIT;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Various stats tables
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Stats views'    , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #dm_exec_query_stats FROM sys.dm_exec_query_stats x WHERE x.plan_handle = @plan_handle OR x.[sql_handle] = @sql_handle;
        IF EXISTS(SELECT * FROM #dm_exec_query_stats)
        BEGIN;
            SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_stats')+N'█'
                , ph = IIF(x.plan_handle = @plan_handle, N'☑️', '')
                , sh = IIF(x.[sql_handle] = @sql_handle, N'☑️', '')
                , [off] = IIF(x.statement_start_offset = @stmt_start AND x.statement_end_offset = @stmt_end, N'☑️', '')
                , N'█' [█]
                , *
            FROM #dm_exec_query_stats x
            ORDER BY x.[sql_handle], x.plan_handle, x.statement_start_offset;
            RAISERROR('.',0,1) WITH NOWAIT;
        END;

        SELECT *
        INTO #stats_tables
        FROM (
            SELECT dmv = 'sys.dm_exec_procedure_stats', *
            FROM sys.dm_exec_procedure_stats
            WHERE [sql_handle] = @sql_handle
                OR plan_handle = @plan_handle
                OR (database_id = @db_id AND [object_id] = @object_id)
            UNION ALL
            SELECT dmv = 'sys.dm_exec_function_stats'
                , database_id, [object_id], [type], [type_desc], [sql_handle], plan_handle, cached_time, last_execution_time, execution_count, total_worker_time, last_worker_time, min_worker_time, max_worker_time, total_physical_reads, last_physical_reads, min_physical_reads, max_physical_reads, total_logical_writes, last_logical_writes, min_logical_writes, max_logical_writes, total_logical_reads, last_logical_reads, min_logical_reads, max_logical_reads, total_elapsed_time, last_elapsed_time, min_elapsed_time, max_elapsed_time
                , total_spills = NULL, last_spills = NULL, min_spills = NULL, max_spills = NULL
                , total_num_physical_reads, last_num_physical_reads, min_num_physical_reads, max_num_physical_reads, total_page_server_reads, last_page_server_reads, min_page_server_reads, max_page_server_reads, total_num_page_server_reads, last_num_page_server_reads, min_num_page_server_reads, max_num_page_server_reads
            FROM sys.dm_exec_function_stats x
            WHERE [sql_handle] = @sql_handle
                OR plan_handle = @plan_handle
                OR (database_id = @db_id AND [object_id] = @object_id)
            UNION ALL
            SELECT dmv = 'sys.dm_exec_trigger_stats', *
            FROM sys.dm_exec_trigger_stats x
            WHERE [sql_handle] = @sql_handle
                OR plan_handle = @plan_handle
                OR (database_id = @db_id AND [object_id] = @object_id)
        ) x;

        IF EXISTS (SELECT * FROM #stats_tables)
        BEGIN;
            SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
                , ph = IIF(x.plan_handle = @plan_handle, N'☑️', '')
                , sh = IIF(x.[sql_handle] = @sql_handle, N'☑️', '')
                , obj = IIF(x.database_id = @db_id AND x.[object_id] = @object_id, N'☑️', '')
                , N'█' [█]
                , x.*
            FROM #stats_tables x;

            SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
                , plans_per_query = COUNT(DISTINCT IIF([sql_handle] = @sql_handle, plan_handle, NULL))
                , queries_per_plan = COUNT(DISTINCT IIF([plan_handle] = @plan_handle, [sql_handle], NULL))
                , plans_per_object = COUNT(DISTINCT IIF([object_id] = @object_id, plan_handle, NULL))
            FROM #stats_tables x
            GROUP BY dmv;
        END;
        RAISERROR('.',0,1) WITH NOWAIT;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Misc
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'        Misc'       , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #dm_db_session_space_usage FROM sys.dm_db_session_space_usage WHERE [session_id] = @session_id;
        IF EXISTS(SELECT * FROM #dm_db_session_space_usage) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_session_space_usage')+N'█', * FROM #dm_db_session_space_usage; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_db_task_space_usage FROM sys.dm_db_task_space_usage WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_db_task_space_usage) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_task_space_usage')+N'█', * FROM #dm_db_task_space_usage ORDER BY exec_context_id; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_cdc_log_scan_sessions FROM sys.dm_cdc_log_scan_sessions WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_cdc_log_scan_sessions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_cdc_log_scan_sessions')+N'█', * FROM #dm_cdc_log_scan_sessions; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_query_memory_grants FROM sys.dm_exec_query_memory_grants WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_query_memory_grants) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_memory_grants')+N'█', * FROM #dm_exec_query_memory_grants; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_os_tasks FROM sys.dm_os_tasks WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_os_tasks) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_os_tasks')+N'█', * FROM #dm_os_tasks ORDER BY exec_context_id; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_os_waiting_tasks FROM sys.dm_os_waiting_tasks WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_os_waiting_tasks) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_os_waiting_tasks')+N'█', * FROM #dm_os_waiting_tasks ORDER BY exec_context_id, blocking_session_id, blocking_exec_context_id; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_cursors FROM sys.dm_exec_cursors(@session_id);
        IF EXISTS (SELECT * FROM #dm_exec_cursors) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_cursors')+N'█', * FROM #dm_exec_cursors; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_exec_xml_handles FROM sys.dm_exec_xml_handles(@session_id);
        IF EXISTS (SELECT * FROM #dm_exec_xml_handles) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_xml_handles')+N'█', * FROM #dm_exec_xml_handles; END;
        RAISERROR('.',0,1) WITH NOWAIT;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Transaction info
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N' Transaction info'  , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        SELECT * INTO #dm_tran_session_transactions FROM sys.dm_tran_session_transactions WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_tran_session_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_session_transactions')+N'█', * FROM #dm_tran_session_transactions; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_tran_active_transactions FROM sys.dm_tran_active_transactions WHERE transaction_id = @txid;
        IF EXISTS (SELECT * FROM #dm_tran_active_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_active_transactions')+N'█', * FROM #dm_tran_active_transactions; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT transaction_id, database_id
            , begin_time                = database_transaction_begin_time
            , [type]                    = database_transaction_type
            , [state]                   = database_transaction_state
            , [status]                  = database_transaction_status
            , status2                   = database_transaction_status2
            , log_record_count          = database_transaction_log_record_count
            , replicate_record_count    = database_transaction_replicate_record_count
            , log_bytes_used            = database_transaction_log_bytes_used
            , log_bytes_reserved        = database_transaction_log_bytes_reserved
            , log_bytes_used_system     = database_transaction_log_bytes_used_system
            , log_bytes_reserved_system = database_transaction_log_bytes_reserved_system
            , begin_lsn                 = database_transaction_begin_lsn
            , last_lsn                  = database_transaction_last_lsn
            , most_recent_savepoint_lsn = database_transaction_most_recent_savepoint_lsn
            , commit_lsn                = database_transaction_commit_lsn
            , last_rollback_lsn         = database_transaction_last_rollback_lsn
            , next_undo_lsn             = database_transaction_next_undo_lsn
        INTO #dm_tran_database_transactions
        FROM sys.dm_tran_database_transactions WHERE transaction_id = @txid;
        IF EXISTS (SELECT * FROM #dm_tran_database_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_database_transactions')+N'█', * FROM #dm_tran_database_transactions; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        SELECT * INTO #dm_tran_active_snapshot_database_transactions FROM sys.dm_tran_active_snapshot_database_transactions WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_tran_active_snapshot_database_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_active_snapshot_database_transactions')+N'█', * FROM #dm_tran_active_snapshot_database_transactions; END;
        RAISERROR('.',0,1) WITH NOWAIT;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
    -- Locks and wait resources
    ------------------------------------------------------------
    BEGIN;
                  SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'  Locks and waits'  , [ ] = N'██████████████████████████████████████████████████'
        UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

        IF (@page_resource IS NOT NULL)
        BEGIN;
            SELECT pgi.*
            INTO #dm_db_page_info
            FROM sys.fn_PageResCracker(@page_resource) prc
                CROSS APPLY sys.dm_db_page_info(prc.[db_id], prc.[file_id], prc.page_id, 'DETAILED') pgi;

            IF EXISTS (SELECT * FROM #dm_db_page_info)
            BEGIN;
                SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_page_info')+N'█'
                    , [database_name] = DB_NAME(database_id)
                    , [schema_name] = OBJECT_SCHEMA_NAME([object_id], database_id)
                    , [object_name] = OBJECT_NAME([object_id], database_id)
                    , pgi.index_id
                    , N'█' [█], *
                FROM #dm_db_page_info;
                RAISERROR('.',0,1) WITH NOWAIT;
            END;
        END;

        SELECT wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
            , [description] = CASE
                WHEN wait_type LIKE 'PAGELATCH[_]%' THEN 'Accessing pages in memory'
                WHEN wait_type LIKE 'PAGEIOLATCH[_]%' THEN 'Pulling pages from disk into memory buffers'
                WHEN wait_type = 'WRITELOG' THEN 'Waiting for a TX log writes to flush to disk'
                WHEN wait_type = 'WAITFOR' THEN 'Result of a WAITFOR statement'
                WHEN wait_type = 'LCK_M_IX' THEN 'Waiting to acquire an Intent Exclusive lock on a resource'
                WHEN wait_type = 'WAIT_ON_SYNC_STATISTICS_REFRESH' THEN 'Waiting for synchronous statistics update to complete before query compilation and execution can resume.'
                WHEN wait_type = 'HTREPARTITION' THEN 'Occurs with parallel batch-mode plans when synchronizing the repartitioning of the hash table on the input side of a hash join/aggregation.'
                WHEN wait_type = 'PREEMPTIVE_OS_PIPEOPS' THEN 'Waiting on OS / Windows - e.g. xp_cmdshell'
                WHEN wait_type = 'PREEMPTIVE_OS_QUERYREGISTRY' THEN 'Waiting on OS / Windows registry - e.g. xp_regread, sys.dm_server_registry'
                WHEN wait_type IN ('CXCONSUMER','CXPACKET','CXSYNC_PORT') THEN 'Query parallelism - Not necessarily bad, unless excessive'
                ELSE NULL
            END
        INTO #dm_exec_session_wait_stats
        FROM sys.dm_exec_session_wait_stats WHERE [session_id] = @session_id;
        IF EXISTS (SELECT * FROM #dm_exec_session_wait_stats) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_session_wait_stats')+N'█', * FROM #dm_exec_session_wait_stats ORDER BY wait_time_ms DESC; END;
        RAISERROR('.',0,1) WITH NOWAIT;

        --IF (1=0)
        BEGIN;
            SELECT * INTO #dm_tran_locks FROM sys.dm_tran_locks l WHERE l.request_session_id = @session_id;

            IF EXISTS (SELECT * FROM #dm_tran_locks)
            BEGIN;
                SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_locks')+N'█'
                    , l.resource_type, l.resource_subtype
                    , resource_database = COALESCE(DB_NAME(l.resource_database_id), CONCAT('UNKNOWN:{',l.resource_database_id,'}'))
                    , x.resource_description, l.resource_associated_entity_id, l.request_mode, l.request_type, l.request_status
                    , request_reference_count = SUM(l.request_reference_count)
                    , l.request_owner_type, l.request_owner_id
                    , N'█' [█]
                    , COUNT(*)
                    , x.resource_name
                    , [object_type] = CASE
                                        WHEN l.resource_type = 'OBJECT' THEN OBJECTPROPERTYEX(l.resource_associated_entity_id, 'BaseType')
                                        WHEN l.resource_type IN ('KEY','PAGE','HOBT') THEN COALESCE(OBJECTPROPERTYEX(p.[object_id], 'BaseType'), ot.[type])
                                        ELSE NULL
                                    END
                    , [index_type]  = COALESCE(i.[type_desc], ti.[type_desc])
                    , N'█' [█], p.[object_id], i.[type_desc]
                    , N'█' [█], tp.[object_id], ti.[type_desc]
                FROM #dm_tran_locks l
                    LEFT JOIN (sys.partitions p
                        JOIN sys.indexes i ON i.[object_id] = p.[object_id] AND i.index_id = p.index_id
                    ) ON p.hobt_id = l.resource_associated_entity_id AND l.resource_type IN ('KEY','PAGE','HOBT') AND l.resource_database_id = DB_ID()
                    LEFT JOIN (tempdb.sys.partitions tp
                        JOIN tempdb.sys.objects ot ON ot.[object_id] = tp.[object_id]
                        JOIN tempdb.sys.indexes ti ON ti.[object_id] = tp.[object_id] AND ti.index_id = tp.index_id
                    ) ON tp.hobt_id = l.resource_associated_entity_id AND l.resource_type IN ('KEY','PAGE','HOBT') AND l.resource_database_id = 2
                    CROSS APPLY (
                        SELECT resource_description = CASE WHEN l.resource_type IN ('KEY','PAGE') THEN '{{Multiple}}' ELSE l.resource_description END
                            , resource_name = NULLIF(CONCAT_WS('.'
                                                , QUOTENAME(CASE WHEN l.resource_type = 'OBJECT' THEN OBJECT_SCHEMA_NAME(l.resource_associated_entity_id, l.resource_database_id) WHEN l.resource_type IN ('KEY','PAGE','HOBT') THEN OBJECT_SCHEMA_NAME(COALESCE(p.[object_id], tp.[object_id]), l.resource_database_id) ELSE NULL END)
                                                , QUOTENAME(CASE WHEN l.resource_type = 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id)        WHEN l.resource_type IN ('KEY','PAGE','HOBT') THEN OBJECT_NAME(COALESCE(p.[object_id], tp.[object_id]), l.resource_database_id)        ELSE NULL END)
                                                , QUOTENAME(COALESCE(i.[name], ti.[name]))
                                            ),'')
                    ) x
                GROUP BY l.resource_type, l.resource_subtype, l.resource_database_id, x.resource_description, l.resource_associated_entity_id, l.request_mode, l.request_type, l.request_status, l.request_owner_type, l.request_owner_id
                    , p.[object_id], i.[type_desc]
                    , tp.[object_id], ot.[type], ti.[type_desc]
                    , x.resource_name
                ORDER BY CASE l.request_mode
                            WHEN 'X' THEN 1
                            WHEN 'RangeX-X' THEN 2
                            WHEN 'IX' THEN 3
                            WHEN 'S' THEN 4
                            WHEN 'IS' THEN 5
                            ELSE 9999 END
                        , CASE l.resource_type
                            WHEN 'DATABASE' THEN 1
                            WHEN 'OBJECT' THEN 2
                            WHEN 'ALLOCATION_UNIT' THEN 3
                            WHEN 'EXTENT' THEN 4
                            WHEN 'PAGE' THEN 5
                            WHEN 'KEY' THEN 6
                            ELSE 9999
                        END
                        , x.resource_name;
                RAISERROR('.',0,1) WITH NOWAIT;
            END;
        END;
    END;
    ------------------------------------------------------------
    
    ------------------------------------------------------------
              SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
    UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Latest plans'   , [ ] = N'██████████████████████████████████████████████████'
    UNION ALL SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

    --IF (1=0)
    BEGIN;
        DECLARE @last_live_query_plan xml;
        WHILE (1=1)
        BEGIN;
            SELECT @last_live_query_plan = query_plan
            FROM sys.dm_exec_query_statistics_xml(@session_id)
            WHERE query_plan IS NOT NULL;
        
            IF (@@ROWCOUNT = 0) BREAK;
            RAISERROR('lqp',0,1) WITH NOWAIT;
            WAITFOR DELAY '00:00:00.300';
        END;
        SELECT last_live_query_plan = @last_live_query_plan;

        WHILE EXISTS(SELECT * FROM sys.dm_exec_requests WHERE [session_id] = @session_id AND plan_handle = @plan_handle)
        BEGIN;
            WAITFOR DELAY '00:00:01.000';
            RAISERROR('...',0,1) WITH NOWAIT;
        END;
    END;
END;

SELECT 'sys.dm_exec_query_plan_stats', * FROM sys.dm_exec_query_plan_stats(@plan_handle);

