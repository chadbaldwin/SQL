/* General Notes:
	This script is intended to capture an individual session while matching various criteria configured
	in the main capture query. Once a running session that matches the criteria is caught, the loop breaks
	and as much information about that session as possible is captured before it exits.

	All of the information is then displayed on a per DMV/DMF basis. The intent here is to help
	anyone using this script to not become dependent on it, while also helping to teach what each of the
	DMV/DMF objects do.

	Only runs on 2019+ due to some new system DMVs and added columns to existing DMVs

	The script is broken into 3 parts:
		* Volatile data capture		= This captures in-flight data that is likely to disappear as soon as the request completes.
									  Things like wait stats, blocks, resources, memory grants, etc

		* Non-volatile data capture	= This captures static/accumulative stats type data that sticks around longer term.
									  Things like query store, object level stats, cached plans, etc.
	
		* Output					= Once all of the data is captured into temp tables, they are then displayed in an organized fashion.
									  The output section can be re-run to bring the view back. Or the individual temp tables can be queried.

	Definitions:
		* plan_handle				= A token that uniquely identifies a query execution plan for a batch that is currently executing.
									  Plan handle is a hash value derived from the compiled plan of the entire batch.

		* sql_handle				= A token that uniquely identifies the batch or stored procedure that the query is part of.
									  For ad hoc queries, the SQL handles are hash values based on the SQL text being submitted to the server, and can originate from any database.
									  For database objects such as stored procedures, triggers or functions, the SQL handles are derived from the database ID, object ID, and object number.

		* statement_sql_handle		= sql_handle of the individual query. This column is NULL if Query Store isn't enabled for the database.
*/
------------------------------------------------------------

------------------------------------------------------------
/* WORK IN PROGRESS
	-- TODO:
	Add section at end to dissect captured execution plan - preferably the statement plan
	rather than the batch plan.

	Things to include:
	* Missing indexes
	* Compiled parameters
	* All RelOp elements - each having estimated vs actual row counts
	* Warnings
	* Optimization info - Level, abort reason, no parallel reason
	* Memory grant info
*/
------------------------------------------------------------

------------------------------------------------------------
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
-- Helper proc
------------------------------------------------------------
CREATE OR ALTER PROC #log (
	@msg nvarchar(2047),
	@ts datetime2 = NULL,
	@rc bigint = NULL
)
AS
BEGIN;
	DECLARE @duration_ms bigint;
	SELECT @duration_ms = DATEDIFF(MILLISECOND, @ts, SYSUTCDATETIME());

	DECLARE @template nvarchar(2047);
	SELECT @template = CONCAT_WS(' ', @msg, '['+NULLIF(CONCAT_WS('; ', FORMAT(@duration_ms,'N0')+' ms', FORMAT(@rc,'N0')+' rows'),'')+']')

	RAISERROR(@template,0,1) WITH NOWAIT;
END;
------------------------------------------------------------
GO
------------------------------------------------------------
DROP TABLE IF EXISTS  #variables
					, #dm_exec_connections
					, #dm_exec_sessions
					, #dm_exec_requests
					, #dm_exec_requests_blockedby
					, #dm_exec_requests_blocking
					, #dm_exec_input_buffer
					, #dm_exec_plan_and_text
					, #dm_exec_cached_plans
					, #dm_exec_query_profiles
					, #dm_exec_plan_attributes
					, #qs_variables
					, #query_store_query
					, #query_store_plan
					, #query_store_runtime_stats
					, #query_store_wait_stats
					, #query_store_plan_feedback
					, #query_store_query_variant
					, #dm_db_missing_index_group_stats_query
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
					, #dm_tran_locks
					, #dm_exec_query_plan_stats
					, #last_query_plan;
------------------------------------------------------------
GO
------------------------------------------------------------
-- Settings variables
------------------------------------------------------------
DECLARE @capture_final_plans bit = 0; -- Whether to capture the final plans - this will cause the script to sit and wait until the requests finishes
------------------------------------------------------------

------------------------------------------------------------
DECLARE -- Data capture variables
		@session_id				int,
		@request_id				int,
		@plan_handle			varbinary(64),
		@sql_handle				varbinary(64),
		@stmt_sql_handle		varbinary(64),
		@stmt_start				int,
		@stmt_end				int,
		@batch_text				nvarchar(MAX),
		@stmt_text				nvarchar(MAX),
		@ib_text				nvarchar(MAX),
		@stmt_plan				xml,
		@batch_plan				xml,
		@txid					bigint,
		@page_resource			varbinary(8),
		@query_hash				binary(8),
		@plan_hash				binary(8),
		@db_id					int,
		@object_id				int,
		@start_time				datetime,
		@blocking_session_id	int,
		@command				nvarchar(32),
		-- Script operational variables
		@ts						datetime2,
		@duration_ms			int,
		@max_timeout_ms			bigint			= 6 * 3600000, -- N * 3600000 = N hours
		@timeout_time			datetime2,
		@rc						bigint;
------------------------------------------------------------

------------------------------------------------------------
SELECT @timeout_time = DATEADD(MILLISECOND, @max_timeout_ms, SYSUTCDATETIME());

WHILE (@session_id IS NULL)
BEGIN;
	SELECT @ts = SYSUTCDATETIME();

	WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
	SELECT TOP(1) 
		  @session_id			= c.[session_id]
		, @request_id			= r.request_id
		, @plan_handle			= NULLIF(r.plan_handle,0x0)
		, @sql_handle			= NULLIF(r.[sql_handle],0x0)
		, @stmt_sql_handle		= NULLIF(r.statement_sql_handle,0x0)
		, @stmt_start			= r.statement_start_offset
		, @stmt_end				= r.statement_end_offset
		, @batch_text			= x.batch_text
		, @stmt_text			= x.stmt_text
		, @ib_text				= ib.event_info
		, @stmt_plan			= x.stmt_plan
		, @batch_plan			= x.batch_plan
		, @txid					= r.transaction_id
		, @page_resource		= r.page_resource
		, @query_hash			= r.query_hash
		, @plan_hash			= r.query_plan_hash
		, @db_id				= COALESCE(tqp_s.[dbid], tqp_b.[dbid], DB_ID(PARSENAME(x.object_name_from_input_buffer,3)))
		, @object_id			= COALESCE(st_sh.objectid, OBJECT_ID(x.object_name_from_input_buffer))
		, @start_time			= r.start_time
		, @blocking_session_id	= NULLIF(r.blocking_session_id, 0)
		, @command				= r.command
	FROM sys.dm_exec_connections c
		JOIN sys.dm_exec_sessions s ON s.[session_id] = c.[session_id]
		JOIN sys.dm_exec_requests r ON r.[session_id] = c.[session_id] -- TODO: should this be left or innner join? I think inner. Especially becuase of the WHERE clause predicates
		OUTER APPLY sys.dm_exec_sql_text(r.[sql_handle]) st_sh
		OUTER APPLY sys.dm_exec_input_buffer(r.[session_id], r.request_id) ib
		OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle, 0, -1) tqp_b
		OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle, r.statement_start_offset, r.statement_end_offset) tqp_s
		CROSS APPLY (
			-- TODO: Need to verify that batch and stmt plans are actually what I say they are
			SELECT batch_plan = TRY_CONVERT(xml, tqp_b.query_plan) -- Using TRY_CONVERT because in some cases, the plan has too many nested levels than SQL Server supports (128 levels)
				, stmt_plan = TRY_CONVERT(xml, tqp_s.query_plan) -- Using TRY_CONVERT because in some cases, the plan has too many nested levels than SQL Server supports (128 levels)
				, batch_text = st_sh.[text] -- sql_handle text is nearly always available, but when it's not, neither is the plan_handle text.
				, stmt_text = SUBSTRING(st_sh.[text], r.statement_start_offset/2+1, IIF(r.statement_end_offset = -1, DATALENGTH(st_sh.[text]), (r.statement_end_offset-r.statement_start_offset)/2+1))
				, object_name_from_input_buffer = IIF(ib.event_type = 'RPC Event', LEFT(ib.event_info, CHARINDEX(';', ib.event_info)-1), NULL)
		) x
	WHERE 1=1
		AND c.[session_id] <> @@SPID -- Exclude self
		AND s.[status] NOT IN ('sleeping') -- Valid values: running, sleeping, dormant, preconnect
		AND r.[status] NOT IN ('background','sleeping') -- Valid values: background, rollback, running, runnable, sleeping, suspended
		AND s.login_name NOT IN ('NT AUTHORITY\SYSTEM','sa') -- Exclude any system run requests - usually these are things I don't need to worry about but are often long running so they bubble up to the top of the sort
		-- Most of these are often long runninng but _usually_ non-blocking items so I don't want to worry about them for now
		AND (x.batch_text NOT IN ('xp_cmdshell','xp_backup_log','xp_backup_database') OR x.batch_text IS NULL)
		AND r.[command] NOT IN ('BACKUP LOG','WAITFOR','UPDATE STATISTICS','RESTORE HEADERONLY','DBCC','BACKUP DATABASE','UPDATE STATISTICS')
		AND (r.wait_type NOT IN ('TRACEWRITE','BROKER_RECEIVE_WAITFOR') OR r.wait_type IS NULL)
		----------------------------------------------------------
--		AND r.total_elapsed_time > 3000 -- Minimum runtime - Who cares about stuff running for <3 seconds? I mean, you should, but not for this script
--		AND r.blocking_session_id <> 0 -- is blocked
--		AND r.blocking_session_id = 0 -- not blocked
--		AND EXISTS (SELECT * FROM sys.dm_exec_requests br WHERE br.blocking_session_id = r.[session_id]) -- is blocking
	ORDER BY r.total_elapsed_time DESC;

	EXEC #log N'Check query run', @ts;

	IF (@session_id IS NULL)
	BEGIN;
		WAITFOR DELAY '00:00:03.000';
	END;

	IF (SYSUTCDATETIME() > @timeout_time)
	BEGIN;
		THROW 51000, 'Capture query loop has timed out.', 1;
	END;
END;
------------------------------------------------------------

------------------------------------------------------------
BEGIN;
	EXEC #log N'███████████████████████████████ - Input buffer';
	EXEC #log @ib_text;
	EXEC #log N'███████████████████████████████ - Statement text';
	EXEC #log @stmt_text;
	EXEC #log N'███████████████████████████████ - Batch text';
	EXEC #log @batch_text;
	EXEC #log N'███████████████████████████████';
END;
------------------------------------------------------------

------------------------------------------------------------
BEGIN;
	SELECT run_time			= CONCAT(FORMAT(DATEDIFF(DAY, @start_time, GETDATE()),'0#'), ' ', FORMAT(DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @start_time, GETDATE()), 0),'HH:mm:ss.fff'))
		, [session_id]		= @session_id
		, request_id		= @request_id
		, transaction_id	= @txid
		, stmt_start		= @stmt_start
		, stmt_end			= @stmt_end
		, page_resource		= @page_resource
		, plan_handle		= @plan_handle
		, [sql_handle]		= @sql_handle
		, stmt_sql_handle	= @stmt_sql_handle
		, query_hash		= @query_hash
		, plan_hash			= @plan_hash
		, input_buffer		= @ib_text
		, batch_text		= @batch_text
		, statement_text	= @stmt_text
		, [db_id]			= @db_id
		, [object_id]		= @object_id
		, [command]			= @command
		-- Technically these aren't variables, BUT if it is a temporary object, we want to capture the names right away.
		, [database_name]	= DB_NAME(@db_id)
		, [schema_name]		= OBJECT_SCHEMA_NAME(@object_id, @db_id)
		, [object_name]		= OBJECT_NAME(@object_id, @db_id)
	INTO #variables;
END;
------------------------------------------------------------

------------------------------------------------------------
DECLARE @main_dt datetime2 = SYSUTCDATETIME();
EXEC #log N'Starting volatile data captures'
------------------------------------------------------------

------------------------------------------------------------
-- Top level stats - Connection/Session/Request
------------------------------------------------------------
BEGIN;
	EXEC #log N'-- Top level stats --';

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_connections FROM sys.dm_exec_connections WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_connections', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_sessions FROM sys.dm_exec_sessions WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_sessions', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_requests FROM sys.dm_exec_requests WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_requests', @ts, @@ROWCOUNT;

	IF (@blocking_session_id IS NOT NULL) --AND 1=0
	BEGIN;
		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #dm_exec_requests_blockedby FROM sys.dm_exec_requests WHERE [session_id] = @blocking_session_id;
		EXEC #log N'#dm_exec_requests_blockedby', @ts, @@ROWCOUNT;
	END;

	IF EXISTS (SELECT * FROM sys.dm_exec_requests WHERE blocking_session_id = @session_id) --AND 1=0
	BEGIN;
		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #dm_exec_requests_blocking FROM sys.dm_exec_requests WHERE blocking_session_id = @session_id;
		EXEC #log N'#dm_exec_requests_blocking', @ts, @@ROWCOUNT;
	END;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Query plan and text
------------------------------------------------------------
BEGIN;
	EXEC #log N'-- Query plan and text --';

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_input_buffer FROM sys.dm_exec_input_buffer(@session_id, @request_id);
	EXEC #log N'#dm_exec_input_buffer', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT *
	INTO #dm_exec_plan_and_text
	FROM (
					SELECT dmv = 'sys.dm_exec_sql_text(@plan_handle)'                  , [dbid], objectid, number, [encrypted], query_plan_xml = NULL      , query_plan_text = NULL      , sql_text = [text] FROM sys.dm_exec_sql_text(@plan_handle) -- Plan and SQL handles only - Does not support QS statement SQL handles
		UNION ALL	SELECT dmv = 'sys.dm_exec_query_plan(@plan_handle)'                , [dbid], objectid, number, [encrypted], query_plan_xml = query_plan, query_plan_text = NULL      , sql_text = NULL   FROM sys.dm_exec_query_plan(@plan_handle) -- Plan handles only
		UNION ALL	SELECT dmv = 'sys.dm_exec_query_plan_stats(@plan_handle)'          , [dbid], objectid, number, [encrypted], query_plan_xml = query_plan, query_plan_text = NULL      , sql_text = NULL   FROM sys.dm_exec_query_plan_stats(@plan_handle) -- Plan handles only
		UNION ALL	SELECT dmv = 'sys.dm_exec_text_query_plan(@plan_handle)'           , [dbid], objectid, number, [encrypted], query_plan_xml = NULL      , query_plan_text = query_plan, sql_text = NULL   FROM sys.dm_exec_text_query_plan(@plan_handle, @stmt_start, @stmt_end) -- Plan handles only
		UNION ALL	SELECT dmv = 'sys.dm_exec_text_query_plan(@plan_handle, 0, -1)'    , [dbid], objectid, number, [encrypted], query_plan_xml = NULL      , query_plan_text = query_plan, sql_text = NULL   FROM sys.dm_exec_text_query_plan(@plan_handle, 0, -1) -- Plan handles only
		UNION ALL	SELECT dmv = 'sys.dm_exec_sql_text(@sql_handle)'                   , [dbid], objectid, number, [encrypted], query_plan_xml = NULL      , query_plan_text = NULL      , sql_text = [text] FROM sys.dm_exec_sql_text(@sql_handle) -- Plan and SQL handles only - Does not support QS statement SQL handles
	) x;
	EXEC #log N'#dm_exec_plan_and_text', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_cached_plans FROM sys.dm_exec_cached_plans WHERE plan_handle = @plan_handle;
	EXEC #log N'#dm_exec_cached_plans', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_query_profiles FROM sys.dm_exec_query_profiles WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_query_profiles', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_plan_attributes FROM sys.dm_exec_plan_attributes(@plan_handle);
	EXEC #log N'#dm_exec_plan_attributes', @ts, @@ROWCOUNT;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Misc
------------------------------------------------------------
BEGIN;
	EXEC #log N'-- Misc --';

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_db_session_space_usage FROM sys.dm_db_session_space_usage WHERE [session_id] = @session_id;
	EXEC #log N'#dm_db_session_space_usage', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_db_task_space_usage FROM sys.dm_db_task_space_usage WHERE [session_id] = @session_id;
	EXEC #log N'#dm_db_task_space_usage', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_cdc_log_scan_sessions FROM sys.dm_cdc_log_scan_sessions WHERE [session_id] = @session_id;
	EXEC #log N'#dm_cdc_log_scan_sessions', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_query_memory_grants FROM sys.dm_exec_query_memory_grants WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_query_memory_grants', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_os_tasks FROM sys.dm_os_tasks WHERE [session_id] = @session_id;
	EXEC #log N'#dm_os_tasks', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_os_waiting_tasks FROM sys.dm_os_waiting_tasks WHERE [session_id] = @session_id;
	EXEC #log N'#dm_os_waiting_tasks', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_cursors FROM sys.dm_exec_cursors(@session_id);
	EXEC #log N'#dm_exec_cursors', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_xml_handles FROM sys.dm_exec_xml_handles(@session_id);
	EXEC #log N'#dm_exec_xml_handles', @ts, @@ROWCOUNT;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Transaction info
------------------------------------------------------------
BEGIN;
	EXEC #log N'-- Transaction info --';

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_tran_session_transactions FROM sys.dm_tran_session_transactions WHERE [session_id] = @session_id;
	EXEC #log N'#dm_tran_session_transactions', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_tran_active_transactions FROM sys.dm_tran_active_transactions WHERE transaction_id = @txid;
	EXEC #log N'#dm_tran_active_transactions', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_tran_database_transactions FROM sys.dm_tran_database_transactions WHERE transaction_id = @txid;
	EXEC #log N'#dm_tran_database_transactions', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_tran_active_snapshot_database_transactions FROM sys.dm_tran_active_snapshot_database_transactions WHERE [session_id] = @session_id;
	EXEC #log N'#dm_tran_active_snapshot_database_transactions', @ts, @@ROWCOUNT;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Locks and wait resources
------------------------------------------------------------
BEGIN;
	EXEC #log N'-- Locks and wait resources --';

	IF (@page_resource IS NOT NULL)
	BEGIN;
		SELECT @ts = SYSUTCDATETIME();
		-- TODO: Page info can be grabbed later, it's mostly non-volatile if we just want to grab things like page type and not the exact data on the page
		SELECT pgi.*
		INTO #dm_db_page_info
		FROM sys.fn_PageResCracker(@page_resource) prc
			CROSS APPLY sys.dm_db_page_info(prc.[db_id], prc.[file_id], prc.page_id, 'DETAILED') pgi;
		EXEC #log N'#dm_db_page_info', @ts, @@ROWCOUNT;
	END;

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_session_wait_stats FROM sys.dm_exec_session_wait_stats WHERE [session_id] = @session_id;
	EXEC #log N'#dm_exec_session_wait_stats', @ts, @@ROWCOUNT;

	IF (1=0) -- Excluding for now, it's always slow
	BEGIN;
		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #dm_tran_locks FROM sys.dm_tran_locks l WHERE l.request_session_id = @session_id;
		EXEC #log N'#dm_tran_locks', @ts, @@ROWCOUNT;
	END;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
EXEC #log N'Volatile data captures done', @main_dt;
EXEC #log N'';


SELECT @main_dt = SYSUTCDATETIME();
EXEC #log N'Starting non-volatile data captures';
------------------------------------------------------------

------------------------------------------------------------
-- Query Store
------------------------------------------------------------
-- Should run at the end since it's not dependent on the transaction in progress
-- TODO: Consider adding a check to see if columnstore is enabled? If not, no reason to do this work.
BEGIN;
	EXEC #log N'-- Query Store --';

	DECLARE @qs_query_id bigint, @qs_plan_id bigint;
	SELECT @qs_query_id = query_id FROM sys.query_store_query WHERE batch_sql_handle = @sql_handle AND query_hash = @query_hash;
	SELECT @qs_plan_id = plan_id FROM sys.query_store_plan WHERE query_id = @qs_query_id AND query_plan_hash = @plan_hash;

	-- Saving off into a temp table so that the values stick around if we need to re-run the output section
	SELECT query_id = @qs_query_id, plan_id = @qs_plan_id INTO #qs_variables;

	IF (@qs_query_id IS NOT NULL AND @qs_plan_id IS NOT NULL)
	BEGIN;
		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_query FROM sys.query_store_query WHERE query_id = @qs_query_id OR [object_id] = @object_id OR batch_sql_handle IN (@sql_handle, @stmt_sql_handle) OR last_compile_batch_sql_handle IN (@sql_handle, @stmt_sql_handle) OR query_hash = @query_hash;
		EXEC #log N'#query_store_query', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_plan FROM sys.query_store_plan WHERE plan_id = @qs_plan_id;
		EXEC #log N'#query_store_plan', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_runtime_stats FROM sys.query_store_runtime_stats WHERE plan_id = @qs_plan_id;
		EXEC #log N'#query_store_runtime_stats', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_wait_stats FROM sys.query_store_wait_stats WHERE plan_id = @qs_plan_id;
		EXEC #log N'#query_store_wait_stats', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_plan_feedback FROM sys.query_store_plan_feedback WHERE plan_id = @qs_plan_id;
		EXEC #log N'#query_store_plan_feedback', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #query_store_query_variant FROM sys.query_store_query_variant WHERE query_variant_query_id = @qs_query_id OR parent_query_id = @qs_query_id OR dispatcher_plan_id = @qs_plan_id;
		EXEC #log N'#query_store_query_variant', @ts, @@ROWCOUNT;

		SELECT @ts = SYSUTCDATETIME();
		SELECT * INTO #dm_db_missing_index_group_stats_query FROM sys.dm_db_missing_index_group_stats_query q WHERE last_sql_handle IN (@sql_handle, @stmt_sql_handle) OR last_statement_sql_handle IN (@sql_handle, @stmt_sql_handle);
		EXEC #log N'#dm_db_missing_index_group_stats_query', @ts, @@ROWCOUNT;
	END;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Various stats tables
------------------------------------------------------------
-- Should run at the end since it's not dependent on the transaction in progress
BEGIN;
	EXEC #log N'-- Various stats tables --';

	SELECT @ts = SYSUTCDATETIME();
	SELECT * INTO #dm_exec_query_stats FROM sys.dm_exec_query_stats x WHERE x.plan_handle = @plan_handle OR x.[sql_handle] = @sql_handle;
	EXEC #log N'#dm_exec_query_stats', @ts, @@ROWCOUNT;

	SELECT @ts = SYSUTCDATETIME();
	WITH sys_dm_exec_function_stats AS (
		-- Store in CTE to normalize columns
		SELECT database_id, [object_id], [type], [type_desc], [sql_handle], plan_handle, cached_time, last_execution_time, execution_count, total_worker_time, last_worker_time, min_worker_time, max_worker_time, total_physical_reads, last_physical_reads, min_physical_reads, max_physical_reads, total_logical_writes, last_logical_writes, min_logical_writes, max_logical_writes, total_logical_reads, last_logical_reads, min_logical_reads, max_logical_reads, total_elapsed_time, last_elapsed_time, min_elapsed_time, max_elapsed_time
			, total_spills = NULL, last_spills = NULL, min_spills = NULL, max_spills = NULL
			, total_num_physical_reads, last_num_physical_reads, min_num_physical_reads, max_num_physical_reads, total_page_server_reads, last_page_server_reads, min_page_server_reads, max_page_server_reads, total_num_page_server_reads, last_num_page_server_reads, min_num_page_server_reads, max_num_page_server_reads
		FROM sys.dm_exec_function_stats
	)
	SELECT *
	INTO #stats_tables
	FROM (
					SELECT dmv = 'sys.dm_exec_procedure_stats'	, * FROM sys.dm_exec_procedure_stats	WHERE [sql_handle] = @sql_handle OR plan_handle = @plan_handle OR (database_id = @db_id AND [object_id] = @object_id)
		UNION ALL	SELECT dmv = 'sys.dm_exec_trigger_stats'	, * FROM sys.dm_exec_trigger_stats		WHERE [sql_handle] = @sql_handle OR plan_handle = @plan_handle OR (database_id = @db_id AND [object_id] = @object_id)
		UNION ALL	SELECT dmv = 'sys.dm_exec_function_stats'	, * FROM sys_dm_exec_function_stats		WHERE [sql_handle] = @sql_handle OR plan_handle = @plan_handle OR (database_id = @db_id AND [object_id] = @object_id)
	) x;
	EXEC #log N'#stats_tables', @ts, @@ROWCOUNT;

	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
EXEC #log N'Non-volatile data captures done', @main_dt;
EXEC #log N'';
------------------------------------------------------------

------------------------------------------------------------
/*	TODO: Consider some sort of lookup at database scoped configurations and trace flags to ensure things like
	light query profiling and such are enabled? If they are not, then these views will likely be empty.
*/
IF (@capture_final_plans = 1)
BEGIN;
	SELECT @ts = SYSUTCDATETIME();
	EXEC #log N'Starting final plan captures';

	-- TODO: Consider a max timeout?
	/*	IF this is a VERY long running process, or we're in the middle of an emergency
		we probably don't want to sit and wait for the request to complete. Instead we
		want to see in-flight information. So maybe an option to enable this, or maybe
		an optional timeout of some sort? Not sure yet */
	DECLARE @last_live_query_plan xml, @last_seen datetime;
	/*	`sys.dm_exec_query_statistics_xml` gives you an "in-flight" live query plan.

		This will loop (indefinitely until the request completes) every N milliseconds
		capturing the in-flight live query plan until the request is completed. And it
		will hold onto that last one.

		The idea being that we want the very last possible refresh of the in-flight
		plan so that we can see things like actual row stats and such.

		_Technically_ `sys.dm_exec_query_plan_stats` is supposed to give you the
		"last known" actual execution plan, but I've found it's not always reliable.
	*/
	WHILE (@rc > 0)
	BEGIN;
		EXEC #log N'lqp';
		SELECT @last_live_query_plan = query_plan, @last_seen = GETDATE()
		FROM sys.dm_exec_query_statistics_xml(@session_id)
		WHERE query_plan IS NOT NULL;
		SELECT @rc = @@ROWCOUNT;
		WAITFOR DELAY '00:00:00.300';
	END;

	SELECT last_seen = @last_seen
		, estimated_total_runtime = CONCAT(FORMAT(DATEDIFF(DAY, @start_time, @last_seen),'0#'), ' ', FORMAT(DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, @start_time, @last_seen), 0),'HH:mm:ss.fff'))
		, last_live_query_plan = @last_live_query_plan
		, N'█' [█], [description] = 'Last captured live query plan'
	INTO #last_query_plan
	WHERE @last_seen IS NOT NULL AND @last_live_query_plan IS NOT NULL;

	/*	`sys.dm_exec_query_plan_stats` gives you the "last known" actual execution plan.

		Since this section won't run until after the prior section completes, then we
		know the request has completed and the "last known" plan is _likely_ for the
		request we captured. However, this 100% an assumption that needs to be verified.
	*/
	SELECT * INTO #dm_exec_query_plan_stats FROM sys.dm_exec_query_plan_stats(@plan_handle);

	EXEC #log N'Final plan captures done', @ts;
	EXEC #log N'';
END;
------------------------------------------------------------

------------------------------------------------------------
-- Output
------------------------------------------------------------
DECLARE @output_ts datetime2 = SYSUTCDATETIME();
BEGIN;
	DECLARE @xml_replace nvarchar(30) = 0x0000010002000300040005000600070008000B000C000E000F00010010001100120013001400150016001700180019001A001B001C001D001E001F00, -- NCHAR's 0-31 (excluding CR, LF, TAB)
			@crlf nchar(2) = NCHAR(13)+NCHAR(10);

				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'     Variables'     , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	SELECT run_time, [session_id], request_id, transaction_id, stmt_start, stmt_end, page_resource, command FROM #variables;
	SELECT 'Handles and hashes', plan_handle, [sql_handle], stmt_sql_handle, query_hash, plan_hash FROM #variables;
	SELECT 'Text'
		, input_buffer		= (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, v.input_buffer, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE)
		, batch_text		= (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, v.batch_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE)
		, statement_text	= (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, v.statement_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE)
	FROM #variables v;
	SELECT 'Object info', [db_id], [object_id], [database_name], [schema_name], [object_name] FROM #variables;

				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N' Session / Request' , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #dm_exec_connections) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_connections')+N'█', * FROM #dm_exec_connections; END;
	IF EXISTS (SELECT * FROM #dm_exec_sessions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_sessions')+N'█', * FROM #dm_exec_sessions; END;
	IF EXISTS (SELECT * FROM #dm_exec_requests) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests')+N'█', * FROM #dm_exec_requests; END;
	IF OBJECT_ID('tempdb..#dm_exec_requests_blockedby') IS NOT NULL
	BEGIN;
		IF EXISTS (SELECT * FROM #dm_exec_requests_blockedby) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests - blocked by')+N'█', * FROM #dm_exec_requests_blockedby; END;
	END;
	IF OBJECT_ID('tempdb..#dm_exec_requests_blocking') IS NOT NULL
	BEGIN;
		IF EXISTS (SELECT * FROM #dm_exec_requests_blocking) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_requests - blocking')+N'█', * FROM #dm_exec_requests_blocking; END;
	END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'Query plan and text', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #dm_exec_input_buffer) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_input_buffer')+N'█', * FROM #dm_exec_input_buffer; END;
	IF EXISTS (SELECT * FROM #dm_exec_plan_and_text)
	BEGIN;
		WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
		SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
			, x.[dbid], x.objectid, x.[encrypted]
			, sql_text = IIF(x.sql_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, x.sql_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
			, y.query_plan_xml
			, query_plan_text = COALESCE(x.query_plan_text, CONVERT(nvarchar(MAX), x.query_plan_xml))
			, N'█' [█]
			, d.[description]
			, plan_stmt_count = y.query_plan_xml.value('count(/ShowPlanXML/BatchSequence/Batch/Statements/*)', 'int')
			, missing_idx_count = y.query_plan_xml.value('count(/ShowPlanXML/BatchSequence/Batch/Statements/*//MissingIndex)', 'int') -- TODO: Dedup count - Currently counts all missing, even if index suggestion is duplicated.
			, warning_count = y.query_plan_xml.value('count(/ShowPlanXML/BatchSequence/Batch/Statements//QueryPlan//Warnings)', 'int') -- TODO: Dedup count - Currently counts all missing, even if index suggestion is duplicated.
			, statement_text = IIF(st.statement_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, st.statement_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
		FROM #dm_exec_plan_and_text x
			CROSS JOIN #variables v
			CROSS APPLY (
				SELECT [description] = CASE 
											WHEN x.dmv LIKE 'sys.dm_exec_sql_text(%'				THEN 'Returns the text of the SQL batch that is identified by the specified sql_handle.'
											WHEN x.dmv LIKE 'sys.dm_exec_query_plan(%'				THEN 'Returns the Showplan in XML format for the batch specified by the plan handle.'
											WHEN x.dmv LIKE 'sys.dm_exec_query_plan_stats(%'		THEN 'Returns the equivalent of the last known actual execution plan for a previously cached query plan.'
											WHEN x.dmv LIKE 'sys.dm_exec_text_query_plan(%'			THEN 'Returns the Showplan in text format for a Transact-SQL batch or for a specific statement within the batch.'
											WHEN x.dmv LIKE 'sys.dm_exec_query_statistics_xml(%'	THEN 'Returns query execution plan for in-flight requests.'
											ELSE NULL
										END
			) d
			CROSS APPLY (SELECT query_plan_xml = COALESCE(x.query_plan_xml, TRY_CONVERT(xml, x.query_plan_text))) y
			CROSS APPLY (SELECT statement_text = SUBSTRING(x.sql_text, v.stmt_start/2+1, IIF(v.stmt_end = -1, DATALENGTH(x.sql_text), (v.stmt_end-v.stmt_start)/2+1))) st
		ORDER BY x.dmv;
	END;
	IF EXISTS (SELECT * FROM #dm_exec_cached_plans) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_cached_plans')+N'█', * FROM #dm_exec_cached_plans; END;
	IF EXISTS (SELECT * FROM #dm_exec_query_profiles)
	BEGIN;
		SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_profiles')+N'█'
			, ce_accuracy_pct = FORMAT(row_count / NULLIF((estimate_row_count * 1.0), 0), 'P0')
			, row_count = FORMAT(row_count, 'N0')
			, N'█' [█], *
		FROM #dm_exec_query_profiles
		ORDER BY node_id, thread_id;
	END;
	IF EXISTS (SELECT * FROM #dm_exec_plan_attributes) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_plan_attributes')+N'█', * FROM #dm_exec_plan_attributes; END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Query Store'    , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #qs_variables WHERE query_id IS NOT NULL AND plan_id IS NOT NULL)
	BEGIN;
		IF EXISTS (SELECT * FROM #query_store_query)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_query')+N'█'
				, batch_sql_handle_match              = CASE q.batch_sql_handle              WHEN v.[sql_handle] THEN '@sql_handle' WHEN v.stmt_sql_handle THEN '@stmt_sql_handle' ELSE NULL END
				, last_compile_batch_sql_handle_match = CASE q.last_compile_batch_sql_handle WHEN v.[sql_handle] THEN '@sql_handle' WHEN v.stmt_sql_handle THEN '@stmt_sql_handle' ELSE NULL END
				, [off]                               = IIF(q.last_compile_batch_offset_start = v.stmt_start AND q.last_compile_batch_offset_end = v.stmt_end, N'☑️', '')
				, obj                                 = IIF(q.[object_id] = v.[object_id], N'☑️', '')
				, qh                                  = IIF(q.query_hash = v.query_hash, N'☑️', '')
				, hh                                  = IIF(q.batch_sql_handle = q.last_compile_batch_sql_handle OR (q.batch_sql_handle IS NULL AND q.last_compile_batch_sql_handle IS NULL), N'☑️', '')
				, N'█ batch_sql_handle ->' [█]
				, sql_text                            = IIF(x.sql_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, x.sql_text , @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, stmt_text                           = IIF(x.stmt_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, x.stmt_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, N'█ last_compile_batch_sql_handle ->' [█]
				, sql_text                            = IIF(y.sql_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, y.sql_text , @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, stmt_text                           = IIF(y.stmt_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, y.stmt_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, N'█' [█], q.*
			FROM #query_store_query q
				OUTER APPLY sys.dm_exec_sql_text(q.batch_sql_handle) t1
				CROSS APPLY (
					SELECT sql_text = t1.[text]
						, stmt_text = SUBSTRING(t1.[text], q.last_compile_batch_offset_start/2+1, IIF(q.last_compile_batch_offset_end = -1, DATALENGTH(t1.[text]), (q.last_compile_batch_offset_end-q.last_compile_batch_offset_start)/2+1))
				) x
				OUTER APPLY sys.dm_exec_sql_text(IIF(LEFT(q.last_compile_batch_sql_handle,1) = 0x09, NULL, q.last_compile_batch_sql_handle)) t2
				CROSS APPLY (
					SELECT sql_text = t2.[text]
						, stmt_text = SUBSTRING(t2.[text], q.last_compile_batch_offset_start/2+1, IIF(q.last_compile_batch_offset_end = -1, DATALENGTH(t2.[text]), (q.last_compile_batch_offset_end-q.last_compile_batch_offset_start)/2+1))
				) y
				CROSS JOIN #variables v
			ORDER BY q.last_execution_time DESC;
		END;
		IF EXISTS (SELECT * FROM #query_store_plan) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_plan')+N'█', * FROM #query_store_plan; END;
		IF EXISTS (SELECT * FROM #query_store_runtime_stats)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_runtime_stats')+N'█'
				, avg_duration = CONCAT(FORMAT(avg_duration/86400000000.0,'0#'), ' ', FORMAT(DATEADD(MICROSECOND, avg_duration, CONVERT(datetime2,'0001-01-01')),'HH:mm:ss.fff'))
				, avg_memory_grant_mb = avg_query_max_used_memory * 8.0 / 1024.0
				, N'█' [█], *
			FROM #query_store_runtime_stats
			ORDER BY last_execution_time DESC;
		END;
		IF EXISTS (SELECT * FROM #query_store_wait_stats) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_wait_stats')+N'█', * FROM #query_store_wait_stats ORDER BY runtime_stats_interval_id, wait_category; END;
		IF EXISTS (SELECT * FROM #query_store_plan_feedback) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_plan_feedback')+N'█', * FROM #query_store_plan_feedback; END;
		IF EXISTS (SELECT * FROM #query_store_plan_feedback) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.query_store_query_variant')+N'█', * FROM #query_store_query_variant; END;
		IF EXISTS (SELECT * FROM #dm_db_missing_index_group_stats_query)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_missing_index_group_stats_query')+N'█'
				, last_sql_handle			= CASE last_sql_handle           WHEN v.[sql_handle] THEN '@sql_handle' WHEN v.stmt_sql_handle THEN '@stmt_sql_handle' ELSE NULL END
				, last_statement_sql_handle	= CASE last_statement_sql_handle WHEN v.[sql_handle] THEN '@sql_handle' WHEN v.stmt_sql_handle THEN '@stmt_sql_handle' ELSE NULL END
				, [off]						= IIF(q.last_statement_start_offset = v.stmt_start AND q.last_statement_end_offset = v.stmt_end, N'☑️', '')
				, qh						= IIF(q.query_hash = v.query_hash, N'☑️', '')
				, ph						= IIF(q.query_plan_hash = v.plan_hash, N'☑️', '')
				, N'█ last_sql_handle ->' [█]
				, sql_text					= IIF(x.sql_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, x.sql_text , @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, stmt_text					= IIF(x.stmt_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, x.stmt_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, N'█ last_statement_sql_handle ->' [█]
				, stmt_text					= IIF(qt.query_sql_text IS NOT NULL, (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, qt.query_sql_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE), NULL)
				, N'█' [█], q.*
				, N'█' [█], id.*
			FROM #dm_db_missing_index_group_stats_query q
				LEFT JOIN sys.dm_db_missing_index_groups ig ON ig.index_group_handle = q.group_handle
				LEFT JOIN sys.dm_db_missing_index_details id ON id.index_handle = ig.index_handle
				LEFT JOIN sys.query_store_query_text qt ON qt.statement_sql_handle = q.last_statement_sql_handle
				OUTER APPLY sys.dm_exec_sql_text(q.last_sql_handle) t1
				CROSS APPLY (
					SELECT sql_text = t1.[text]
						, stmt_text = SUBSTRING(t1.[text], q.last_statement_start_offset/2+1, IIF(q.last_statement_end_offset = -1, DATALENGTH(t1.[text]), (q.last_statement_end_offset-q.last_statement_start_offset)/2+1))
				) x
				CROSS JOIN #variables v
			ORDER BY q.avg_user_impact DESC;
		END;
	END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Stats views'    , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #dm_exec_query_stats)
	BEGIN;
		SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_stats')+N'█'
			, ph = IIF(x.plan_handle = v.plan_handle, N'☑️', '')
			, sh = IIF(x.[sql_handle] = v.[sql_handle], N'☑️', '')
			, [off] = IIF(x.statement_start_offset = v.stmt_start AND x.statement_end_offset = v.stmt_end, N'☑️', '')
			, N'█' [█]
				, statement_text = (SELECT [processing-instruction(q)] = TRANSLATE(REPLACE(CONCAT(N'--', @crlf, stt.statement_text, @crlf, N'--'),'?>','??') COLLATE Latin1_General_Bin2, @xml_replace, REPLICATE(N'?',LEN(@xml_replace))) FOR XML PATH(''), TYPE)
				, avg_elapsed_time = CONCAT(FORMAT(x.total_elapsed_time/x.execution_count/86400000000.0,'0#'), ' ', FORMAT(DATEADD(MILLISECOND, x.total_elapsed_time/(x.execution_count*1.0), 0),'HH:mm:ss.fff')) -- TODO: Validate accuracy - Finding this to be inaccurate fairly often due to "incorrect" execution counts (too low)
				, avg_grant_mb = CONVERT(decimal(15,3), x.total_grant_kb / x.execution_count / 1024.0)
			, N'█' [█], x.*
		FROM #dm_exec_query_stats x
			CROSS JOIN #variables v
			OUTER APPLY sys.dm_exec_sql_text(v.[sql_handle]) st
			CROSS APPLY (SELECT statement_text = SUBSTRING(st.[text], x.statement_start_offset/2+1, IIF(x.statement_end_offset = -1, DATALENGTH(st.[text]), (x.statement_end_offset-x.statement_start_offset)/2+1))) stt
		ORDER BY x.[sql_handle], x.plan_handle, x.statement_start_offset;
	END;

	IF EXISTS (SELECT * FROM #stats_tables)
	BEGIN;
		SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
			, ph = IIF(x.plan_handle = v.plan_handle, N'☑️', '')
			, sh = IIF(x.[sql_handle] = v.[sql_handle], N'☑️', '')
			, obj = IIF(x.database_id = v.[db_id] AND x.[object_id] = v.[object_id], N'☑️', '')
			, N'█' [█]
			, x.*
		FROM #stats_tables x
			CROSS JOIN #variables v;

		SELECT [dmv                                              █] = CONVERT(nchar(49), x.dmv)+N'█'
			, plans_per_query = COUNT(DISTINCT IIF(x.[sql_handle] = v.[sql_handle], x.plan_handle, NULL))
			, queries_per_plan = COUNT(DISTINCT IIF(x.[plan_handle] = v.plan_handle, x.[sql_handle], NULL))
			, plans_per_object = COUNT(DISTINCT IIF(x.[object_id] = v.[object_id], x.plan_handle, NULL))
		FROM #stats_tables x
			CROSS JOIN #variables v
		GROUP BY dmv;
	END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'        Misc'       , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #dm_db_session_space_usage) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_session_space_usage')+N'█', * FROM #dm_db_session_space_usage; END;
	IF EXISTS (SELECT * FROM #dm_db_task_space_usage) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_task_space_usage')+N'█', * FROM #dm_db_task_space_usage ORDER BY exec_context_id; END;
	IF EXISTS (SELECT * FROM #dm_cdc_log_scan_sessions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_cdc_log_scan_sessions')+N'█', * FROM #dm_cdc_log_scan_sessions; END;
	IF EXISTS (SELECT * FROM #dm_exec_query_memory_grants)
	BEGIN;
		SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_memory_grants')+N'█'
			, requested_memory_gb = FORMAT(requested_memory_kb / 1024.0 / 1024.0, 'N3')
			, granted_memory_gb = FORMAT(granted_memory_kb / 1024.0 / 1024.0, 'N3')
			, used_memory_gb = FORMAT(used_memory_kb / 1024.0 / 1024.0, 'N3')
			, max_used_memory_gb = FORMAT(max_used_memory_kb / 1024.0 / 1024.0, 'N3')
			, N'█' [█], *
		FROM #dm_exec_query_memory_grants;
	END;
	IF EXISTS (SELECT * FROM #dm_os_tasks) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_os_tasks')+N'█', * FROM #dm_os_tasks ORDER BY exec_context_id; END;
	IF EXISTS (SELECT * FROM #dm_os_waiting_tasks) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_os_waiting_tasks')+N'█', * FROM #dm_os_waiting_tasks ORDER BY exec_context_id, blocking_session_id, blocking_exec_context_id; END;
	IF EXISTS (SELECT * FROM #dm_exec_cursors) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_cursors')+N'█', * FROM #dm_exec_cursors; END;
	IF EXISTS (SELECT * FROM #dm_exec_xml_handles) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_xml_handles')+N'█', * FROM #dm_exec_xml_handles; END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N' Transaction info'  , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF EXISTS (SELECT * FROM #dm_tran_session_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_session_transactions')+N'█', * FROM #dm_tran_session_transactions; END;
	IF EXISTS (SELECT * FROM #dm_tran_active_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_active_transactions')+N'█', * FROM #dm_tran_active_transactions; END;
	IF EXISTS (SELECT * FROM #dm_tran_database_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_database_transactions')+N'█', * FROM #dm_tran_database_transactions; END;
	IF EXISTS (SELECT * FROM #dm_tran_active_snapshot_database_transactions) BEGIN; SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_active_snapshot_database_transactions')+N'█', * FROM #dm_tran_active_snapshot_database_transactions; END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'  Locks and waits'  , [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';

	IF (OBJECT_ID('tempdb..#dm_db_page_info') IS NOT NULL)
	BEGIN;
		IF EXISTS (SELECT * FROM #dm_db_page_info)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_db_page_info')+N'█'
				, [database_name] = DB_NAME(database_id)
				, [schema_name] = OBJECT_SCHEMA_NAME([object_id], database_id)
				, [object_name] = OBJECT_NAME([object_id], database_id)
				, index_id
				, N'█' [█], *
			FROM #dm_db_page_info;
		END;
	END;

	IF EXISTS (SELECT * FROM #dm_exec_session_wait_stats)
	BEGIN;
		SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_session_wait_stats')+N'█'
			, *
			, N'█' [█]
			, [description] = CASE -- TODO: Lots of unecessary duplicate work when many existing scripts and tools provide this - Can we pull from those tools/repos? Or keep this going because I prefer different descriptions?
								WHEN wait_type LIKE 'PAGELATCH[_]%' THEN 'Accessing pages in memory'
								WHEN wait_type LIKE 'PAGEIOLATCH[_]%' THEN 'Pulling pages from disk into memory buffers'
								WHEN wait_type = 'WRITELOG' THEN 'Waiting for a TX log writes to flush to disk'
								WHEN wait_type = 'WAITFOR' THEN 'Result of a WAITFOR statement'
								WHEN wait_type = 'LCK_M_IX' THEN 'Waiting to acquire an Intent Exclusive lock on a resource'
								WHEN wait_type = 'WAIT_ON_SYNC_STATISTICS_REFRESH' THEN 'Waiting for synchronous statistics update to complete before query compilation and execution can resume.'
								WHEN wait_type = 'PREEMPTIVE_OS_PIPEOPS' THEN 'Waiting on OS / Windows - e.g. xp_cmdshell'
								WHEN wait_type = 'PREEMPTIVE_OS_QUERYREGISTRY' THEN 'Waiting on OS / Windows registry - e.g. xp_regread, sys.dm_server_registry'
								WHEN wait_type IN ('CXCONSUMER','CXPACKET','CXSYNC_PORT','CXSYNC_CONSUMER','CXROWSET_SYNC') THEN 'Query parallelism - Not necessarily bad, unless excessive'
								WHEN wait_type = 'LATCH_EX' THEN 'Waiting to obtain an exclusive latch on a non-page memory structure'
								WHEN wait_type = 'SLEEP_TASK' THEN 'Task is sleeping while waiting for a generic event to occur.'
								WHEN wait_type = 'LOGMGR_FLUSH' THEN 'Waiting for the current log flush to complete'
								-- Batch-Mode
								WHEN wait_type = 'BPSORT' THEN 'Batch-Mode - Thread is involved in a batch-mode sort'
								WHEN wait_type = 'HTBUILD' THEN 'Batch-Mode - Synchronizing the building of the hash table on the input side of a hash join/aggregation'
								WHEN wait_type = 'HTREPARTITION' THEN 'Batch-Mode - Synchronizing the repartitioning of the hash table on the input side of a hash join/aggregation'
								WHEN wait_type = 'HTDELETE' THEN 'Batch-Mode - Synchronizing at the end of a hash join/aggregation.'
								WHEN wait_type = 'HTMEMO' THEN 'Batch-Mode - Synchronizing before scanning hash table to output matches / non-matches in hash join/aggregation'
								WHEN wait_type = 'HTREINIT' THEN 'Batch-Mode - Synchronizing before resetting a hash join/aggregation for the next partial join'
								WHEN wait_type = 'BMPALLOCATION' THEN 'Batch-Mode - Synchronizing the allocation of a large bitmap filter'
								WHEN wait_type = 'BMPBUILD' THEN 'Batch-Mode - Synchronizing the building of a large bitmap filter'
								WHEN wait_type = 'BMPREPARTITION' THEN 'Batch-Mode - Synchronizing the repartitioning of a large bitmap filter'
								--
								WHEN wait_type = 'PWAIT_QRY_BPMEMORY' THEN 'Internal use only'
								ELSE NULL
							END
		FROM #dm_exec_session_wait_stats
		ORDER BY wait_time_ms DESC;
	END;

	IF (OBJECT_ID('tempdb..#dm_tran_locks') IS NOT NULL)
	BEGIN;
		IF EXISTS (SELECT * FROM #dm_tran_locks)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_tran_locks')+N'█'
				, l.resource_type, l.resource_subtype
				, resource_database = COALESCE(DB_NAME(l.resource_database_id), CONCAT('UNKNOWN:{',l.resource_database_id,'}'))
				, l.resource_description, l.resource_associated_entity_id, l.request_mode, l.request_type, l.request_status
				, l.request_reference_count
				, l.request_owner_type, l.request_owner_id, l.lock_owner_address
				, N'█' [█]
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
					SELECT resource_name = NULLIF(CONCAT_WS('.'
											, QUOTENAME(CASE WHEN l.resource_type = 'OBJECT' THEN OBJECT_SCHEMA_NAME(l.resource_associated_entity_id, l.resource_database_id) WHEN l.resource_type IN ('KEY','PAGE','HOBT') THEN OBJECT_SCHEMA_NAME(COALESCE(p.[object_id], tp.[object_id]), l.resource_database_id) ELSE NULL END)
											, QUOTENAME(CASE WHEN l.resource_type = 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id)        WHEN l.resource_type IN ('KEY','PAGE','HOBT') THEN OBJECT_NAME(COALESCE(p.[object_id], tp.[object_id]), l.resource_database_id)        ELSE NULL END)
											, QUOTENAME(COALESCE(i.[name], ti.[name]))
										),'')
				) x
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
					, x.resource_name
					, l.lock_owner_address;
		END;
	END;
	------------------------------------------------------------
	
	------------------------------------------------------------
				SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'    Latest plans   ', [ ] = N'██████████████████████████████████████████████████'
	UNION ALL	SELECT [ ] = N'██████████████████████████████████████████████████', [ ] = N'███████████████████', [ ] = N'██████████████████████████████████████████████████';
	IF (OBJECT_ID('tempdb..#dm_exec_query_plan_stats') IS NOT NULL)
	BEGIN;
		IF EXISTS (SELECT * FROM #dm_exec_query_plan_stats)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_plan_stats')+N'█', *
				, N'█' [█], [description] = 'Last known actual execution plan'
			FROM #dm_exec_query_plan_stats;
		END;
	END;

	IF (OBJECT_ID('tempdb..#last_query_plan') IS NOT NULL)
	BEGIN;
		IF EXISTS (SELECT * FROM #last_query_plan)
		BEGIN;
			SELECT [dmv                                              █] = CONVERT(nchar(49), N'sys.dm_exec_query_statistics_xml')+N'█', *
			FROM #last_query_plan;
		END;
	END;
END;
EXEC #log N'Output done', @output_ts;
------------------------------------------------------------
GO
------------------------------------------------------------
