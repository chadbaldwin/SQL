/*

This script takes any query as input, and returns a temp table for the first result set returned by the script.

It is hardcoded based on a tab-size of 4, and is written to format the table in my own personal formatting style.

It could easily be re-written to support other size tabs, however, the effort required to make it adjustable with
parameters is beyond the effort I'm willing to put into this.

This is also written in a form that supports earlier versions where STRING_AGG() is not yet available.

*/

-- Query to obtain schema from
DECLARE @sql nvarchar(MAX) = N'
	/*
	Enter your query here to generate output
	*/
';

DECLARE @output nvarchar(MAX);

-- Generate, format and join column declarations
SELECT @output = (
	SELECT CONCAT(CHAR(9)																-- Indent each column by 1 tab
			, x.ColName																	-- Column Name
			, REPLICATE(CHAR(9), CEILING((x.MaxColNameLen - LEN(x.ColName)) / 4.0))		-- Add tabs after column name so that data type lines up
			, x.ColType																	-- Data type of column
			, REPLICATE(CHAR(9), CEILING((x.MaxTypeNameLen - LEN(x.ColType)) / 4.0))	-- Add tabs after data type so that NULL/NOT NULL lines up
			, IIF(x.is_nullable = 1, CHAR(9) + 'NULL', 'NOT NULL')						-- Add an extra tab to align 'NULL'
			, ',{{br}}')
	FROM (
		SELECT y.ColName, ColType = dt.TypeName, ColOrder = x.column_ordinal, x.is_nullable, x.is_identity_column
			, MaxColNameLen = MAX(LEN(y.ColName)) OVER () + (4-(MAX(LEN(y.ColName)) OVER () % 4))
			, MaxTypeNameLen = MAX(LEN(dt.TypeName)) OVER () + (4-(MAX(LEN(dt.TypeName)) OVER () % 4))
		FROM sys.dm_exec_describe_first_result_set(@sql, NULL, 1) x
			CROSS APPLY (
				SELECT TypeName = CONCAT(x.system_type_name
						,	CASE
								WHEN x.system_type_name IN ('datetime2', 'time')	THEN IIF(x.scale = 7, NULL, CONCAT('(', x.scale, ')')) --scale of (7) is the default so it can be ignored, (0) is a valid value
								WHEN x.system_type_name IN ('datetimeoffset')		THEN CONCAT('(', x.scale, ')')
								WHEN x.system_type_name IN ('decimal', 'numeric')	THEN CONCAT('(', x.[precision], ',', x.scale,')')
								WHEN x.system_type_name IN ('nchar', 'nvarchar')	THEN IIF(x.max_length = -1, '(MAX)', CONCAT('(', x.max_length/2, ')'))
								WHEN x.system_type_name IN ('char', 'varchar')		THEN IIF(x.max_length = -1, '(MAX)', CONCAT('(', x.max_length, ')'))
								WHEN x.system_type_name IN ('binary', 'varbinary')	THEN IIF(x.max_length = -1, '(MAX)', CONCAT('(', x.max_length, ')'))
								ELSE NULL
							END)
			) dt
			CROSS APPLY (
				SELECT ColName = IIF(x.[name] IN ( -- Add sqare brackets only to reserved keywords or names containing unescaped characters
						'action','address','affinity','algorithm','allow_encrypted_value_modifications','allow_page_locks','allow_row_locks','ansi_defaults','ansi_null_dflt_on','ansi_nulls','ansi_padding','ansi_warnings','app_name','application','arithabort','automated_backup_preference','availability_mode','backup_priority','broker_instance','bucket_count','catalog','clear_port','cluster_type','columns','compatibility_level','compression_delay','concat_null_yields_null','context_info','conversation_group_id','conversation_handle','counter','data','data_compression','data_source','database','database_name','database_principal_id','date','date_format','datefirst','dateformat','day','dayofyear','days','db_failover','db_id','db_name','dbid','dbidexec','deadlock_priority','default_database','default_logon_domain','definition','delayed_durability','deny','description','desired_state','dtc_support','durability','enabled','encoding','encrypted','encrypted_value','encryption_type','endpoint','endpoint_url','entity_id','entity_name','error_message','error_number','error_severity','error_state','event','expiredate','expiry_date','failover_mode','failure_condition_level','federated_service_account','fetch_status','field_terminator','file_id','file_name','filegroup_id','filename','first','flush_interval_seconds','format_type','guid','hash','header_limit','health_check_timeout','high','host_name','identity','ignore_dup_key','index','interval_length_minutes','key_guid','key_id','key_name','key_path','key_store_provider_name','label','langid','language','length','level','lifetime','location','lock_escalation','lock_timeout','login_type','low','max_duration','max_outstanding_io_per_volume','max_plans_per_query','max_storage_size_mb','message','messages','minutes','mirror_address','month','monthname','name','namespace','object_id','object_name','objid','optimize_for_sequential_key','or','order','parameters','partition_id','password','path','permission_name','permission_set','Permissions','platform','population','port','precision','predicate','priority','procedure_name','program_name','provider','quarter','query_capture_mode','quoted_identifier','rank','read','read_only_routing_url','reject_sample_value','reject_type','reject_value','remote_service_name','replace','required_synchronized_secondaries_to_commit','resource_manager_location','retention_period','role','root','row','row_terminator','rowcount','rows','schema_id','schema_name','secondary_type','seeding_mode','sequence','serde_method','server','service_name','session_id','session_timeout','setopts','shard_map_name','sid','site','size_based_cleanup_mode','source','sql','sql_handle','ssl_port','start_date','state','statement','status','string_delimiter','subject','system','target','text','TextPtr','timeout','timestamp','trim','type','type_desc','type_name','uid','url','use_type_default','user','user_id','validation','value','version','weekday','weight','year'
					) OR x.[name] LIKE '%[^a-z0-9_#]%' OR x.[name] LIKE '[0-9]%', QUOTENAME(x.[name]), x.[name])
			) y
		WHERE x.is_hidden = 0
	) x
	ORDER BY x.ColOrder
	FOR XML PATH ('')
);

-- Add prepend/append CREATE TABLE code
SELECT @output = CONCAT('IF OBJECT_ID(''tempdb..#tmpTable'',''U'') IS NOT NULL DROP TABLE #tmpTable; --SELECT TOP(100) * FROM #tmpTable;', '{{br}}',
						'CREATE TABLE #tmpTable (', '{{br}}', @output, ');');
-- Swap in line breaks
SELECT @output = REPLACE(@output, '{{br}}', CHAR(13)+CHAR(10));
-- Return result
RAISERROR(@output,0,1) WITH NOWAIT; SELECT @output;
