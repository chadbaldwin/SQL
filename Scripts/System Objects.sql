IF OBJECT_ID('tempdb..#so','U') IS NOT NULL DROP TABLE #so; --SELECT * FROM #so
SELECT system_object_id = so.[object_id], so.[name], y.Category
    , NameWithParams = CONCAT(SCHEMA_NAME(so.[schema_id]), '.', so.[name], x.Params), so.[type], so.[type_desc]
    , y.IsCompatView, y.IsUndocumented, y.IsDeprecated, y.IsUnsupported
    --
    , has_database_id			= scd.[name]
    , has_schema_id				= scs.[name]
    , has_object_id				= sco.[name]
    , has_column_id				= scc.[name]
    , has_index_id				= sci.[name]
    , has_parent_object_id		= scpo.[name]
    , has_parent_column_id		= scpc.[name]
    , has_data_space_id			= scds.[name]
    , has_file_id				= scf.[name]
    , has_session_id			= scss.[name]
    , has_statement_sql_handle	= scst.[name]
    , has_sql_handle			= scsh.[name]
    , has_plan_handle			= scph.[name]
    , has_is_ms_shipped			= mss.[name]
    , has_other_object_id		= oid.column_names
INTO #so
FROM sys.system_objects so
    LEFT JOIN sys.system_columns scd  ON scd.[object_id]  = so.[object_id] AND scd.[name]  IN ('database_id','db_id','dbid')
    LEFT JOIN sys.system_columns scs  ON scs.[object_id]  = so.[object_id] AND scs.[name]  IN ('schema_id','idSch')
    LEFT JOIN sys.system_columns sco  ON sco.[object_id]  = so.[object_id] AND sco.[name]  IN ('object_id','objectid','table_id','tabid','objid')
    LEFT JOIN sys.system_columns scc  ON scc.[object_id]  = so.[object_id] AND scc.[name]  IN ('column_id','columnid')
    LEFT JOIN sys.system_columns sci  ON sci.[object_id]  = so.[object_id] AND sci.[name]  IN ('index_id','stats_id','unique_index_id','indexid')
    LEFT JOIN sys.system_columns scpo ON scpo.[object_id] = so.[object_id] AND scpo.[name] IN ('parent_object_id')
    LEFT JOIN sys.system_columns scpc ON scpc.[object_id] = so.[object_id] AND scpc.[name] IN ('parent_column_id')
    LEFT JOIN sys.system_columns scds ON scds.[object_id] = so.[object_id] AND scds.[name] IN ('data_space_id','filegroup_id','FileGroup ID')
    LEFT JOIN sys.system_columns scf  ON scf.[object_id]  = so.[object_id] AND scf.[name]  IN ('file_id', 'File ID', 'FileId')
    LEFT JOIN sys.system_columns mss  ON mss.[object_id]  = so.[object_id] AND mss.[name]  IN ('is_ms_shipped')
    LEFT JOIN sys.system_columns scss ON scss.[object_id] = so.[object_id] AND scss.[name] IN ('session_id')
    LEFT JOIN sys.system_columns scst ON scst.[object_id] = so.[object_id] AND scst.[name] IN ('statement_sql_handle')
    LEFT JOIN sys.system_columns scsh ON scsh.[object_id] = so.[object_id] AND scsh.[name] IN ('sql_handle')
    LEFT JOIN sys.system_columns scph ON scph.[object_id] = so.[object_id] AND scph.[name] IN ('plan_handle')
	OUTER APPLY ( -- Find other columns that might contain an object_id
		SELECT column_names = STRING_AGG(CONVERT(nvarchar(MAX), sc.[name]), ', ') WITHIN GROUP (ORDER BY sc.[name])
		FROM sys.system_columns sc
		WHERE sc.[object_id] = so.[object_id]
			AND sc.[name] LIKE '%[_]object[_]id'
			AND scpo.[name] IS NULL
	) oid
    OUTER APPLY (
        SELECT Params = '(' + STRING_AGG(CONVERT(nvarchar(MAX), CONCAT(p.[name], ' ', TYPE_NAME(p.system_type_id))), ', ') WITHIN GROUP (ORDER BY p.parameter_id) + ')'
        FROM sys.system_parameters p
        WHERE p.[object_id] = so.[object_id]
            AND p.parameter_id > 0
    ) x
    CROSS APPLY (SELECT MatchName = '_'+so.[name]+'_') m
    CROSS APPLY (
            -- System Compatibility views
        SELECT IsCompatView     = IIF(so.[name] IN ('sysaltfiles','syscacheobjects','syscharsets','syscolumns','syscomments','sysconfigures','sysconstraints','syscurconfigs','sysdatabases','sysdepends','sysdevices','sysfilegroups','sysfiles','sysforeignkeys','sysfulltextcatalogs','sysindexes','sysindexkeys','syslanguages','syslockinfo','syslogins','sysmembers','sysmessages','sysobjects','sysoledbusers','sysperfinfo','syspermissions','sysprocesses','sysprotects','sysreferences','sysremotelogins','sysservers','systypes','sysusers'), 1, 0)
            -- Unsupported / undocumented / internal use only
            , IsUndocumented    = IIF(so.[name] IN ('dm_db_database_page_allocations','dm_db_mirroring_past_actions','dm_db_rda_migration_status','dm_db_rda_schema_update_status','dm_db_script_level','dm_db_stats_properties_internal','dm_db_xtp_checkpoint_internals','dm_logconsumer_cachebufferrefs','dm_logconsumer_privatecachebuffers','dm_logpool_consumers','dm_logpool_hashentries','dm_logpool_sharedcachebuffers','dm_logpool_stats','dm_logpoolmgr_freepools','dm_logpoolmgr_respoolsize','dm_logpoolmgr_stats','dm_os_dispatchers','dm_os_enumerate_filesystem','dm_os_file_exists','dm_os_memory_broker_clerks','dm_os_memory_node_access_stats','dm_tran_global_recovery_transactions','dm_tran_global_transactions','dm_tran_global_transactions_enlistments','dm_tran_global_transactions_log','dm_xtp_threads','dm_xtp_transaction_recent_rows','fn_cColvEntries_80','fn_cdc_check_parameters','fn_cdc_hexstrtobin','fn_column_store_row_groups','fn_dblog','fn_dblog_xtp','fn_dump_dblog_xtp','fn_EnumCurrentPrincipals','fn_fIsColTracked','fn_full_dblog','fn_GetCurrentPrincipal','fn_GetRowsetIdFromRowDump','fn_hadr_is_same_replica','fn_helpdatatypemap','fn_IsBitSetInBitmask','fn_isrolemember','fn_MapSchemaType','fn_MSdayasnumber','fn_MSgeneration_downloadonly','fn_MSget_dynamic_filter_login','fn_MSorbitmaps','fn_MSrepl_getsrvidfromdistdb','fn_MSrepl_map_resolver_clsid','fn_MStestbit','fn_MSvector_downloadonly','fn_numberOf1InBinaryAfterLoc','fn_numberOf1InVarBinary','fn_PhysLocCracker','fn_PhysLocFormatter','fn_repladjustcolumnmap','fn_repldecryptver4','fn_replformatdatetime','fn_replgetcolidfrombitmap','fn_replgetparsedddlcmd','fn_replp2pversiontotranid','fn_replreplacesinglequote','fn_replreplacesinglequoteplusprotectstring','fn_repluniquename','fn_replvarbintoint','fn_RowDumpCracker','fn_sqlagent_job_history','fn_sqlagent_jobs','fn_sqlagent_jobsteps','fn_sqlagent_jobsteps_logs','fn_sqlagent_subsystems','fn_sqlvarbasetostr','fn_varbintohexstr','fn_varbintohexsubstring','fn_yukonsecuritymodelrequired','resource_governor_external_resource_pool_affinity','resource_governor_resource_pool_affinity','selective_xml_index_namespaces','server_principal_credentials','syscscontainers','syscursorcolumns','syscursorrefs','syscursors','syscursortables','system_internals_allocation_units','system_internals_partition_columns','system_internals_partitions','via_endpoints'), 1, 0)
            -- Explicitly not supported
            , IsUnsupported     = IIF(so.[name] IN ('dm_os_function_symbolic_name', 'dm_os_memory_allocations', 'dm_os_ring_buffers','dm_os_sublatches','dm_os_worker_local_storage','dm_exec_query_transformation_stats'), 1, 0)
            , IsDeprecated      = IIF(so.[name] IN ('fn_get_sql'), 1, 0)
            , Category          = CASE 
                                    WHEN m.MatchName LIKE '%[_]availability[_]%' OR m.MatchName LIKE '%[_]hadr[_]%' OR m.MatchName LIKE '%[_]cluster[_]%' THEN 'AG'
                                    WHEN m.MatchName LIKE '%[_]fulltext[_]%' OR m.MatchName LIKE '%[_]fts[_]%' THEN 'FTS'
                                    WHEN m.MatchName LIKE '%[_]xtp[_]%' THEN 'In-Memory'
                                    WHEN m.MatchName LIKE '%[_]xe[_]%' OR m.MatchName LIKE '%[_]trace[_]%'  THEN 'Extended Events / Traces'
                                    WHEN m.MatchName LIKE '%[_]resource[_]governor[_]%' THEN 'Resource Governor'
                                    WHEN m.MatchName LIKE '%[_]column[_]store[_]%' THEN 'Columnstore'
                                    WHEN m.MatchName LIKE '%[_]query[_]store[_]%' THEN 'Query Store'
                                    WHEN m.MatchName LIKE '%[_]xml[_]schema[_]%' THEN 'XML Schema'
                                    WHEN m.MatchName LIKE '%[_]mirroring[_]%' THEN 'Database Mirroring'
                                    WHEN m.MatchName LIKE '%[_]repl[_]%' THEN 'Replication'
                                    WHEN m.MatchName LIKE '%[_]endpoints[_]%' OR m.MatchName LIKE '%[_]endpoint[_]%' THEN 'Endpoints'
                                    WHEN m.MatchName LIKE '%[_]cryptographic[_]provider[_]%' OR so.[name] = 'cryptographic_providers' THEN 'Cryptographic Providers'
                                    WHEN m.MatchName LIKE '%[_]clr[_]%' OR m.MatchName LIKE '%[_]assembly[_]%' OR m.MatchName LIKE '%[_]assemblies[_]%' THEN 'Assemblies / CLR'
                                    WHEN m.MatchName LIKE '%[_]filestream[_]%' OR m.MatchName LIKE '%[_]filetable[_]%' OR so.[name] = 'filetables' THEN 'FILESTREAM / FileTable'
                                    ELSE NULL
                                    END
    ) y
WHERE 1=1
    --AND so.[type] NOT IN ('P','X','AF','FS','PC','FN') -- Exclude procs, extended procs, aggregate function, CLR proc, scalar functions
    AND so.[type] NOT IN ('P','X','AF','PC')
    AND SCHEMA_NAME(so.[schema_id]) = 'sys';

SELECT *
FROM #so
ORDER BY [name]
