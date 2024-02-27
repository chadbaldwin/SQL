SELECT so.[object_id], so.[name], y.Category
    , NameWithParams = CONCAT(SCHEMA_NAME(so.[schema_id]), '.', so.[name], x.Params), so.[type], so.[type_desc]
    , y.IsCompatView, y.IsUndocumented, y.IsUnsupported
    --
    , [database_id] = scd.[name], [schema_id] = scs.[name], [object_id] = sco.[name], [column_id] = scc.[name]
    , [index_id] = sci.[name], [parent_object_id] = scpo.[name], [parent_column_id] = scpc.[name], data_space_id = scds.[name]
    , [file_id] = scf.[name], is_ms_shipped = mss.[name]
FROM sys.system_objects so
    LEFT JOIN sys.system_columns scd  ON scd.[object_id]  = so.[object_id] AND scd.[name]  IN ('database_id','db_id','dbid')
    LEFT JOIN sys.system_columns scs  ON scs.[object_id]  = so.[object_id] AND scs.[name]  IN ('schema_id','idSch')
    LEFT JOIN sys.system_columns sco  ON sco.[object_id]  = so.[object_id] AND sco.[name]  IN ('object_id','objectid','table_id','tabid','objid')
    LEFT JOIN sys.system_columns scc  ON scc.[object_id]  = so.[object_id] AND scc.[name]  IN ('column_id','columnid')
    LEFT JOIN sys.system_columns sci  ON sci.[object_id]  = so.[object_id] AND sci.[name]  IN ('index_id','stats_id','unique_index_id','indexid')
    LEFT JOIN sys.system_columns scpo ON scpo.[object_id] = so.[object_id] AND scpo.[name] = 'parent_object_id'
    LEFT JOIN sys.system_columns scpc ON scpc.[object_id] = so.[object_id] AND scpc.[name] = 'parent_column_id'
    LEFT JOIN sys.system_columns scds ON scds.[object_id] = so.[object_id] AND scds.[name] IN ('data_space_id','filegroup_id','FileGroup ID')
    LEFT JOIN sys.system_columns scf  ON scf.[object_id]  = so.[object_id] AND scf.[name]  IN ('file_id', 'File ID', 'FileId')
    LEFT JOIN sys.system_columns mss  ON mss.[object_id]  = so.[object_id] AND mss.[name]  = 'is_ms_shipped'
    OUTER APPLY (
        SELECT Params = '(' + STRING_AGG(CONCAT(p.[name], ' ', TYPE_NAME(p.system_type_id)), ', ') WITHIN GROUP (ORDER BY p.parameter_id) + ')'
        FROM sys.system_parameters p
        WHERE p.[object_id] = so.[object_id]
            AND p.parameter_id > 0
    ) x
    CROSS APPLY (SELECT MatchName = '_'+so.[name]+'_') m
    CROSS APPLY (
            -- System Compatibility views
        SELECT IsCompatView     = IIF(so.[name] IN ('sysaltfiles','syscacheobjects','syscharsets','syscolumns','syscomments','sysconfigures','sysconstraints','syscurconfigs'
                                        ,'sysdatabases','sysdepends','sysdevices','sysfilegroups','sysfiles','sysforeignkeys','sysfulltextcatalogs','sysindexes','sysindexkeys'
                                        ,'syslanguages','syslockinfo','syslogins','sysmembers','sysmessages','sysobjects','sysoledbusers','sysperfinfo','syspermissions'
                                        ,'sysprocesses','sysprotects','sysreferences','sysremotelogins','sysservers','systypes','sysusers','syscursorcolumns','syscursorrefs'
                                        ,'syscursors','syscursortables','sysopentapes','syscscontainers'), 1, 0)
            -- Unsupported / undocumented / internal use only
            , IsUndocumented    = IIF(so.[name] IN ('dm_db_database_page_allocations','dm_db_mirroring_past_actions','dm_db_script_level','dm_db_stats_properties_internal'
                                        ,'dm_db_xtp_checkpoint_internals','dm_logconsumer_cachebufferrefs','dm_logconsumer_privatecachebuffers','dm_logpool_consumers',
                                        'dm_logpool_hashentries','dm_logpool_sharedcachebuffers','dm_logpool_stats','dm_logpoolmgr_freepools','dm_logpoolmgr_respoolsize',
                                        'dm_logpoolmgr_stats','dm_os_dispatchers','dm_os_enumerate_filesystem','dm_os_file_exists','dm_os_memory_broker_clerks',
                                        'dm_os_memory_node_access_stats','dm_tran_global_recovery_transactions','dm_tran_global_transactions_enlistments',
                                        'dm_tran_global_transactions_log','dm_tran_global_transactions','dm_xtp_threads','dm_xtp_transaction_recent_rows',
                                        'resource_governor_external_resource_pool_affinity','resource_governor_resource_pool_affinity','selective_xml_index_namespaces',
                                        'server_principal_credentials','system_internals_allocation_units','system_internals_partition_columns','system_internals_partitions',
                                        'via_endpoints','fn_column_store_row_groups','fn_dblog','fn_dblog_xtp','fn_dump_dblog_xtp','fn_full_dblog','fn_EnumCurrentPrincipals',
                                        'fn_helpdatatypemap','fn_PhysLocCracker','fn_replgetcolidfrombitmap','fn_RowDumpCracker','fn_sqlagent_job_history'), 1, 0)
            -- Explicitly not supported
            , IsUnsupported     = IIF(so.[name] IN ('dm_os_memory_allocations','dm_os_ring_buffers','dm_os_sublatches','dm_os_worker_local_storage','dm_exec_query_transformation_stats'), 1, 0)
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
WHERE so.[type] NOT IN ('P','X','AF','FS','PC','FN')
    AND SCHEMA_NAME(so.[schema_id]) = 'sys'
    AND y.IsCompatView = 0
    AND y.IsUnsupported = 0
    AND y.IsUndocumented = 0
ORDER BY y.Category, so.[name]
