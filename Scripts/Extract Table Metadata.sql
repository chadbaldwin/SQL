/* Rules for consistency:
    - ID pointer columns (such as schema_id, history_table_id, default_object_id) should be converted to "*_name" columns
      UNLESS that object is part of the table definition. AKA, if you drop the table and that object disappears, then its
      metadata should be included in this snapshot. The only exception to this rule would probably be table triggers because
      their definitions are often quite large, and we can track triggers in their own separate object.

      If the column is changing to name, then the naming convention should be from "foo_id" to "foo_name". If the item has
      a schema associated with it, then it should be the fully-qualified name with square brackets and escaping. Otherwise
      just the name as a string is fine.

    - All parent level columns should be kept in original table order, minus excluded columns. Additional properties should
      appended to the end and the `extended_properties` property always goes at the end.

    - Typically excluded columns:
      - create_date
      - modify_date
      - is_ms_shipped

    - If parent table has `is_ms_shipped`, it does not need to be in the output, but it should be filtered on in the WHERE clause

    - If a sub-query should only ever return a single row, then it should be formatted as such in the JSON by using:
      `JSON_QUERY(<sub-query>)`
      as well as using `WITHOUT_ARRAY_WRAPPER`

      The combination of these two items will ensure the object returned is an object rather than an array, and ensures that
      SQL Server will interpret it as JSON rather than a string which needs escaping.
*/
/* Top level/generic considerations:
    - Remove all columns who have a matching `*_desc` column. No real reason to log and hold onto that data if it's less verbose and duplicated
    - In any case where a child has a name + schema_name, convert it to a combined `[schema_name].[name]` string. The opposite argument
      being that it would make more sense to keep everything separate in case we ever want to parse and use this data. It would be annoying
      to have to parse the value and determine schema/name. It probably makes more sense JSON wise, to just have a child object instead
      of a string.
*/
SELECT SchemaName        = SCHEMA_NAME(x.[schema_id])
    , ObjectName         = x.[name]
    , ObjectType         = RTRIM(CONVERT(varchar(2), x.[type] COLLATE DATABASE_DEFAULT))
    , [object_id]        = x.[object_id]
    , ObjectDateCreated  = x.create_date
    , ObjectDateModified = x.modify_date
    , DDL = JSON_QUERY((
        SELECT t.[name]
            , [schema_name] = SCHEMA_NAME(t.[schema_id])
            , t.[type], t.[type_desc], t.is_published, t.is_schema_published
            , t.lob_data_space_id, t.filestream_data_space_id /* Consideration: Possibly convert these over to names or objects? */
            , t.max_column_id_used /* Consideration: May remove this as it could cause a false change detection if the table is dropped and rebuilt */
            , t.lock_on_bulk_load, t.uses_ansi_nulls, t.is_replicated, t.has_replication_filter
            , t.is_merge_published, t.is_sync_tran_subscribed, t.has_unchecked_assembly_data, t.text_in_row_limit, t.large_value_types_out_of_row, t.is_tracked_by_cdc, t.[lock_escalation], t.lock_escalation_desc, t.is_filetable
            , t.is_memory_optimized, t.[durability], t.durability_desc, t.temporal_type, t.temporal_type_desc
            , [history_table] = JSON_QUERY((
                SELECT ht.[name], [schema_name] = SCHEMA_NAME(ht.[schema_id]), ht.[type], ht.[type_desc]
                FROM sys.tables ht
                WHERE ht.[object_id] = t.[object_id]
                    AND ht.is_ms_shipped = 0
                FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
            ))
            , t.is_remote_data_archive_enabled, t.is_external, t.history_retention_period, t.history_retention_period_unit, t.history_retention_period_unit_desc, t.is_node, t.is_edge
            --
            , [columns] = (
                SELECT c.[name]
                    , c.column_id /* Consideration: May remove this as it could cause a false change detection if the table is dropped and rebuilt. Instead, we could possibly rely on ordering */
                    , system_type_name = TYPE_NAME(c.system_type_id)
                    , user_type_name = TYPE_NAME(c.user_type_id)
                    , c.max_length, c.[precision], c.scale, c.collation_name, c.is_nullable, c.is_ansi_padded, c.is_rowguidcol, c.is_identity, c.is_computed, c.is_filestream, c.is_replicated, c.is_non_sql_subscribed, c.is_merge_published
                    , c.is_dts_replicated, c.is_xml_document, c.xml_collection_id
                    , [rule_object] = JSON_QUERY((
                        SELECT ro.[name], [schema_name] = SCHEMA_NAME(ro.[schema_id]), ro.[type], ro.[type_desc]
                        FROM sys.objects ro
                        WHERE ro.[object_id] = c.rule_object_id
                            AND ro.is_ms_shipped = 0
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , c.is_sparse, c.is_column_set, c.generated_always_type, c.generated_always_type_desc, c.[encryption_type], c.encryption_type_desc, c.encryption_algorithm_name, c.column_encryption_key_id, c.column_encryption_key_database_name
                    , c.is_hidden, c.is_masked, c.graph_type, c.graph_type_desc
                    --
                    , [identity_column] = JSON_QUERY((
                        SELECT ic.seed_value, ic.increment_value, ic.is_not_for_replication
                        FROM sys.identity_columns ic
                        WHERE c.[object_id] = ic.[object_id]
                            AND c.column_id = ic.column_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , [computed_column] = JSON_QUERY((
                        SELECT cc.[definition], cc.uses_database_collation, cc.is_persisted
                        FROM sys.computed_columns cc
                        WHERE c.[object_id] = cc.[object_id]
                            AND c.column_id = cc.column_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , [default_constraint] = JSON_QUERY((
                        SELECT dc.[name], [schema_name] = SCHEMA_NAME(dc.[schema_id])
                            , dc.is_published, dc.is_schema_published, dc.[definition], dc.is_system_named
                            --
                            , [extended_properties] = (
                                SELECT ep.[name], ep.[value]
                                FROM sys.extended_properties ep
                                WHERE ep.class = 1
                                    AND ep.major_id = dc.[object_id]
                                    AND ep.minor_id = 0
                                ORDER BY ep.[name]
                                FOR JSON AUTO, INCLUDE_NULL_VALUES
                            )
                        FROM sys.default_constraints dc
                        WHERE dc.parent_object_id = c.[object_id]
                            AND dc.parent_column_id = c.column_id
                            AND dc.is_ms_shipped = 0
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , [check_constraints] = ( -- yes, a column can have more than 1 check constraint
                        SELECT cc.[name], [schema_name] = SCHEMA_NAME(cc.[schema_id])
                            , cc.is_published, cc.is_schema_published, cc.is_disabled, cc.is_not_for_replication, cc.is_not_trusted
                            , parent_column_name = COL_NAME(cc.parent_object_id, cc.parent_column_id)
                            , cc.[definition], cc.uses_database_collation, cc.is_system_named
                            --
                            , [extended_properties] = (
                                SELECT ep.[name], ep.[value]
                                FROM sys.extended_properties ep
                                WHERE ep.class = 1
                                    AND ep.major_id = cc.[object_id]
                                    AND ep.minor_id = 0
                                ORDER BY ep.[name]
                                FOR JSON AUTO, INCLUDE_NULL_VALUES
                            )
                        FROM sys.check_constraints cc
                        WHERE cc.parent_object_id = c.[object_id]
                            AND cc.parent_column_id = c.column_id
                            AND cc.is_ms_shipped = 0
                        ORDER BY cc.[name]
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                    , [extended_properties] = (
                        SELECT ep.[name], ep.[value]
                        FROM sys.extended_properties ep
                        WHERE ep.class = 1
                            AND ep.major_id = c.[object_id]
                            AND ep.minor_id = c.column_id
                        ORDER BY ep.[name]
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                FROM sys.columns c
                WHERE c.[object_id] = t.[object_id]
                ORDER BY c.column_id
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [indexes] = (
                /* Not including index_id because they can be re-used and can change if a table or indexes are dropped and re-created
                    which can potentially trigger a false change detection.
                */
                SELECT i.[name], i.[type], i.[type_desc], i.is_unique
                    /* Consideration: Changing this section to remove attributes that are not directly related to the parent table object
                        Filegroups and partitions functions should be tracked as their own object
                        Attributes of the filegroups and partition functions are not technically a change to the table itself
                        Consider changing to name references only, for example:
                            "data_space": { "name": "ps_Foo", "type": "PS", "partition_function_name": "pf_Foo" }
                        This would go along with similar patterns used elsewhere.
                        
                        The other side of this consideration could be argued that if you change a partition scheme or a partition
                        function, that it will directly impact a table and it may be useful to trigger any affected tables
                        to be logged as a change in the schema capture. But you could still argue that with things like functions
                        that are used in a view. Should that view be seen as "changed" even if its definition and attributes did
                        not change? Probaly not.

                        UPDATE 1: Current solution is to exclude filegroup properties completely and to only include name and
                        type informaiton for the partition function, but not other attributes/settings. I feel this is somewhat
                        equivalent to logging a table trigger's top level information, like trigger type, but not the actual
                        definition of the trigger.
                    */
                    , [data_space] = JSON_QUERY((
                        SELECT ds.[name], ds.[type], ds.[type_desc]--, ds.is_default, ds.is_system
                          -- , [filegroup] = JSON_QUERY((
                          --     SELECT fg.filegroup_guid, fg.log_filegroup_id, fg.is_read_only, fg.is_autogrow_all_files
                          --     FROM sys.filegroups fg
                          --     WHERE fg.data_space_id = ds.data_space_id
                          --     FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                          -- ))
                            , [partition_function] = JSON_QUERY((
                                SELECT pf.[name], pf.[type], pf.[type_desc]--, pf.fanout, pf.boundary_value_on_right, pf.is_system
                                -- Not including extended properties as they are irrelevant to the status of the table object
                                FROM sys.partition_functions pf
                                WHERE pf.function_id = ps.function_id
                                FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                            ))
                        FROM sys.data_spaces ds
                            /* Note: sys.partition_schemes doesn't need to be a child object because it doesn't provide any extra
                               info other than the partition function function_id */
                            LEFT JOIN sys.partition_schemes ps ON ps.data_space_id = ds.data_space_id
                        WHERE ds.data_space_id = i.data_space_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , i.[ignore_dup_key], i.is_primary_key, i.is_unique_constraint
                    , i.fill_factor /* Consideration: Adding logic to normalize 100 back to 0 to prevent unecessary change detection */
                    , i.is_padded, i.is_disabled, i.is_hypothetical, i.is_ignored_in_optimization, i.[allow_row_locks], i.[allow_page_locks], i.has_filter, i.filter_definition, i.[compression_delay], i.suppress_dup_key_messages, i.auto_created
                    --
                    , [index_columns] = (
                        SELECT ic.index_column_id
                            , column_name = COL_NAME(ic.[object_id], ic.column_id)
                            , ic.key_ordinal, ic.partition_ordinal, ic.is_descending_key, ic.is_included_column
                        FROM sys.index_columns ic
                        WHERE ic.[object_id] = i.[object_id]
                            AND ic.index_id = i.index_id
                        ORDER BY ic.key_ordinal, ic.index_column_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                    -- TODO: Add partitions - need to figure out how to write query that collapses partitions by number range
                    , [key_constraint] = JSON_QUERY((
                        SELECT kc.[name], [schema_name] = SCHEMA_NAME(kc.[schema_id])
                            , kc.[type], kc.[type_desc], kc.is_published, kc.is_schema_published, kc.is_system_named, kc.is_enforced
                            --
                            , [extended_properties] = (
                                SELECT ep.[name], ep.[value]
                                FROM sys.extended_properties ep
                                WHERE ep.class = 1
                                    AND ep.major_id = kc.[object_id]
                                    AND ep.minor_id = 0
                                ORDER BY ep.[name]
                                FOR JSON AUTO, INCLUDE_NULL_VALUES
                            )
                        FROM sys.key_constraints kc
                        WHERE kc.parent_object_id = i.[object_id]
                            AND kc.unique_index_id = i.index_id
                            AND kc.is_ms_shipped = 0
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , [stats] = (
                        SELECT st.[name], st.user_created, st.auto_created, st.no_recompute, st.has_filter, st.filter_definition, st.is_temporary, st.is_incremental
						-- TODO: Add sys.stats_columns data
                        FROM sys.stats st
                        WHERE st.user_created = 1 -- Limited to only user created index stats to avoid unecessary bloat in this object
                            AND st.[object_id] = i.[object_id]
                            AND st.stats_id = i.index_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                    , [extended_properties] = (
                        SELECT ep.[name], ep.[value]
                        FROM sys.extended_properties ep
                        WHERE ep.class = 7
                            AND ep.major_id = i.[object_id]
                            AND ep.minor_id = i.index_id
                        ORDER BY ep.[name]
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                FROM sys.indexes i
                WHERE i.[object_id] = t.[object_id]
                ORDER BY i.[name]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [check_constraints] = (
                SELECT cc.[name], [schema_name] = SCHEMA_NAME(cc.[schema_id])
                    , cc.is_published, cc.is_schema_published, cc.is_disabled, cc.is_not_for_replication, cc.is_not_trusted, cc.[definition], cc.uses_database_collation, cc.is_system_named
                    --
                    , [extended_properties] = (
                        SELECT ep.[name], ep.[value]
                        FROM sys.extended_properties ep
                        WHERE ep.class = 1
                            AND ep.major_id = cc.[object_id]
                            AND ep.minor_id = 0
                        ORDER BY ep.[name]
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                FROM sys.check_constraints cc
                WHERE cc.parent_column_id = 0
                    AND cc.parent_object_id = t.[object_id]
                    AND cc.is_ms_shipped = 0
                ORDER BY cc.[name]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [foreign_keys] = (
                SELECT fk.[name], [schema_name] = SCHEMA_NAME(fk.[schema_id])
                    , fk.is_published, fk.is_schema_published
                    , [referenced_object] = JSON_QUERY((
                        SELECT fkro.[name], [schema_name] = SCHEMA_NAME(fkro.[schema_id]), fkro.[type], fkro.[type_desc]
                        FROM sys.objects fkro
                        WHERE fkro.[object_id] = fk.referenced_object_id
                            AND fkro.is_ms_shipped = 0
                        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                    ))
                    , key_index_name = NULLIF(fki.[name],'') -- hack to prevent JSON serialization from placing value in a sub-object
                    , fk.is_disabled, fk.is_not_for_replication, fk.is_not_trusted, fk.delete_referential_action, fk.delete_referential_action_desc, fk.update_referential_action, fk.update_referential_action_desc, fk.is_system_named
                    --
                    , [foreign_key_columns] = (
                        SELECT fkc.constraint_column_id
                            , parent_column_name = COL_NAME(fkc.parent_object_id, fkc.parent_column_id)
                            , referenced_column_name = COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id)
                        FROM sys.foreign_key_columns fkc
                        WHERE fkc.constraint_object_id = fk.[object_id]
                        ORDER BY fkc.constraint_column_id
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                    , [extended_properties] = (
                        SELECT ep.[name], ep.[value]
                        FROM sys.extended_properties ep
                        WHERE ep.class = 1
                            AND ep.major_id = fk.[object_id]
                            AND ep.minor_id = 0
                        ORDER BY ep.[name]
                        FOR JSON AUTO, INCLUDE_NULL_VALUES
                    )
                FROM sys.foreign_keys fk
                    JOIN sys.indexes fki ON fki.[object_id] = fk.referenced_object_id AND fki.index_id = fk.key_index_id
                WHERE fk.parent_object_id = t.[object_id]
                    AND fk.is_ms_shipped = 0
                ORDER BY fk.[name]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [periods] = ( -- not sure if a table can have multiple periods?
                SELECT per.[name], per.period_type, per.period_type_desc
                    , start_column_name = COL_NAME(per.[object_id], per.start_column_id)
                    , end_column_name = COL_NAME(per.[object_id], per.end_column_id)
                FROM sys.periods per
                WHERE per.[object_id] = t.[object_id]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [triggers] = (
                /* Excluding fields that only impact the trigger and have no impact on the parent table itself. */
                SELECT tr.[name], tr.[type], tr.[type_desc], tr.is_disabled/*, tr.is_not_for_replication*/, tr.is_instead_of_trigger
                FROM sys.triggers tr
                WHERE tr.parent_class = 1
                    AND tr.parent_id = t.[object_id]
                    AND tr.is_ms_shipped = 0
                ORDER BY tr.[name]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
            , [change_tracking] = JSON_QUERY((
                SELECT ctt.is_track_columns_updated_on
                FROM sys.change_tracking_tables ctt
                WHERE ctt.[object_id] = t.[object_id]
                FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
            ))
            , [extended_properties] = (
                SELECT ep.[name], ep.[value]
                FROM sys.extended_properties ep
                WHERE ep.class = 1
                    AND ep.major_id = t.[object_id]
                    AND ep.minor_id = 0
                ORDER BY ep.[name]
                FOR JSON AUTO, INCLUDE_NULL_VALUES
            )
        FROM sys.tables t
        WHERE t.[object_id] = x.[object_id]
        FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
    ))
FROM sys.tables x
WHERE x.is_ms_shipped = 0
ORDER BY SCHEMA_NAME(x.[schema_id]), x.[name];
