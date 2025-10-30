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
	) dt;
