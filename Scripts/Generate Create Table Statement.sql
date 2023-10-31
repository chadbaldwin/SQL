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
			, IIF(x.is_identity_column = 1, ' IDENTITY', NULL)							-- Add identity setting
			, ',{{br}}')
	FROM (
		SELECT y.ColName, ColType = x.system_type_name, ColOrder = x.column_ordinal, x.is_nullable, x.is_identity_column
			, MaxColNameLen = MAX(LEN(y.ColName)) OVER () + (4-(MAX(LEN(y.ColName)) OVER () % 4))
			, MaxTypeNameLen = MAX(LEN(x.system_type_name)) OVER () + (4-(MAX(LEN(x.system_type_name)) OVER () % 4))
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
										'action','address','application','data','date','day','dayofyear','definition','deny','description','enabled','endpoint',
										'expiredate','filename','hash','index','level','location','message','minutes','month','monthname','name','or','order',
										'password','path','population','port','priority','provider','quarter','rank','read','replace','role','row','rowcount',
										'sequence','server','source','state','status','subject','text','trim','type','uid','url','user','value','version','weekday',
										'year','object_id','schema_id','type_desc','lock_escalation','durability','messages','weight','ignore_dup_key','allow_row_locks',
										'allow_page_locks','compression_delay','partition_id'
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
