WITH cte AS (
	SELECT * FROM sys.partition_range_values
	UNION ALL
	SELECT prv.function_id, IIF(pf.boundary_value_on_right = 0, MAX(prv.boundary_id)+1, MIN(prv.boundary_id)-1), prv.parameter_id, NULL
	FROM sys.partition_functions pf
		JOIN sys.partition_range_values prv ON prv.function_id = pf.function_id
	GROUP BY pf.[name], prv.function_id, prv.parameter_id, pf.boundary_value_on_right
)
SELECT f.function_id
	, function_name = f.[name]
	, function_type = IIF(f.boundary_value_on_right = 0, 'RANGE LEFT', 'RANGE RIGHT')
	, f.fanout
	, f.boundary_value_on_right
	, parameter_datatype = TYPE_NAME(p.system_type_id)
	, rv.boundary_id
	, partition_number = rv.boundary_id + f.boundary_value_on_right
	, boundary_value = rv.[value]
	, N'█' [█]
	, partition_lower_bound = COALESCE(IIF(f.boundary_value_on_right = 0, LAG(rv.[value]) OVER win, rv.[value]), '(Min Value)')
	, [ ] = IIF(f.boundary_value_on_right = 0, '> ', '>=') + ' (Value) ' + IIF(f.boundary_value_on_right = 0, '<=', '<')
	, partition_upper_bound = COALESCE(IIF(f.boundary_value_on_right = 0, rv.[value], LEAD(rv.[value]) OVER win), '(Max Value)')
FROM sys.partition_functions f
	JOIN sys.partition_parameters p ON p.function_id = f.function_id
	JOIN cte rv ON f.function_id = rv.function_id
WINDOW win AS (PARTITION BY f.function_id ORDER BY rv.boundary_id)
ORDER BY function_id, boundary_id;
