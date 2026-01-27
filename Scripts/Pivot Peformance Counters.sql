DECLARE @script nvarchar(MAX);
DECLARE cursor_scripts CURSOR LOCAL FAST_FORWARD READ_ONLY
FOR SELECT REPLACE(REPLACE(REPLACE(
            CONVERT(nvarchar(MAX), CONCAT(
                'SELECT ', QUOTENAME(LEFT('object_name'+SPACE(200),MAX(LEN([object_name])) OVER ())), ' = ''{{object_name}}''
                    , ', QUOTENAME(LEFT('instance_name'+SPACE(200),MAX(MAX(instance_name_len)) OVER ())), ' = p.instance_name
                    ,  {{p.column_list}}
                FROM (
                    SELECT counter_name = RTRIM(CONVERT(nvarchar(128), [counter_name]))
                        , instance_name = RTRIM(CONVERT(nvarchar(128), [instance_name]))
                        , cntr_value = CONVERT(decimal(38,0), cntr_value)
                    FROM sys.dm_os_performance_counters
                    WHERE [object_name] = ''{{object_name}}''
                ) s
                    PIVOT (MAX(s.cntr_value) FOR s.counter_name IN ({{column_list}})) p
                ORDER BY p.instance_name;'
            ))
            , '{{object_name}}'  , x.[object_name])
            , '{{column_list}}'  ,      STRING_AGG(CONVERT(nvarchar(MAX), QUOTENAME(x.counter_name)), ', ')   WITHIN GROUP (ORDER BY x.counter_name))
            , '{{p.column_list}}', 'p.'+STRING_AGG(CONVERT(nvarchar(MAX), QUOTENAME(x.counter_name)), ', p.') WITHIN GROUP (ORDER BY x.counter_name))
    FROM (
        SELECT [object_name] = RTRIM(CONVERT(nvarchar(128), [object_name]))
            , counter_name = RTRIM(CONVERT(nvarchar(128), [counter_name]))
            , instance_name_len = MAX(LEN(instance_name))
        FROM sys.dm_os_performance_counters
        GROUP BY [object_name], counter_name
    ) x
    WHERE [object_name] NOT IN ('SQLServer:Deprecated Features')
    GROUP BY [object_name]
    ORDER BY [object_name];

OPEN cursor_scripts; FETCH NEXT FROM cursor_scripts INTO @script;
WHILE (@@FETCH_STATUS = 0)
BEGIN;
    EXEC sys.sp_executesql @stmt = @script
    FETCH NEXT FROM cursor_scripts INTO @script;
END;
CLOSE cursor_scripts; DEALLOCATE cursor_scripts;
