DECLARE @Login nvarchar(128) = 'DOMAIN\myusername';

DROP TABLE IF EXISTS #tmp;
SELECT s.[session_id], s.[program_name], s.login_name, r.wait_resource
    , blocking_session_id = COALESCE(r.blocking_session_id, 0)
INTO #tmp
FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON r.[session_id] = s.[session_id]

DECLARE @crlf char(2) = CHAR(13) + CHAR(10);
DECLARE @template nvarchar(MAX) = CONCAT_WS(@crlf, 'digraph G {','rankdir=LR','node[shape=Mrecord, fontname="Consolas"]','{{data}}','}');

DECLARE @data nvarchar(MAX);
SELECT @data = CONCAT(STRING_AGG(CONVERT(nvarchar(MAX), x.nodes), @crlf), @crlf, STRING_AGG(CONVERT(nvarchar(MAX), x.edges), @crlf))
FROM (
    SELECT nodes = CONCAT(c.[session_id],' [', x.node_label, ', '+x.node_style,']')
        , edges = IIF(c.blocking_session_id > 0, CONCAT(c.[session_id], ' -> ', c.blocking_session_id), NULL)
    FROM #tmp c
        CROSS APPLY (
            SELECT node_label = CONCAT('label="'
                                    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(
                                        c.login_name,' (',c.[session_id],')'
                                        ,IIF(LEN(c.[program_name]) > 0, CONCAT('|','Program: ' , c.[program_name],'#~l'), NULL)
                                        ,IIF(LEN(c.wait_resource) > 0 , CONCAT('|','Resource: ', c.wait_resource ,'#~l'), NULL)
                                    ),'\','\\'),'{','\{'),'}','\}'),'"','\"'),'#~l','\l'), '"')
                , node_style = CASE WHEN c.blocking_session_id = 0 THEN 'style=filled, fillcolor=pink' WHEN c.login_name = @Login THEN 'style=filled, fillcolor=lightgreen' ELSE NULL END
        ) x
    WHERE EXISTS (
            SELECT *
            FROM #tmp r2
            WHERE r2.blocking_session_id > 0
                AND c.[session_id] IN (r2.[session_id], r2.blocking_session_id)
        )
) x
GROUP BY ();

SELECT REPLACE(@template, '{{data}}', @data);
