DROP TABLE IF EXISTS #root_filter;
CREATE TABLE #root_filter (
    FQON nvarchar(500) NOT NULL,
);
------------------------------------------------------------

------------------------------------------------------------
/*  Root filters, must be specified as a fully qualified object name (FQON)

    This will ensure that all relationships returned tie back to these objects at some point in the chain.

    This is helpful for very large databases with complex schemas where you only want to view parents and children
    of a specific set of tables.

    This only acts as a filter on nodes, not edges. If there are any objects (nodes which have FKs (edges) to other objects within
    the chain, those edges will still appear.

    Edges in red signify a direct link between root filter objects
    Edges in green signify a direct link to any root filter object from any other object
    Edges in blue signify self referencing relationships for any object.

    Root filter objects will be highlighted in red.
*/
INSERT #root_filter (FQON)
VALUES ('[dbo].[TableFoo]'),('[dbo].[TableBar]');
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #idx;
SELECT x.FQON, i.is_primary_key, i.is_unique_constraint, i.is_unique, ic.ColumnList
INTO #idx
FROM sys.indexes i
    CROSS APPLY (
        SELECT FQON = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(i.[object_id])), '.', QUOTENAME(OBJECT_NAME(i.[object_id])))
    ) x
    CROSS APPLY (
        SELECT ColumnList = STRING_AGG(CONVERT(nvarchar(MAX), QUOTENAME(COL_NAME(ic.[object_id], ic.column_id))), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
        FROM sys.index_columns ic
        WHERE ic.key_ordinal > 0
            AND ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    ) ic
WHERE i.is_unique = 1
    AND i.is_disabled = 0
    AND (i.is_primary_key = 1 OR EXISTS (SELECT * FROM sys.foreign_keys fk WHERE fk.is_disabled = 0 AND fk.referenced_object_id = i.[object_id] AND fk.key_index_id = i.index_id)); -- limit to PKs and indexes referenced by a FK
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #fk;
SELECT x.ParentFQON, fkc.ParentColumnList, x.ReferencedFQON, fkc.ReferencedColumnList
INTO #fk
FROM sys.foreign_keys fk
    CROSS APPLY (
        SELECT ParentFQON    = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id    )), '.', QUOTENAME(OBJECT_NAME(fk.parent_object_id    )))
            , ReferencedFQON = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)), '.', QUOTENAME(OBJECT_NAME(fk.referenced_object_id)))
    ) x
    CROSS APPLY (
        SELECT ParentColumnList    = STRING_AGG(CONVERT(nvarchar(MAX), QUOTENAME(COL_NAME(fkc.parent_object_id    , fkc.parent_column_id    ))), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id)
            , ReferencedColumnList = STRING_AGG(CONVERT(nvarchar(MAX), QUOTENAME(COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id))), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id)
        FROM sys.foreign_key_columns fkc
        WHERE fkc.constraint_object_id = fk.[object_id]
    ) fkc;
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #filter;
CREATE TABLE #filter (FQON nvarchar(500) NOT NULL);

IF EXISTS (SELECT * FROM #root_filter)
BEGIN;
    WITH cte_referencing AS (
        SELECT DISTINCT RefLevel = 1, x.ParentFQON, Chain = CONVERT(nvarchar(MAX), NCHAR(9999)+x.ParentFQON+NCHAR(9999))
        FROM #fk x WHERE EXISTS (SELECT * FROM #root_filter rf WHERE rf.FQON = x.ParentFQON)
        UNION ALL
        SELECT RefLevel = c.RefLevel + 1, x.ParentFQON, Chain = CONVERT(nvarchar(MAX), CONCAT(c.Chain, x.ParentFQON, NCHAR(9999)))
        FROM #fk x
            JOIN cte_referencing c ON c.ParentFQON = x.ReferencedFQON
        WHERE NOT EXISTS (SELECT * FROM STRING_SPLIT(c.Chain, NCHAR(9999)) y WHERE y.[value] = x.ParentFQON)
    ), cte_referenced AS (
        SELECT DISTINCT RefLevel = 1, x.ReferencedFQON, Chain = CONVERT(nvarchar(MAX), NCHAR(9999)+x.ReferencedFQON+NCHAR(9999))
        FROM #fk x WHERE EXISTS (SELECT * FROM #root_filter rf WHERE rf.FQON = x.ReferencedFQON)
        UNION ALL
        SELECT RefLevel = c.RefLevel + 1, x.ReferencedFQON, Chain = CONVERT(nvarchar(MAX), CONCAT(c.Chain, x.ReferencedFQON, NCHAR(9999)))
        FROM #fk x
            JOIN cte_referenced c ON c.ReferencedFQON = x.ParentFQON
        WHERE NOT EXISTS (SELECT * FROM STRING_SPLIT(c.Chain, NCHAR(9999)) y WHERE y.[value] = x.ReferencedFQON)
    )
    INSERT #filter (FQON)
    SELECT x.FQON
    FROM (
        SELECT FQON = c.ParentFQON FROM cte_referencing c
        UNION
        SELECT FQON = c.ReferencedFQON FROM cte_referenced c
    ) x
END;
------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS #dot;
SELECT *
INTO #dot
FROM (
    SELECT [Type] = 'node', x.ParentFQON, x.ReferencedFQON
        , String = CONCAT('"', x.ParentFQON, '" [', IIF(rf.IsRootFilter >= 1, 'style=filled, fillcolor=red, ', ''), 'label="', x.ParentFQON, '|', STRING_AGG(CONCAT('<', x.ColumnList, '>', x.ColumnList, ' ', x.Symbol), '|') WITHIN GROUP (ORDER BY x.[Type]), '"]')
    FROM (
        SELECT ParentFQON = x.FQON, ReferencedFQON = NULL, x.ColumnList, x.Symbol, x.[Type]
            , rn = ROW_NUMBER() OVER (PARTITION BY x.FQON, x.ColumnList ORDER BY x.[Type] DESC)
        FROM (
            SELECT x.FQON, x.ColumnList, Symbol = CHOOSE(s.[Type], N'🗝️', '[UQ]', '[UX]'), s.[Type]
            FROM #idx x
                CROSS APPLY (
                    SELECT [Type] = CASE
                                        WHEN x.is_primary_key = 1 THEN 1
                                        WHEN x.is_unique_constraint = 1 THEN 2
                                        WHEN x.is_unique = 1 THEN 3
                                        ELSE NULL
                                    END
                ) s
            UNION ALL
            SELECT x.ParentFQON, x.ParentColumnList, '[FK]', 4 FROM #fk x
        ) x
    ) x
        CROSS APPLY (SELECT IsRootFilter = COUNT(*) FROM #root_filter rf WHERE rf.FQON = x.ParentFQON) rf
    WHERE x.rn = 1
        AND EXISTS (SELECT * FROM #fk fk WHERE x.ParentFQON IN (fk.ParentFQON, fk.ReferencedFQON))
    GROUP BY x.ParentFQON, x.ReferencedFQON, rf.IsRootFilter
    ---------
    UNION ALL
    ---------
    SELECT [Type] = 'edge', fk.ParentFQON, fk.ReferencedFQON
        , String = CONCAT('"',fk.ParentFQON,'":"',fk.ParentColumnList,'" -> "',fk.ReferencedFQON,'":"',fk.ReferencedColumnList,'"'
                , CASE
                    WHEN fk.ParentFQON = fk.ReferencedFQON THEN ' [color=blue]'
                    WHEN prf.ParentIsRootFilter >= 1 AND rrf.ReferencedIsRootFilter >= 1 THEN ' [color=red]'
                    WHEN prf.ParentIsRootFilter >= 1 OR rrf.ReferencedIsRootFilter >= 1 THEN ' [color=green]'
                    ELSE ''
                  END -- If self-referencing, make edge color blue
            )
    FROM #fk fk
        OUTER APPLY (SELECT ParentIsRootFilter = COUNT(*) FROM #root_filter rf WHERE rf.FQON = fk.ParentFQON) prf
        OUTER APPLY (SELECT ReferencedIsRootFilter = COUNT(*) FROM #root_filter rf WHERE rf.FQON = fk.ReferencedFQON) rrf
) x
WHERE 1=1
    AND EXISTS (SELECT * FROM #filter f WHERE f.FQON = x.ParentFQON)
    AND (EXISTS (SELECT * FROM #filter f WHERE f.FQON = x.ReferencedFQON) OR x.ReferencedFQON IS NULL)
ORDER BY x.[Type] DESC, x.ParentFQON;
------------------------------------------------------------

------------------------------------------------------------
-- Return graphviz lines
SELECT 'digraph g {
    node [shape=record];
    rankdir=LR'
UNION ALL
SELECT '    '+String FROM #dot WHERE [Type] = 'node'
UNION ALL
SELECT '    '+String FROM #dot WHERE [Type] = 'edge'
UNION ALL
SELECT '}'
------------------------------------------------------------

------------------------------------------------------------
