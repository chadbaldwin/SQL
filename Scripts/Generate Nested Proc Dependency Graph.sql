SET NOCOUNT ON;
----------------------------------------------------------------------

----------------------------------------------------------------------
DROP TABLE IF EXISTS #targets;
CREATE TABLE #targets (
    [object_id] int NOT NULL,
    SchemaName nvarchar(128) NOT NULL,
    ObjectName nvarchar(128) NOT NULL,
    FQON nvarchar(500) NOT NULL,
)

INSERT INTO #targets ([object_id], SchemaName, ObjectName, FQON)
SELECT o.[object_id], SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name]
    , FQON = CONCAT(QUOTENAME(SCHEMA_NAME(o.[schema_id])), '.', QUOTENAME(o.[name]))
FROM sys.objects o
WHERE o.is_ms_shipped = 0
    AND o.[type] = 'P'
    --
    AND o.[object_id] IN (
        OBJECT_ID('dbo.usp_MyProc'),
        OBJECT_ID('dbo.usp_MyOtherProc')
    )
----------------------------------------------------------------------

----------------------------------------------------------------------
DROP TABLE IF EXISTS #tmp_children;
WITH cte_find_children AS (
    -- Root nodes
    SELECT Referencing_ObjectServerName = CONVERT(nvarchar(128), NULL)
        , Referencing_ObjectDatabaseName = CONVERT(nvarchar(128), NULL)
        , Referencing_ObjectSchema = CONVERT(nvarchar(128), NULL)
        , Referencing_ObjectName = CONVERT(nvarchar(128), NULL)
        , Referencing_ObjectID = CONVERT(int, NULL)
        --
        , Referenced_ObjectServerName = CONVERT(nvarchar(128), NULL)
        , Referenced_ObjectDatabaseName = DB_NAME()
        , Referenced_ObjectSchema =  OBJECT_SCHEMA_NAME(o.[object_id])
        , Referenced_ObjectName = o.[name]
        , Referenced_ObjectID = o.[object_id]
        --
        , NestLevel = 0
        , ReferenceIDChain = ','+CONVERT(nvarchar(MAX), o.[object_id])
        , DeDupID = ROW_NUMBER() OVER (PARTITION BY o.[object_id] ORDER BY (SELECT NULL))
    FROM sys.objects o
    WHERE o.[type] = 'P'
        AND EXISTS (SELECT * FROM #targets t WHERE t.[object_id] = o.[object_id])
    UNION ALL
    -- Children
    SELECT Referencing_ObjectServerName = fc.Referencing_ObjectServerName 
        , Referencing_ObjectDatabaseName = fc.Referenced_ObjectDatabaseName
        , Referencing_ObjectSchema = fc.Referenced_ObjectSchema
        , Referencing_ObjectName = fc.Referenced_ObjectName
        , Referencing_ObjectID = fc.Referenced_ObjectID
        --
        , Referenced_ObjectServerName = d.referenced_server_name -- TODO: Maybe look up in sys.servers to validate?
        , Referenced_ObjectDatabaseName = rd.Referenced_ObjectDatabaseName
        , Referenced_ObjectSchema = OBJECT_SCHEMA_NAME(x.Referenced_ObjectID, DB_ID(rd.Referenced_ObjectDatabaseName))
        , Referenced_ObjectName = d.referenced_entity_name
        , Referenced_ObjectID = x.Referenced_ObjectID
        --
        , NestLevel = fc.NestLevel+1
        , ReferenceIDChain = CONVERT(nvarchar(MAX), CONCAT(fc.ReferenceIDChain, ',', x.Referenced_ObjectID))
        , DeDupID = ROW_NUMBER() OVER (PARTITION BY x.Referenced_ObjectID ORDER BY (SELECT NULL))
    FROM cte_find_children fc
        -- Get list of all objects within each prior object
        CROSS APPLY sys.dm_sql_referenced_entities(CONCAT(QUOTENAME(fc.Referenced_ObjectSchema),'.',QUOTENAME(fc.Referenced_ObjectName)), 'OBJECT') d
        -- If a database name isn't supplied, then we can assume it's the current DB
        -- If a server name is supplied, then database is always supplied as well, so we don't need to account for that situation
        CROSS APPLY (SELECT Referenced_ObjectDatabaseName = COALESCE(d.referenced_database_name, DB_NAME())) rd
        -- Getting the schema is the trickiest part because it's optional with 3 and 4 part names, so we just have to do our best here...
        CROSS APPLY (
            -- The actual schema rules used by SQL Server are more complicated than this as they also include the user's default schema, so this is a best attempt
            SELECT FQON_InheritSchema = CONCAT_WS('.'
                                            , QUOTENAME(rd.Referenced_ObjectDatabaseName) -- Database
                                            , QUOTENAME(COALESCE(NULLIF(d.referenced_schema_name,''), fc.Referenced_ObjectSchema)) -- Schema -- Inherit referencing object schema when missing
                                            , QUOTENAME(d.referenced_entity_name) -- Object
                                        )
                , FQON_DotDotSchema   = CONCAT_WS('.'
                                            , QUOTENAME(rd.Referenced_ObjectDatabaseName) -- Database
                                            , COALESCE(QUOTENAME(NULLIF(d.referenced_schema_name,'')),'') -- Schema -- Use .. schema shortcut
                                            , QUOTENAME(d.referenced_entity_name) -- Object
                                        )
        ) f
        CROSS APPLY (
            SELECT  Referenced_ObjectID = CASE
                                            WHEN d.referenced_server_name IS NOT NULL   THEN NULL -- If a server name is provided, there isn't anything we can do here to validate it, so assume it's valid
                                            WHEN d.referenced_id IS NOT NULL            THEN d.referenced_id -- Nothing fancy to do here since we have what we need
                                            WHEN d.referenced_id IS NULL                THEN COALESCE(OBJECT_ID(f.FQON_InheritSchema), OBJECT_ID(f.FQON_DotDotSchema)) -- First we try inheriting, then we try dotdot
                                            ELSE NULL
                                        END
        ) x
        CROSS APPLY (SELECT FQON = CONCAT_WS('.'
                                        , QUOTENAME(rd.Referenced_ObjectDatabaseName)
                                        , QUOTENAME(OBJECT_SCHEMA_NAME(x.Referenced_ObjectID, DB_ID(rd.Referenced_ObjectDatabaseName)))
                                        , QUOTENAME(OBJECT_NAME(x.Referenced_ObjectID, DB_ID(rd.Referenced_ObjectDatabaseName)))
                                    )) f2
    WHERE d.referencing_minor_id = 0 -- Object level references only, exclude things like column references
        AND d.referenced_entity_name NOT IN ('usp_RethrowError') -- Referenced objects we can ignore
        AND (x.Referenced_ObjectID IS NOT NULL OR d.referenced_server_name IS NOT NULL) -- Only return results where we can determine an object id, or it is an external server reference
        AND fc.Referencing_ObjectServerName IS NULL -- Nothing to check for external references
        AND fc.ReferenceIDChain NOT LIKE CONCAT('%,',x.Referenced_ObjectID,',%') -- Exclude objects already in chain to stop infinite recursion
        AND fc.DeDupID = 1 -- The previous iteration may return multiple instances of the same referencing object, but we only need to get children for each once
        AND (OBJECT_ID(f2.FQON, 'P') IS NOT NULL OR OBJECT_ID(f2.FQON, 'TR') IS NOT NULL OR d.referenced_server_name IS NOT NULL) -- Only procs/triggers and external references
)
SELECT *
INTO #tmp_children
FROM cte_find_children
----------------------------------------------------------------------

----------------------------------------------------------------------
DROP TABLE IF EXISTS #tmp_parents;
WITH cte_find_parents AS (
    -- Parents are a little easier to find because the referencing dependency view can't know about external references, like other databases or servers
    -- We would have to use dynamic SQL or looping and run this code against all other local databases in order to get cross database parent references
    -- Root nodes
    SELECT Referencing_ObjectSchema = SCHEMA_NAME(og.[schema_id])
        , Referencing_ObjectName = og.[name]
        , Referencing_ObjectID = og.[object_id]
        --
        , Referenced_ObjectSchema = SCHEMA_NAME(od.[schema_id])
        , Referenced_ObjectName = od.[name]
        , Referenced_ObjectID = od.[object_id]
        --
        , ReferenceIDChain = CONVERT(nvarchar(MAX), CONCAT(og.[object_id], ',', od.[object_id]))
        , DeDupID = ROW_NUMBER() OVER (PARTITION BY og.[object_id] ORDER BY (SELECT NULL))
    FROM sys.objects od
        CROSS APPLY sys.dm_sql_referencing_entities(CONCAT(QUOTENAME(SCHEMA_NAME(od.[schema_id])), '.', QUOTENAME(od.[name])), 'OBJECT') g
        JOIN sys.objects og ON og.[object_id] = g.referencing_id
    WHERE 1=1
        AND EXISTS (SELECT * FROM #targets t WHERE t.[object_id] = od.[object_id])
        AND og.[type] IN ('P','TR') AND od.[type] IN ('P','TR') -- Limit to only Proc/Trigger to Proc/Trigger calls
    UNION ALL
    -- Parents
    SELECT Referencing_ObjectSchema = SCHEMA_NAME(og.[schema_id])
        , Referencing_ObjectName = og.[name]
        , Referencing_ObjectID = og.[object_id]
        --
        , Referenced_ObjectSchema = fd.Referencing_ObjectSchema
        , Referenced_ObjectName = fd.Referencing_ObjectName
        , Referenced_ObjectID = fd.Referencing_ObjectID
        --
        , ReferenceIDChain = CONCAT(og.[object_id], ',', fd.ReferenceIDChain)
        , DeDupID = ROW_NUMBER() OVER (PARTITION BY og.[object_id] ORDER BY (SELECT NULL))
    FROM cte_find_parents fd
        CROSS APPLY sys.dm_sql_referencing_entities(CONCAT(fd.Referencing_ObjectSchema, '.', fd.Referencing_ObjectName), 'OBJECT') g
        JOIN sys.objects og ON og.[object_id] = g.referencing_id
    WHERE og.[type] IN ('P','TR')
        AND ','+fd.ReferenceIDChain+',' NOT LIKE CONCAT('%,',og.[object_id],',%') -- Exclude objects already in chain to stop infinite recursion
        AND fd.DeDupID = 1 -- The previous iteration may return multiple instances of the same referencing object, but we only need to get children for each once
)
SELECT *
INTO #tmp_parents
FROM cte_find_parents p
----------------------------------------------------------------------

----------------------------------------------------------------------
DROP TABLE IF EXISTS #relationships;
SELECT *, dot_relationship = CONCAT('"', x.parent, '" -> "', x.child, '"')
INTO #relationships
FROM (
    SELECT parent = CONCAT_WS('.'
                , QUOTENAME(c.Referencing_ObjectServerName)
                , QUOTENAME(IIF(c.Referencing_ObjectServerName IS NULL AND c.Referencing_ObjectDatabaseName = DB_NAME(), NULL, c.Referencing_ObjectDatabaseName))
                , QUOTENAME(c.Referencing_ObjectSchema)
                , QUOTENAME(c.Referencing_ObjectName)
            )
        , child = CONCAT_WS('.'
                , QUOTENAME(c.Referenced_ObjectServerName)
                , QUOTENAME(IIF(c.Referenced_ObjectServerName IS NULL AND c.Referenced_ObjectDatabaseName = DB_NAME(), NULL, c.Referenced_ObjectDatabaseName))
                , QUOTENAME(c.Referenced_ObjectSchema)
                , QUOTENAME(c.Referenced_Objectname)
            )
    FROM #tmp_children c
    WHERE c.NestLevel > 0 -- Exclude root level
    UNION
    SELECT parent = CONCAT_WS('.', QUOTENAME(p.Referencing_ObjectSchema), QUOTENAME(p.Referencing_ObjectName))
        , child = CONCAT_WS('.', QUOTENAME(p.Referenced_ObjectSchema), QUOTENAME(p.Referenced_Objectname))
    FROM #tmp_parents p
) x


DECLARE @crlf nchar(2) = NCHAR(13)+NCHAR(10),
        @tab nchar(1) = NCHAR(9),
        @rank_same_top nvarchar(MAX),
        @rank_same_bottom nvarchar(MAX),
        @targets nvarchar(MAX);

SELECT @targets = STRING_AGG(CONVERT(nvarchar(MAX), @tab+CONCAT('"', FQON, '" [color=blue, penwidth=5]')), @crlf)
FROM #targets

DECLARE @d nvarchar(10) = @crlf+@tab+@tab

SELECT @rank_same_top       = CONCAT_WS(@crlf+@tab, @tab+'{', @tab+'rank=same', @tab+STRING_AGG(CONVERT(nvarchar(MAX), x.val), @d), '}')
FROM (
    SELECT DISTINCT val = CONCAT('"', p.parent, '" [style=filled, fillcolor=lightgreen]')
    FROM #relationships p
    WHERE NOT EXISTS (SELECT * FROM #relationships p2 WHERE p2.child = p.parent) -- find all parents without parents
) x

SELECT @rank_same_bottom    = CONCAT_WS(@crlf+@tab, @tab+'{', @tab+'rank=same', @tab+STRING_AGG(CONVERT(nvarchar(MAX), x.val), @d), '}')
FROM (
    SELECT DISTINCT val = CONCAT('"', p.child, '" [style=filled, fillcolor=pink]')
    FROM #relationships p
    WHERE NOT EXISTS (SELECT * FROM #relationships p2 WHERE p2.parent = p.child) -- find all children without children
) x
----------------------------------------------------------------------

----------------------------------------------------------------------
          SELECT 'Paste the following code into your preferred'
UNION ALL SELECT 'Graphviz renderer. Suggested: VSCode with the'
UNION ALL SELECT '"Graphviz Interactive Preview" extension.'

DECLARE @final nvarchar(MAX);
SELECT @final = CONCAT_WS(@crlf
        , 'digraph G {'
        , @tab+'rankdir=LR'
        , ''
        , @tab+'# targets'
        , @targets
        , ''
        , @tab+'# relationships'
        , STRING_AGG(CONVERT(nvarchar(MAX), @tab+x.dot_relationship), @crlf)
        , ''
        , @tab+'# top level parents same rank'
        , @rank_same_top
        ,''
        , @tab+'# bottom level children same rank'
        , @rank_same_bottom
        , '}'
    )
FROM #relationships x

SELECT @final

-- TODO: Add line coloring for circular references?
