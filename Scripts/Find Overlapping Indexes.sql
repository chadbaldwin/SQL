/*  Needs to be `bigint` for reasons...
    decimal(38,0) is implicitly converted to float when getting the value of POWER(@2, N)
    With float, you lose precision...So 4611686018427387904 (2^62) becomes 4611686018427387900
    because it wants to treat it as 4.61168601842739E+18.

    However, if you use bigint...then it always treats it as a bigint and no precision is lost.
*/

/*  Types of overlap - All comparisons are include col ordinal insensitive:
    - Duplicate - entire index is duplicated - key cols, key ordinal, key sort, include cols
        - e.g. IndexA: (A,-B,C) Incl (D,F,E)   ---   IndexB: (A,-B,C) Incl (F,E,D)
    - Covered   - keys in parent index contains all keys (ordinal & sort) of the child, and contains all includes cols
        - e.g. IndexA: (A,-B,C) Incl (D,F,E)   ---   IndexB: (A,-B) Incl (F,D)
    - Mergable  - indexes are nearly covering - key cols are covered, includes are nearly covered - comparison index has small number of extra includes
        - e.g. IndexA: (A,-B,C) Incl (D,F,E)   ---   IndexB: (A,-B) Incl (F,D,G) -- IndexB has extra include column 'G'
*/

/*  Considerations:
*/

DECLARE @2                      bigint  = 2,
        @Mergable_ExtraColMax   tinyint = 4;

IF OBJECT_ID('tempdb..#tmp_idx','U') IS NOT NULL DROP TABLE #tmp_idx; --SELECT * FROM #tmp_idx
SELECT ID = IDENTITY(int), i.[object_id], i.index_id
    , SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]
    , ObjectType = o.[type_desc], IndexType = i.[type_desc]
    , i.is_unique, i.is_primary_key, i.is_unique_constraint, i.is_disabled, i.is_hypothetical, i.has_filter
    , x.KeyCols, x.InclCols, x.InclColsNoCLK, x.InclBitmap1, x.InclBitmap2, x.InclBitmap3
    , ixs.used_kb, ixs.reserved_kb
    , filter_definition = COALESCE(fd.NewFilterDef, '')
INTO #tmp_idx
FROM sys.indexes i
    JOIN sys.objects o ON o.[object_id] = i.[object_id]
    CROSS APPLY (
        SELECT KeyCols       = STRING_AGG(IIF(ic.is_included_column = 0, CONCAT(IIF(ic.is_descending_key    = 1, '-', ''), ic.column_id), NULL), ',') WITHIN GROUP (ORDER BY ic.key_ordinal)+','
            ,  InclCols      = STRING_AGG(IIF(ic.is_included_column = 1, CONCAT(IIF(cl.IsClusteredKeyColumn = 1, 'c', ''), ic.column_id), NULL), ',') WITHIN GROUP (ORDER BY ic.key_ordinal)+','
            /*  Ignore include columns that are also part of the clustering key because on clustered indexes, those columns are automatically included in every nonclustered index */
            ,  InclColsNoCLK = STRING_AGG(IIF(ic.is_included_column = 1 AND cl.IsClusteredKeyColumn = 0                  , ic.column_id , NULL), ',') WITHIN GROUP (ORDER BY ic.key_ordinal)+','
            /*  Only supports tables with up to 189 columns....after that you're gonna have to edit the query yourself, just follow the pattern.
                Magic number '63' - bigint is 8 bytes (64 bits). The 64th bit is used for signing (+/-), so the highest bit we can use is the 63rd bit position.
                Also, someone please explain to me why bitwise operators don't support binary/varbinary on both sides? */
            ,  InclBitmap1   = CONVERT(bigint, SUM(IIF(ic.is_included_column = 1 AND cl.IsClusteredKeyColumn = 0 AND (ic.column_id > 63*0 AND ic.column_id <= 63*1), POWER(@2, ic.column_id-1 - 63*0), 0))) --   0 < column_id <=  63
            ,  InclBitmap2   = CONVERT(bigint, SUM(IIF(ic.is_included_column = 1 AND cl.IsClusteredKeyColumn = 0 AND (ic.column_id > 63*1 AND ic.column_id <= 63*2), POWER(@2, ic.column_id-1 - 63*1), 0))) --  63 < column_id <= 126
            ,  InclBitmap3   = CONVERT(bigint, SUM(IIF(ic.is_included_column = 1 AND cl.IsClusteredKeyColumn = 0 AND (ic.column_id > 63*2 AND ic.column_id <= 63*3), POWER(@2, ic.column_id-1 - 63*2), 0))) -- 126 < column_id <= 189
        FROM sys.index_columns ic
            CROSS APPLY (
                SELECT IsClusteredKeyColumn = CONVERT(bit, COUNT(*))
                FROM sys.indexes si
                    JOIN sys.index_columns sic ON sic.[object_id] = si.[object_id] AND sic.index_id = si.index_id
                WHERE si.[type] = 1 -- Clustered
                    AND si.[object_id] = ic.[object_id]
                    AND sic.column_id = ic.column_id
                    AND ic.is_included_column = 1
            ) cl
        WHERE ic.[object_id] = i.[object_id]
            AND ic.index_id = i.index_id
    ) x
    JOIN (
        SELECT ps.[object_id], ps.index_id
            , used_kb       = SUM(ps.used_page_count)     * 8 -- KB
            , reserved_kb   = SUM(ps.reserved_page_count) * 8 -- KB
        FROM sys.dm_db_partition_stats ps
        GROUP BY ps.[object_id], ps.index_id
    ) ixs ON ixs.[object_id] = i.[object_id] AND ixs.index_id = i.index_id
    /*  SQL Server is really awesome in that it doesn't do any sort of re-ordering of the filter definition when it's created.
        It does reformat the definition, but I'm not sure how it determines the order (original? different?).
        That said...Filters can only use 'AND' statements, it cannot use 'OR'. So that means we can split, re-order and concat
        the filters to create a more reliable string for matching.
    */
    OUTER APPLY (
        SELECT NewFilterDef = CONCAT(N'(', STRING_AGG(x.[value], N' AND ') WITHIN GROUP (ORDER BY x.[value]), N')')
        FROM STRING_SPLIT(REPLACE(SUBSTRING(i.filter_definition, 2, LEN(i.filter_definition) - 2), N' AND ', NCHAR(9999)), NCHAR(9999)) x
        WHERE i.filter_definition IS NOT NULL
        GROUP BY ()
    ) fd
WHERE i.[type] = 2 -- non-clustered only
    AND o.is_ms_shipped = 0 -- exclude system objects
    AND i.is_primary_key = 0 -- exclude primary keys
    AND i.is_disabled = 0 -- exclude disabled
    AND i.is_hypothetical = 0
------------------------------------------------------------------------------

------------------------------------------------------------------------------
DROP TABLE IF EXISTS #matches; --SELECT * FROM #matches
SELECT x.MatchType, x.MatchRank, x.SourceID, x.MatchID, x.ExtraColCount
INTO #matches
FROM (
    -- Duplicate - exact duplicates
    SELECT MatchRank = 1, MatchType = 'Duplicate'
        , SourceID = x.ID, MatchID = y.ID, ExtraColCount = 0
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID
            FROM #tmp_idx i
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.is_unique = x.is_unique AND i.has_filter = x.has_filter AND i.filter_definition = x.filter_definition -- Match on index type (unique, filtered, etc)
                --
                AND x.KeyCols = i.KeyCols           -- Keys are exact match
                AND i.InclBitmap1 = x.InclBitmap1   -- Includes are exact match
                AND i.InclBitmap2 = x.InclBitmap2
                AND i.InclBitmap3 = x.InclBitmap3
        ) y
    UNION ALL
    -- Overlapping / Covered
    SELECT MatchRank = 2, MatchType = 'Overlapping'
        , SourceID = x.ID, MatchID = y.ID, ExtraColCount = 0
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID
            FROM #tmp_idx i
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.is_unique = x.is_unique AND i.has_filter = x.has_filter AND i.filter_definition = x.filter_definition -- Match on index type (unique, filtered, etc)
                --
                AND x.KeyCols LIKE i.KeyCols + '%' AND x.KeyCols <> i.KeyCols -- Keys are covering, but not duplicate
                AND i.InclBitmap1 & x.InclBitmap1 = i.InclBitmap1   -- Includes are covering, including duplicates
                AND i.InclBitmap2 & x.InclBitmap2 = i.InclBitmap2
                AND i.InclBitmap3 & x.InclBitmap3 = i.InclBitmap3
            --  AND CONCAT_WS('|', i.InclBitmap1, i.InclBitmap2, i.InclBitmap3) <> CONCAT_WS('|', x.InclBitmap1, x.InclBitmap2, x.InclBitmap3) -- Exclude duplicate includes
        ) y
    UNION ALL
    -- Mergable
    SELECT MatchRank = 3, MatchType = 'Mergable'
        , SourceID = x.ID, MatchID = y.ID, ExtraColCount = y.ExtraColCount
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID, c.ExtraColCount
            FROM #tmp_idx i
                CROSS APPLY (
                    SELECT ExtraColCount = sys.fn_numberOf1InVarBinary(~x.InclBitmap1 & i.InclBitmap1)
                                         + sys.fn_numberOf1InVarBinary(~x.InclBitmap2 & i.InclBitmap2)
                                         + sys.fn_numberOf1InVarBinary(~x.InclBitmap3 & i.InclBitmap3)
                ) c
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.is_unique = x.is_unique AND i.has_filter = x.has_filter AND i.filter_definition = x.filter_definition -- Match on index type (unique, filtered, etc)
                --
                AND x.KeyCols LIKE i.KeyCols + '%' -- Keys are covering, including duplicates
                AND (c.ExtraColCount >= 1 AND c.ExtraColCount <= @Mergable_ExtraColMax) -- Includes are off by small number
        ) y
) x;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
SELECT si.[object_id], si.SchemaName, si.ObjectName, si.ObjectType, si.is_unique, si.has_filter, si.filter_definition
    , N'█' [██], m.MatchRank, m.MatchType
    , GroupID   = DENSE_RANK() OVER (ORDER BY     si.SchemaName, si.ObjectName, mi.index_id)
    , DupID     = ROW_NUMBER() OVER (PARTITION BY si.SchemaName, si.ObjectName, mi.index_id ORDER BY m.MatchRank, m.ExtraColCount, si.index_id)
    , N'█' [██], si.IndexType, si.is_unique_constraint, si.index_id, si.IndexName, si.KeyCols, si.InclColsNoCLK
        , used_kb           = RIGHT(CONCAT(SPACE(16), FORMAT(si.used_kb, 'N0')),16) 
        , reserved_kb       = RIGHT(CONCAT(SPACE(16), FORMAT(si.reserved_kb, 'N0')),16) 
    , N'█' [██], mi.IndexType, mi.is_unique_constraint, mi.index_id, mi.IndexName, mi.KeyCols, mi.InclColsNoCLK
        , used_kb           = RIGHT(CONCAT(SPACE(16), FORMAT(mi.used_kb, 'N0')),16) 
        , reserved_kb       = RIGHT(CONCAT(SPACE(16), FORMAT(mi.reserved_kb, 'N0')),16)
        , ExtraColCount     = RIGHT(CONCAT(SPACE(16), m.ExtraColCount),16)
    , N'█' [██]
        , used_kb_diff      = RIGHT(CONCAT(SPACE(16), FORMAT(si.used_kb - mi.used_kb, 'N0')),16) 
        , reserved_kb_diff  = RIGHT(CONCAT(SPACE(16), FORMAT(si.reserved_kb - mi.reserved_kb, 'N0')),16)
FROM #matches m
    JOIN #tmp_idx si ON si.ID = m.SourceID
    JOIN #tmp_idx mi ON mi.ID = m.MatchID
WHERE m.MatchRank = 1
ORDER BY  si.SchemaName, si.ObjectName, mi.index_id
        , m.MatchRank, m.ExtraColCount, si.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
