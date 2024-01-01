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
    - Mergeable  - indexes are nearly covering - key cols are covered, includes are nearly covered - comparison index has small number of extra includes
        - e.g. IndexA: (A,-B,C) Incl (D,F,E)   ---   IndexB: (A,-B) Incl (F,D,G) -- IndexB has extra include column 'G'
*/

/*  Considerations:
  * Improve ability to compare the entire index definition. Currently, only keys and includes are compared.
    However, things like...padding, fillfactor and other settings are ignored.

  * "Actual index structure" comparison ability? Currently, the best this script is able to do is remove key columns from includes.
    The reality is much more complicated. Under the hood, depending on if the table is a heap or clustered and unique/non-unique as
    well as the comparison non-clustered index (unique/non-unique), the actual index structure will be different from the defined
    index structure.

    Some sort of feature which correctly pieces to gether the actual underlying index structure and then uses that for the comparison
    would be better than the current method. For example...on non-unique clustered indexes, all key columns are added to the index key
    list. If any of those keys are part of the included columns, then they are promoted to keys.

    For exmaple:
    IndexA is a     unique,     clustered index with KEY (A, B, C, F)
    IndexB is a non-unique, non-clustered index with KEY (B, C)       INCLUDE (D, E, A)

    The actual index structure would be created as:  KEY (B, C, A, F) INCLUDE (D, E)       -- A is promoted to key, F is added as a key
    If IndexB were instead unique, it would be       KEY (B, C)       INCLUDE (D, E, A, F) -- A stays the same    , F is added as a key

    Due to this new found knowledge, it seem that the "overlapping" and "mergeable" detection types may be faulty.

    For example:
    CX    : (A, B)
    IndexA: (A, C)    INCLUDE (B, E, F)
    IndexB: (A)       INCLUDE (B, F)

    IndexB will be marked as covered by IndexA. However, the underlying physical index structures are not necessarily the same. If the
    clustered index on the table/view is a unique, clustered index, then the physical index structures would be:

    IndexA: (A, C, B) INCLUDE (E, F) -- B is promoted to a key
    IndexB: (A, B)    INCLUDE (F)    -- B is promoted to a key

    Which means if you drop IndexB by assuming it is duplicated (covered) by IndexA, you may cause a performance degredation since the
    actual underlying physical structure of the index is not actually covered.
*/
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Generate index column list that represents the physical index structure
------------------------------------------------------------------------------
/*  Because the goal of this script is to find duplicate/overlapping indexes, we do not need
    to worry about adding hidden columns for things like RID's for heaps and UNIQUIFIER's for
    non-unique indexes.
*/
IF OBJECT_ID('tempdb..#tmp_idx_cols','U') IS NOT NULL DROP TABLE #tmp_idx_cols; --SELECT * FROM #tmp_idx_cols
CREATE TABLE #tmp_idx_cols (
    [object_id]         int           NOT NULL,
    index_id            int           NOT NULL,
    column_id           int           NOT NULL,
    column_name         nvarchar(128) NOT NULL,
    key_ordinal         int           NOT NULL,
    is_descending_key   bit               NULL,
    is_included_column  bit               NULL,
    is_secret_column    bit               NULL DEFAULT (0),
    rn                  int           NOT NULL,
);

INSERT INTO #tmp_idx_cols ([object_id], index_id, column_id, column_name, key_ordinal, is_descending_key, is_included_column, is_secret_column, rn)
SELECT x.[object_id], x.index_id, x.column_id, c.[name], x.key_ordinal, x.is_descending_key, x.is_included_column, x.is_secret_column
    /*
        We order by is_included_column first so that we capture the "promoted" columns. Columns that are part of the include
        column list in the defintion, but in the phsycial structure they are promoted to keys. Since includes don't have
        a sort direction, we can just inherit what is in the clustered index.

        Then we order by key_ordinal. This way we grab the existing index_columns record, rather than our new one. This is
        necessary because we are not looking at is_descending_key. So whatever the definition has needs to take precedence.

        What we end up with are missing clustered index columns appended as keys. And clustered index columns as includes
        promoting to keys for non-unique indexes and for unique indexes they are only added as include columns.
    */
    , rn = ROW_NUMBER() OVER (PARTITION BY x.[object_id], x.index_id, x.column_id ORDER BY x.is_included_column, x.key_ordinal)
FROM (
    SELECT ic.[object_id], ic.index_id, ic.column_id
            , ic.key_ordinal, ic.is_descending_key, ic.is_included_column
            , is_secret_column = 0
    FROM sys.index_columns ic
    UNION
    SELECT i.[object_id], i.index_id, ic.column_id
        , key_ordinal = IIF(i.is_unique = 0, ic.key_ordinal + 1000000, 0) /* Just a hack to ensure the secret columns are always sorted to the end while also maintaining their clustered index ordinal position  */
        , ic.is_descending_key
        , is_included_column = i.is_unique /* This just happens to line up, if a non-clustered index is unique, then missing clustered index columns are added as includes instead */
        , is_secret_column = 1
    FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.index_id = 1
    WHERE i.[type] = 2
) x
    JOIN sys.columns c ON c.[object_id] = x.[object_id] AND c.column_id = x.column_id
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#idx_col_cnt','U') IS NOT NULL DROP TABLE #idx_col_cnt; --SELECT * FROM #idx_col_cnt
SELECT i.[object_id], i.index_id, index_type = i.[type], column_count = COUNT(*)
INTO #idx_col_cnt
FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
GROUP BY i.[object_id], i.index_id, i.[type]
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#obj_col_cnt','U') IS NOT NULL DROP TABLE #obj_col_cnt; --SELECT * FROM #obj_col_cnt
SELECT c.[object_id], column_count = COUNT(*)
INTO #obj_col_cnt
FROM sys.columns c
GROUP BY c.[object_id]
------------------------------------------------------------------------------

------------------------------------------------------------------------------
DECLARE @2 bigint  = 2;

IF OBJECT_ID('tempdb..#idx_collapse','U') IS NOT NULL DROP TABLE #idx_collapse; --SELECT * FROM #idx_collapse ORDER BY [object_id], index_id
SELECT ic.[object_id], ic.index_id
    ,  OrigKeyColIDs  = STRING_AGG(IIF(ic.is_descending_key = 1, '-', '') + id.OrigKeyColID     , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
    ,  OrigInclColIDs = STRING_AGG(id.OrigInclColID                                             , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
    ,  PhysKeyColIDs  = STRING_AGG(IIF(ic.is_descending_key = 1, '-', '') + id.PhysKeyColID     , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
    ,  PhysInclColIDs = STRING_AGG(id.PhysInclColID                                             , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
    --
    ,  OrigKeyCols    = STRING_AGG(n.OrigKeyColName + IIF(ic.is_descending_key = 1, ' DESC', ''), ', ' ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
    ,  OrigInclCols   = STRING_AGG(n.OrigInclColName                                            , ', ' ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
    ,  PhysKeyCols    = STRING_AGG(n.PhysKeyColName + IIF(ic.is_descending_key = 1, ' DESC', ''), ', ' ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
    ,  PhysInclCols   = STRING_AGG(n.PhysInclColName                                            , ', ' ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
    /*  Only supports tables with up to 189 columns....after that you're gonna have to edit the query yourself, just follow the pattern.
        Magic number '63' - bigint is 8 bytes (64 bits). The 64th bit is used for signing (+/-), so the highest bit we can use is the 63rd bit position.
        Also, someone please explain to me why bitwise operators don't support binary/varbinary on both sides? */
    ,  InclBitmap1    = CONVERT(bigint, SUM(IIF(id.PhysInclColID > 63*0 AND id.PhysInclColID <= 63*1, POWER(@2, id.PhysInclColID-1 - 63*0), 0))) --   0 < column_id <=  63
    ,  InclBitmap2    = CONVERT(bigint, SUM(IIF(id.PhysInclColID > 63*1 AND id.PhysInclColID <= 63*2, POWER(@2, id.PhysInclColID-1 - 63*1), 0))) --  63 < column_id <= 126
    ,  InclBitmap3    = CONVERT(bigint, SUM(IIF(id.PhysInclColID > 63*2 AND id.PhysInclColID <= 63*3, POWER(@2, id.PhysInclColID-1 - 63*2), 0))) -- 126 < column_id <= 189
INTO #idx_collapse
FROM #tmp_idx_cols ic
    JOIN sys.columns c ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
    CROSS APPLY (
        SELECT OrigKeyColID    = IIF(ic.is_included_column = 0 AND ic.is_secret_column = 0, ic.column_id, NULL)
            ,  OrigInclColID   = IIF(ic.is_included_column = 1 AND ic.is_secret_column = 0, ic.column_id, NULL)
            ,  PhysKeyColID    = IIF(ic.is_included_column = 0 AND ic.rn = 1              , ic.column_id, NULL)
            ,  PhysInclColID   = IIF(ic.is_included_column = 1 AND ic.rn = 1              , ic.column_id, NULL)
    ) id
    CROSS APPLY (
        SELECT OrigKeyColName  = IIF(id.OrigKeyColID  IS NOT NULL, ic.column_Name, NULL)
            ,  OrigInclColName = IIF(id.OrigInclColID IS NOT NULL, ic.column_Name, NULL)
            ,  PhysKeyColName  = IIF(id.PhysKeyColID  IS NOT NULL, ic.column_Name, NULL)
            ,  PhysInclColName = IIF(id.PhysInclColID IS NOT NULL, ic.column_Name, NULL)
    ) n
GROUP BY ic.[object_id], ic.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#tmp_idx','U') IS NOT NULL DROP TABLE #tmp_idx; --SELECT * FROM #tmp_idx
SELECT ID = IDENTITY(int), i.[object_id], i.index_id
    , SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]
    , FQIN = CONCAT_WS('.', QUOTENAME(SCHEMA_NAME(o.[schema_id])), QUOTENAME(o.[name]), QUOTENAME(i.[name]))
    , ObjectType = o.[type_desc], IndexType = i.[type_desc], filter_definition = COALESCE(i.filter_definition, '')
    , i.is_unique
    , ic.OrigKeyColIDs, OrigInclColIDs = COALESCE(ic.OrigInclColIDs, '')
    , ic.PhysKeyColIDs, PhysInclColIDs = COALESCE(ic.PhysInclColIDs, '')
    , ic.PhysKeyCols, ic.PhysInclCols, ic.OrigKeyCols, ic.OrigInclCols
    , ic.InclBitmap1, ic.InclBitmap2, ic.InclBitmap3
    , ObjectRowCount = CONVERT(bigint, OBJECTPROPERTYEX(i.[object_id], 'Cardinality')), IndexRowCount = ixs.row_count, ixs.used_kb, ixs.reserved_kb
INTO #tmp_idx
FROM sys.indexes i
    JOIN sys.objects o ON o.[object_id] = i.[object_id]
    JOIN #idx_collapse ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    JOIN (
        SELECT ps.[object_id], ps.index_id
            , row_count     = SUM(ps.row_count)
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
WHERE o.is_ms_shipped = 0 -- exclude system objects
    AND i.is_disabled = 0 -- exclude disabled
    AND i.is_hypothetical = 0;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    UPDATE i SET i.filter_definition = fd.NewFilterDef
    FROM #tmp_idx i
        CROSS APPLY (
            SELECT NewFilterDef = CONCAT(N'(', STRING_AGG(x.[value], N' AND ') WITHIN GROUP (ORDER BY x.[value]), N')')
            FROM STRING_SPLIT(REPLACE(SUBSTRING(i.filter_definition, 2, LEN(i.filter_definition) - 2), N' AND ', NCHAR(9999)), NCHAR(9999)) x
            WHERE i.filter_definition IS NOT NULL
            GROUP BY ()
        ) fd
    WHERE LEN(i.filter_definition) > 0;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#matches','U') IS NOT NULL DROP TABLE #matches; --SELECT * FROM #matches
    CREATE TABLE #matches (
        ID              int         NOT NULL IDENTITY,
        MatchRank       int         NOT NULL,
        MatchType       varchar(50) NOT NULL,
        SourceID        int         NOT NULL,
        MatchID         int         NOT NULL,
        ExtraColCount   int         NOT NULL DEFAULT(0),
    );
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    -- Duplicate - exact duplicates
    INSERT INTO #matches (MatchRank, MatchType, SourceID, MatchID)
    SELECT 1, 'Duplicate - Definitional', x.ID, y.ID
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID
            FROM #tmp_idx i
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.filter_definition = x.filter_definition -- If it's filtered, make sure they match
                -- Match logic
                AND i.OrigKeyColIDs = x.OrigKeyColIDs AND i.OrigInclColIDs = x.OrigInclColIDs
                -- Restrictions
                AND i.IndexType <> 'CLUSTERED' -- Keep clustered index matches on the left
                AND NOT (x.is_unique = 0 AND i.is_unique = 1) -- Not a valid duplicate - if the left is not-unique but the right is, then the right is not a droppable dupe
        ) y

    INSERT INTO #matches (MatchRank, MatchType, SourceID, MatchID)
    SELECT 2, 'Duplicate - Physical', x.ID, y.ID
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID
            FROM #tmp_idx i
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.filter_definition = x.filter_definition -- If it's filtered, make sure they match
                -- Match logic
                AND i.PhysKeyColIDs = x.PhysKeyColIDs AND i.PhysInclColIDs = x.PhysInclColIDs
                -- Restrictions
                AND i.IndexType <> 'CLUSTERED' -- Keep clustered index matches on the left
                AND NOT (x.is_unique = 0 AND i.is_unique = 1) -- Not a valid duplicate - if the left is not-unique but the right is, then the right is not a droppable dupe
        ) y
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    DECLARE @Mergeable_ExtraColMax  tinyint = 4;

    -- Overlapping / Covered
    INSERT INTO #matches (MatchRank, MatchType, SourceID, MatchID)
    SELECT 3, 'Overlapping', x.ID, y.ID
    FROM #tmp_idx x
        CROSS APPLY (
            SELECT i.ID
            FROM #tmp_idx i
            WHERE i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.filter_definition = x.filter_definition -- If it's filtered, make sure they match
                --
                AND x.PhysKeyColIDs LIKE i.PhysKeyColIDs + '%' AND x.PhysKeyColIDs <> i.PhysKeyColIDs -- Keys are covering, but not duplicate
                AND i.InclBitmap1 & x.InclBitmap1 = i.InclBitmap1   -- Includes are covering, including duplicates
                AND i.InclBitmap2 & x.InclBitmap2 = i.InclBitmap2
                AND i.InclBitmap3 & x.InclBitmap3 = i.InclBitmap3
            --  AND CONCAT_WS('|', i.InclBitmap1, i.InclBitmap2, i.InclBitmap3) <> CONCAT_WS('|', x.InclBitmap1, x.InclBitmap2, x.InclBitmap3) -- Exclude duplicate includes
        ) y
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    -- Mergeable
    INSERT INTO #matches (MatchRank, MatchType, SourceID, MatchID, ExtraColCount)
    SELECT 4, 'Mergeable', x.ID, y.ID, y.ExtraColCount
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
                AND i.filter_definition = x.filter_definition -- If it's filtered, make sure they match
                --
                AND x.PhysKeyColIDs LIKE i.PhysKeyColIDs + '%' -- Keys are covering, including duplicates
                AND (c.ExtraColCount >= 1 AND c.ExtraColCount <= @Mergeable_ExtraColMax) -- Includes are off by small number
        ) y
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    DELETE x
    FROM (
        SELECT rn = ROW_NUMBER() OVER (PARTITION BY x.DeDupID ORDER BY m.MatchRank, m.ExtraColCount, m.SourceID, m.MatchID)
        FROM #matches m
            CROSS APPLY (SELECT DeDupID = IIF(SourceID > MatchID, CONCAT(MatchID, '_', SourceID), CONCAT(SourceID, '_', MatchID))) x
    ) x
    WHERE x.rn > 1
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    SELECT x.MatchType, x.MatchRank, x.ExtraColCount
        , t.SchemaName, t.ObjectName, t.IndexName--, x.FQIN
        , t.ObjectType, t.IndexType, t.OrigKeyColIDs, t.OrigInclColIDs, t.PhysKeyColIDs, t.PhysInclColIDs, t.filter_definition
        , CurrentIndexColCount      = icc.column_count
        , ClusteredIndexColCount    = ixc.column_count
        , ObjectColCount            = occ.column_count
        , t.used_kb, t.reserved_kb, t.ObjectRowCount, t.IndexRowCount
        , i.is_unique, i.data_space_id, i.[ignore_dup_key], i.is_primary_key, i.is_unique_constraint, i.fill_factor, i.is_padded, i.is_disabled, i.[allow_page_locks], i.has_filter
    FROM (
        SELECT ID = SourceID, MatchType, MatchRank, ExtraColCount FROM #matches
        UNION
        SELECT ID = MatchID , MatchType, MatchRank, ExtraColCount FROM #matches
    ) x
        JOIN #tmp_idx t ON t.ID = x.ID
        JOIN sys.indexes i ON i.[object_id] = t.[object_id] AND i.index_id = t.index_id
        JOIN #idx_col_cnt icc ON icc.[object_id] = t.[object_id] AND icc.index_id = t.index_id
        LEFT JOIN #idx_col_cnt ixc ON ixc.[object_id] = t.[object_id] AND ixc.index_type = 1
        JOIN #obj_col_cnt occ ON occ.[object_id] = t.[object_id]
    WHERE x.MatchRank IN (1,2)
    ORDER BY t.SchemaName, t.ObjectName, x.MatchType;
------------------------------------------------------------------------------

------------------------------------------------------------------------------