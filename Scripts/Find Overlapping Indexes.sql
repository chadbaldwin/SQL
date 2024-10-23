------------------------------------------------------------------------------
-- Notes / Documentation
------------------------------------------------------------------------------
/*
  All comparisons use the physical index structure and ignore include column ordinal positions.

  ## Match types

  - Duplicate - entire index is duplicated - key cols, key ordinal position, key sort direction, include column set
    - e.g. IndexA: (A,-B,C) Incl (D,F,E)
           IndexB: (A,-B,C) Incl (F,E,D)

  - Covered - keys in parent index contains all keys (ordinal & sort direction) of the child, and contains entire include column set
    - e.g. IndexA: (A,-B,C) Incl (D,F,E)
           IndexB: (A,-B)   Incl (F,D)

  - Mergeable - indexes are nearly covering - key cols are covered, includes are nearly covered - comparison index has small number of extra include columns
    - e.g. IndexA: (A,-B,C) Incl (D,F,E)
           IndexB: (A,-B)   Incl (F,D,G) -- IndexB has extra include column 'G'

  ## Physical index structure

  This script deduces the index's physical structure and uses that for all comparisons rather than the defined index structure.
  AKA, it tries to use what SQL Server actually creates and stores, rather than what is simply written in the CREATE INDEX script.

  There are some pieces left out here regarding RID's on heaps and UNIQUIFIER's on non-unique clustered indexes
  because this script does not need to worry about them.

  For exmaple:
  IndexA is a     unique,     clustered index with:   KEY (A, B, C, F)
  IndexB is a non-unique, non-clustered index with:   KEY (B, C)       INCLUDE (D, E, A)

  The physical structure for IndexB would be:         KEY (B, C, A, F) INCLUDE (D, E)       -- A is promoted to key, F is added as a key
  If IndexB were instead a unique index, it would be: KEY (B, C)       INCLUDE (D, E, A, F) -- The key stays the same, but A and F are added as includes

  Using the physical structure instead of the defined structure helps detect more duplicate and covered indexes and prevents false positives.

  For example, a table has the following indexes defined:
  Unique Clustered: (A, B)
  IndexA:           (B, C)    INCLUDE (A, E, F)
  IndexB:           (B)       INCLUDE (A, F)

  IndexB appears to be covered by IndexA. However, the underlying physical index structures are not actually the same.
  In reality, the physical index structures would look more like this:

  IndexA: (B, C, A) INCLUDE (E, F) -- A is promoted to a key
  IndexB: (B, A)    INCLUDE (F)    -- A is promoted to a key

  Even though their defined structure appears to be covered, their physical structure is not.
*/

/*  Considerations / TODO:
  - Improve ability to compare the entire index definition. Currently, only keys and includes are compared.
    However, things like...padding, fillfactor and other settings are ignored.
  - Add list of column differences for mergeable indexes. For now it's just a list of column ID's which isnt very helpful.
  - Should clustered vs nonclustered and/or unique vs non-unique duplicate matches indexes be classified as "duplicate"
    or overlapping? (currently classified as overlapping)
  - Add support for partitioned indexes and columnstore indexes
*/
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    /*  Create a list of indexes that qualify for analysis */

    IF OBJECT_ID('tempdb..#target_indexes','U') IS NOT NULL DROP TABLE #target_indexes; --SELECT * FROM #target_indexes;
    SELECT IndexID = IDENTITY(int), i.[object_id], i.index_id
         , n.SchemaName, n.ObjectName, n.IndexName, x.FQIN
         , ObjectType  = o.[type_desc]
         , IndexType   = i.[type_desc]
         , IndexTypeID = i.[type]
         , i.is_unique, i.data_space_id, i.[ignore_dup_key], i.is_primary_key, i.is_unique_constraint, i.fill_factor, i.is_padded
         , i.[allow_row_locks], i.[allow_page_locks], i.has_filter, i.filter_definition
         , i.auto_created
    INTO #target_indexes
    FROM sys.indexes i
        JOIN sys.objects o ON o.[object_id] = i.[object_id]
        CROSS APPLY (SELECT SchemaName = SCHEMA_NAME(o.[schema_id]), ObjectName = o.[name], IndexName = i.[name]) n
        CROSS APPLY (SELECT FQIN = CONCAT_WS('.', QUOTENAME(n.SchemaName), QUOTENAME(n.ObjectName), QUOTENAME(n.IndexName))) x
    WHERE o.is_ms_shipped = 0
        AND i.is_disabled = 0
        AND i.is_hypothetical = 0
        AND i.[type] IN (1,2); -- This script only supports simple rowstore clustered and non-clustered indexes
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    /*  Re-arrange filtered index definitions for easier comparison */

    /*  SQL Server is really awesome (sarcasm) in that it doesn't do any sort of re-ordering of the filter definition when
        it's created. It does reformat the definition adding parens and brackets, but I'm not sure how it determines the
        order of the predicates (original? different?). That said...Filters can only use 'AND' statements, they cannot use
        'OR'. So that means we can split, re-order and concat the filters to create a more reliable string for matching.

        For example, we have two filtered indexes, with their filter definitions defined as:
        IndexA: WHERE ([ColA] = 1 AND [ColB] > 2)
        IndexB: WHERE ([ColB] > 2 AND [ColA] = 1)

        This will re-arrange the predicates so that both will become:
        WHERE ([ColA] = 1 AND [ColB] > 2)
    */
    UPDATE i SET i.filter_definition = fd.NewFilterDef
    --SELECT *
    FROM #target_indexes i
        CROSS APPLY (
            /* Split by " AND ", sort it alphabetically, then cram it back together with " AND " */
            SELECT NewFilterDef = CONCAT(N'(', STRING_AGG(x.[value], N' AND ') WITHIN GROUP (ORDER BY x.[value]), N')')
            FROM STRING_SPLIT(REPLACE(SUBSTRING(i.filter_definition, 2, LEN(i.filter_definition) - 2), N' AND ', NCHAR(9999)), NCHAR(9999)) x
            WHERE i.filter_definition IS NOT NULL
            GROUP BY ()
        ) fd
    WHERE i.has_filter = 1
        AND i.filter_definition <> fd.NewFilterDef;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#idx_cols','U') IS NOT NULL DROP TABLE #idx_cols; --SELECT * FROM #idx_cols ORDER BY object_id, index_id, is_included_column, key_ordinal, column_id
    CREATE TABLE #idx_cols (
        IndexID             int            NOT NULL,
        [object_id]         int            NOT NULL,
        index_id            int            NOT NULL,
        column_id           int            NOT NULL,
        column_name         nvarchar(128)  NOT NULL,
        key_ordinal         int            NOT NULL,
        is_descending_key   bit            NOT NULL,
        is_included_column  bit            NOT NULL,
        is_secret_column    bit            NOT NULL,
        is_clustering_key   bit            NOT NULL,
        data_type           nvarchar(128)      NULL,
        max_length          smallint           NULL,
        [precision]         tinyint            NULL,
        scale               tinyint            NULL,
        data_type_text      nvarchar(200)  NOT NULL,
        rn                  int            NOT NULL DEFAULT(0),
    );

    INSERT INTO #idx_cols (IndexID, [object_id], index_id, column_id, column_name, key_ordinal, is_descending_key, is_included_column, is_secret_column, is_clustering_key, data_type, max_length, [precision], scale, data_type_text)
    SELECT x.IndexID, x.[object_id], x.index_id, x.column_id
        , COL_NAME(x.[object_id], x.column_id)
        , x.key_ordinal, x.is_descending_key, x.is_included_column, x.is_secret_column, x.is_clustering_key
        , TYPE_NAME(c.system_type_id), c.max_length, c.[precision], c.scale
        , dt.TypeName
    FROM (
        /* Default records */
        SELECT i.IndexID, i.[object_id], i.index_id, ic.column_id, ic.key_ordinal, ic.is_descending_key, ic.is_included_column, is_secret_column = 0, is_clustering_key = IIF(i.IndexTypeID = 1, 1, 0)
    FROM #target_indexes i
            JOIN sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
        UNION ALL
    /* Add missing clustering keys - can be either keys or includes depending on is_unique status */
        SELECT i.IndexID, i.[object_id], i.index_id, ic.column_id
        , key_ordinal = IIF(i.is_unique = 0, ic.key_ordinal + 1000000, 0) /*  Just a hack to ensure the secret columns are always sorted to the end while also maintaining their clustered index ordinal position */
        , ic.is_descending_key
        , is_included_column = i.is_unique /*  This just happens to line up, if a non-clustered index is unique, then missing clustered index columns are added as includes instead */
        , is_secret_column = 1
        , is_clustering_key = 1
    FROM #target_indexes i
        JOIN sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.index_id = 1 -- clustered
        WHERE i.IndexType = 'NONCLUSTERED'
        UNION ALL
        /* Add missing clustered index "include" columns - This way we treat clustered indexes the same as non-clustered as having keys and includes */
        SELECT i.IndexID, i.[object_id], i.index_id, c.column_id, key_ordinal = 0, is_descending_key = 0, is_included_column = 1, is_secret_column = 0, is_clustering_key = 0
        FROM #target_indexes i
            JOIN sys.columns c ON c.[object_id] = i.[object_id]
        WHERE i.IndexType = 'CLUSTERED'
            AND NOT EXISTS (
                SELECT *
                FROM sys.index_columns ic
                WHERE ic.[object_id] = c.[object_id] AND ic.index_id = i.IndexID 
                    AND ic.column_id = c.column_id
            )
    ) x
        JOIN sys.columns c ON c.[object_id] = x.[object_id] AND c.column_id = x.column_id
        CROSS APPLY (
            SELECT TypeName = CONCAT(TYPE_NAME(c.system_type_id)
                    ,   CASE
                            WHEN TYPE_NAME(c.system_type_id) IN ('datetime2', 'time')   THEN IIF(c.scale = 7, NULL, CONCAT('(', c.scale, ')')) --scale of (7) is the default so it can be ignored, (0) is a valid value
                            WHEN TYPE_NAME(c.system_type_id) IN ('datetimeoffset')      THEN CONCAT('(', c.scale, ')')
                            WHEN TYPE_NAME(c.system_type_id) IN ('decimal', 'numeric')  THEN CONCAT('(', c.[precision], ',', c.scale,')')
                            WHEN TYPE_NAME(c.system_type_id) IN ('nchar', 'nvarchar')   THEN IIF(c.max_length = -1, '(MAX)', CONCAT('(', c.max_length/2, ')'))
                            WHEN TYPE_NAME(c.system_type_id) IN ('char', 'varchar')     THEN IIF(c.max_length = -1, '(MAX)', CONCAT('(', c.max_length, ')'))
                            WHEN TYPE_NAME(c.system_type_id) IN ('binary', 'varbinary') THEN IIF(c.max_length = -1, '(MAX)', CONCAT('(', c.max_length, ')'))
                            ELSE NULL
                        END)
        ) dt;

    UPDATE ic SET ic.is_clustering_key = 1
    FROM #idx_cols ic
        JOIN sys.index_columns sic ON sic.[object_id] = ic.[object_id] AND sic.column_id = ic.column_id AND sic.index_id = 1
    WHERE ic.is_clustering_key = 0;

    /*  We order by is_included_column first so that we capture the "promoted" columns. Columns that are part of the include
        column list in the defintion, but in the phsycial structure they are promoted to keys. Since includes don't have
        a sort direction, we can just inherit what is in the clustered index.

        Then we order by key_ordinal. This way we grab the existing index_columns record, rather than our new one. This is
        necessary because we are not looking at is_descending_key. So whatever the definition has needs to take precedence.

        What we end up with are missing clustered index columns appended as keys. And clustered index columns as includes
        promoting to keys for non-unique indexes and for unique indexes they are only added as include columns.
    */
    UPDATE c
    SET c.rn = c.new_rn
    FROM (
        SELECT c.rn, new_rn = ROW_NUMBER() OVER (PARTITION BY c.[object_id], c.index_id, c.column_id ORDER BY c.is_included_column, c.key_ordinal)
        FROM #idx_cols c
    ) c
    WHERE c.rn = 0;

    /* -- How to read the data in #idx_cols:
        SELECT * FROM #idx_cols WHERE is_secret_column = 0 -- Definitional index structure
        SELECT * FROM #idx_cols WHERE rn = 1               -- Physical index structure
    */
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    /*  Get column counts for indexes and objects */

    IF OBJECT_ID('tempdb..#idx_col_cnt','U') IS NOT NULL DROP TABLE #idx_col_cnt; --SELECT * FROM #idx_col_cnt
    SELECT i.[object_id], i.index_id, i.IndexType, ColumnCount = COUNT(*)
    INTO #idx_col_cnt
    FROM #target_indexes i
        JOIN #idx_cols ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
    WHERE ic.rn = 1
    GROUP BY i.[object_id], i.index_id, i.IndexType;

    IF OBJECT_ID('tempdb..#obj_col_cnt','U') IS NOT NULL DROP TABLE #obj_col_cnt; --SELECT * FROM #obj_col_cnt
    SELECT c.[object_id], ColumnCount = COUNT(*)
    INTO #obj_col_cnt
    FROM sys.columns c
    GROUP BY c.[object_id];
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    /*  Collapse all the columns down into lists and bitmaps */

    /*  Needs to be `bigint` for reasons...
        decimal(38,0) is implicitly converted to float when getting the value of POWER(@2, N)
        With float, you lose precision...So 4611686018427387904 (2^62) becomes 4611686018427387900
        because it wants to treat it as 4.61168601842739E+18.

        However, if you use bigint...then it always treats it as a bigint and no precision is lost.
    */
    DECLARE @2 bigint  = 2;

    IF OBJECT_ID('tempdb..#idx_collapse','U') IS NOT NULL DROP TABLE #idx_collapse; --SELECT * FROM #idx_collapse ORDER BY [object_id], index_id
    WITH cte_all_cols AS (
        --DECLARE @2 bigint  = 2;
        SELECT ic.[object_id], ic.index_id
            /*  All column IDs */
            ,  AllColIDs      = STRING_AGG(CONCAT(IIF(ic.is_clustering_key = 1, 'c', NULL), ic.column_id), ',') WITHIN GROUP (ORDER BY ic.column_id)+','
            /*  Only supports tables with up to 189 columns....after that you're gonna have to edit the query yourself, just follow the pattern.
                Magic number '63' - bigint is 8 bytes (64 bits). The 64th bit is used for signing (+/-), so the highest bit we can use is the 63rd bit position.
                Also, someone please explain to me why bitwise operators don't support binary/varbinary on both sides? */
            ,  AllColBitmap1  = CONVERT(bigint, SUM(IIF(ic.column_id > 63*0 AND ic.column_id <= 63*1, POWER(@2, ic.column_id-1 - 63*0), 0))) --   0 < column_id <=  63
            ,  AllColBitmap2  = CONVERT(bigint, SUM(IIF(ic.column_id > 63*1 AND ic.column_id <= 63*2, POWER(@2, ic.column_id-1 - 63*1), 0))) --  63 < column_id <= 126
            ,  AllColBitmap3  = CONVERT(bigint, SUM(IIF(ic.column_id > 63*2 AND ic.column_id <= 63*3, POWER(@2, ic.column_id-1 - 63*2), 0))) -- 126 < column_id <= 189
        FROM (SELECT DISTINCT [object_id], index_id, column_id, column_name, is_clustering_key FROM #idx_cols) ic
        GROUP BY ic.[object_id], ic.index_id
    ), cte_clst_keys AS (
        SELECT ti.[object_id]
            /*  Clustering keys */
            ,  ClstKeyColIDs  = STRING_AGG(IIF(ic.is_descending_key = 1, '-', '') + id.ClstKeyColID     , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
            ,  ClstKeyCols    = STRING_AGG(CONVERT(nvarchar(MAX), n.ClstKeyColName + IIF(ic.is_descending_key = 1, ' DESC', '')), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
        FROM #target_indexes ti
            JOIN #idx_cols ic ON ic.[object_id] = ti.[object_id] AND ic.index_id = ti.index_id
            CROSS APPLY (SELECT column_id = CONCAT(IIF(ic.is_clustering_key = 1, 'c', NULL), ic.column_id)) ci
            CROSS APPLY (SELECT ClstKeyColID = IIF(ic.is_clustering_key  = 1 AND ic.rn = 1, ci.column_id, NULL)) id
            CROSS APPLY (SELECT ClstKeyColName = IIF(id.ClstKeyColID  IS NOT NULL, ic.column_Name, NULL)) n
        WHERE ti.IndexType = 'CLUSTERED'
        GROUP BY ti.[object_id]
    ), cte_idx_cols AS (
        SELECT ic.[object_id], ic.index_id
            /*  Defined index structure */
            ,  OrigKeyColIDs  = STRING_AGG(IIF(ic.is_descending_key = 1, '-', '') + id.OrigKeyColID     , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
            ,  OrigInclColIDs = STRING_AGG(id.OrigInclColID                                             , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
            ,  OrigKeyCols    = STRING_AGG(CONVERT(nvarchar(MAX), n.OrigKeyColName + IIF(ic.is_descending_key = 1, ' DESC', '')), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
            ,  OrigInclCols   = STRING_AGG(CONVERT(nvarchar(MAX), n.OrigInclColName + ' ' + ic.data_type_text                  ), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
            /*  Physical index structure */
            ,  PhysKeyColIDs  = STRING_AGG(IIF(ic.is_descending_key = 1, '-', '') + id.PhysKeyColID     , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
            ,  PhysInclColIDs = STRING_AGG(id.PhysInclColID                                             , ','  ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)+','
            ,  PhysKeyCols    = STRING_AGG(CONVERT(nvarchar(MAX), n.PhysKeyColName + IIF(ic.is_descending_key = 1, ' DESC', '')), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
            ,  PhysInclCols   = STRING_AGG(CONVERT(nvarchar(MAX), n.PhysInclColName + ' ' + ic.data_type_text                  ), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.column_id)
        --INTO #idx_collapse
        FROM #idx_cols ic
            CROSS APPLY (SELECT column_id = CONCAT(IIF(ic.is_clustering_key = 1, 'c', NULL), ic.column_id)) ci
            CROSS APPLY (
                SELECT OrigKeyColID    = IIF(ic.is_included_column = 0 AND ic.is_secret_column = 0, ci.column_id, NULL)
                    ,  OrigInclColID   = IIF(ic.is_included_column = 1 AND ic.is_secret_column = 0, ci.column_id, NULL)
                    ,  PhysKeyColID    = IIF(ic.is_included_column = 0 AND ic.rn = 1              , ci.column_id, NULL)
                    ,  PhysInclColID   = IIF(ic.is_included_column = 1 AND ic.rn = 1              , ci.column_id, NULL)
            ) id
            CROSS APPLY (
                SELECT OrigKeyColName  = IIF(id.OrigKeyColID  IS NOT NULL, ic.column_Name, NULL)
                    ,  OrigInclColName = IIF(id.OrigInclColID IS NOT NULL, ic.column_Name, NULL)
                    ,  PhysKeyColName  = IIF(id.PhysKeyColID  IS NOT NULL, ic.column_Name, NULL)
                    ,  PhysInclColName = IIF(id.PhysInclColID IS NOT NULL, ic.column_Name, NULL)
            ) n
        GROUP BY ic.[object_id], ic.index_id
    )
    SELECT ti.[object_id], ti.index_id
        , ac.AllColIDs, ac.AllColBitmap1, ac.AllColBitmap2, ac.AllColBitmap3
        , ic.PhysKeyColIDs, ic.PhysInclColIDs, ic.OrigKeyColIDs, ic.OrigInclColIDs, ck.ClstKeyColIDs
        , ic.PhysKeyCols, ic.PhysInclCols, ic.OrigKeyCols, ic.OrigInclCols, ck.ClstKeyCols
    INTO #idx_collapse
    FROM #target_indexes ti
        JOIN cte_all_cols ac ON ac.[object_id] = ti.[object_id] AND ac.index_id = ti.index_id
        JOIN cte_idx_cols ic ON ic.[object_id] = ti.[object_id] AND ic.index_id = ti.index_id
        LEFT JOIN cte_clst_keys ck ON ck.[object_id] = ti.[object_id];
------------------------------------------------------------------------------

------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#idx','U') IS NOT NULL DROP TABLE #idx; --SELECT * FROM #idx WHERE PhysKeyColIDs LIKE '%-%'
    SELECT i.IndexID, i.[object_id], i.index_id
        , i.SchemaName, i.ObjectName, i.IndexName, i.ObjectType, i.IndexType, i.IndexTypeID, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.has_filter, i.filter_definition
        , ic.PhysKeyColIDs, PhysInclColIDs = COALESCE(ic.PhysInclColIDs, '')
        , ic.OrigKeyColIDs, OrigInclColIDs = COALESCE(ic.OrigInclColIDs, '')
        , ic.ClstKeyColIDs, ic.AllColIDs
        , ic.PhysKeyCols, ic.PhysInclCols, ic.OrigKeyCols, ic.OrigInclCols, ic.ClstKeyCols
        , ic.AllColBitmap1, ic.AllColBitmap2, ic.AllColBitmap3
        , ObjectRowCount = CONVERT(bigint, OBJECTPROPERTYEX(i.[object_id], 'Cardinality'))
        , ixs.IndexRowCount, ixs.UsedKB, ixs.ReservedKB
        , CurrIdxColCount  = icc.ColumnCount
        , ClustIdxColCount = xcc.ColumnCount
        , ObjColCount      = occ.ColumnCount
        , i.FQIN
    INTO #idx
    FROM #target_indexes i
        JOIN #idx_collapse ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id
        JOIN #obj_col_cnt occ ON occ.[object_id] = i.[object_id]                               -- object col count
        JOIN #idx_col_cnt icc ON icc.[object_id] = i.[object_id] AND icc.index_id = i.index_id -- current index col count (LEFT JOIN due to heaps)
        LEFT JOIN #idx_col_cnt xcc ON xcc.[object_id] = i.[object_id] AND xcc.index_id = 1     -- clustered index col count
        JOIN (
            SELECT ps.[object_id], ps.index_id
                , IndexRowCount = SUM(ps.row_count)
                , UsedKB        = SUM(ps.used_page_count)     * 8 -- KB
                , ReservedKB    = SUM(ps.reserved_page_count) * 8 -- KB
            FROM sys.dm_db_partition_stats ps
            GROUP BY ps.[object_id], ps.index_id
        ) ixs ON ixs.[object_id] = i.[object_id] AND ixs.index_id = i.index_id;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Duplicates
------------------------------------------------------------------------------
    /*  Duplicates don't really have a parent/child type of match relationship. So rather than comparing them side by side
        as a pair of matches, we just insert them into a table to be compared veritcally with a grouping IndexID.

        The only time there's really a parent/child sort of relationship is when unique indexes are involved because you
        would likely prefer to drop the non-unique index when it duplicates a unique index.

        It is up to the user to look at the results of the query and decide what to do with the them.

        This match type is intended to be extremely strict in its matching criteria.
        The entire structure of the index must match in regard to key cols, key ordinal, key sort direction and include column list.
    */
    IF OBJECT_ID('tempdb..#idx_dupes','U') IS NOT NULL DROP TABLE #idx_dupes; --SELECT * FROM #idx_dupes
    SELECT x.DupeGroupID, x.DupeGroupCount, x.IndexID
    INTO #idx_dupes
    FROM (
        SELECT DupeGroupID    = MIN(x.IndexID) OVER (PARTITION BY x.SchemaName, x.ObjectName, x.filter_definition, x.PhysKeyColIDs, x.PhysInclColIDs)
            ,  DupeGroupCount = COUNT(*)  OVER (PARTITION BY x.SchemaName, x.ObjectName, x.filter_definition, x.PhysKeyColIDs, x.PhysInclColIDs)
            , x.IndexID
        FROM #idx x
        WHERE x.IndexType = 'NONCLUSTERED' -- Non-clustered indexes only. "Duplicates" involving clustered indexes will be classified as other match types (likely as covering indexes).
    ) x
    WHERE x.DupeGroupCount > 1;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Covered
------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#idx_cover','U') IS NOT NULL DROP TABLE #idx_cover; --SELECT * FROM #idx_cover
    SELECT MatchID = IDENTITY(int), MergeIntoID = x.IndexID, MergeFromID = y.IndexID
    INTO #idx_cover
    FROM #idx x
        CROSS APPLY (
            SELECT i.IndexID
            FROM #idx i
            WHERE 1=1
                /* Minimum match criteria */
                AND i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.has_filter = x.has_filter AND COALESCE(i.filter_definition, '') = COALESCE(x.filter_definition, '') -- If it's filtered, make sure they match
                AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 -- Exclude constraints as children
                /* Restrictions */
                AND i.IndexType <> 'CLUSTERED' /* Right side indexes are targets for dropping and we typically do not want to drop clustered indexes, so exclude them from the right side */
                /*  If the left index is unique (right index unique status doesn't matter), then any matching indexes must
                    have the same keys. Non-unique indexes can be merged into a unique index, but not the other way around.
                    If both indexes are non-unique, then the left keys must be covering the right index keys. */
                AND ((x.is_unique = 1 AND x.PhysKeyColIDs = i.PhysKeyColIDs) OR (x.is_unique = 0 AND i.is_unique = 0 AND x.PhysKeyColIDs LIKE i.PhysKeyColIDs + '%'))
                /* Mergeable criteria */
                AND ((      x.IndexType = 'NONCLUSTERED' -- This line isn't necessary, but it helps with reading the code
                        AND i.AllColBitmap1 & x.AllColBitmap1 = i.AllColBitmap1 -- \
                        AND i.AllColBitmap2 & x.AllColBitmap2 = i.AllColBitmap2 --  |-- Left index include column set contains all of the right index's include columns
                        AND i.AllColBitmap3 & x.AllColBitmap3 = i.AllColBitmap3 -- /
                        AND (x.PhysKeyColIDs <> i.PhysKeyColIDs OR x.AllColIDs <> i.AllColIDs) -- Keys or includes - at least one must be different
                    )
                    OR x.IndexType = 'CLUSTERED' -- If the left is a clustered index, then we only care if the keys are covered or match
                )
        ) y;

    /* Get full list of column texts for extra columns */
    IF OBJECT_ID('tempdb..#idx_cover_extra_cols','U') IS NOT NULL DROP TABLE #idx_cover_extra_cols; --SELECT * FROM #idx_cover_extra_cols
    WITH cte_cols AS (SELECT * FROM #idx_cols c WHERE rn = 1)
    SELECT ic.MatchID
        , ExtraCols = STRING_AGG(x.ColText, ', ') WITHIN GROUP (ORDER BY ci.is_included_column, ci.key_ordinal)
    INTO #idx_cover_extra_cols
    FROM #idx_cover ic
        JOIN cte_cols ci ON ci.IndexID = ic.MergeIntoID
        CROSS APPLY (
            SELECT ColText = CONCAT('{', ci.column_id, '}', QUOTENAME(ci.column_name), ' ', ci.data_type_text, IIF(ci.is_descending_key = 1, ' DESC', ''))
        ) x
    WHERE NOT EXISTS (
            SELECT *
            FROM cte_cols cf
            WHERE cf.IndexID = ic.MergeFromID
                AND cf.column_id = ci.column_id
        )
    GROUP BY ic.MatchID;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Mergeable
------------------------------------------------------------------------------
    /*  The difference between "covered" indexes and "mergeable" indexes is that *merging* two indexes results in a
        new index that is different from either of the original two in regard to their include columns. Whereas with
        covered indexes, the resulting action is to drop the covered index.
    */

    /*  Mergeable indexes are determined by checking the difference in extra columns.

        In order do do that, we use some bitwise tricks.
        Explanation:
        IndexA.AllColBitmap1 - 0100101001
        IndexB.AllColBitmap1 - 0110111001
        Differences:             *  *

        This shows that IndexB has two columns that IndexA does not. In order to figure that out with bitwise operators
        we can first negate the bitmap for IndexA...then we can use a bitwise AND comparison. This allows us to produce
        a new binary value consisting only of the "different" bits.

        So the comparison becomes:
        IndexA.AllColBitmap1 - 1011010110 (original value negated)
        IndexB.AllColBitmap1 - 0110111001
        Bitwise AND:           0010010000 (provides a new bitmap which indicates which column bits are set for IndexB but not IndexA)

        Fortunately, SQL Server provides a built in function for counting the number of 1's in a varbinary. So all we have
        to do is use that function and we have our answer...there's 2 extra colums in IndexB than there are in IndexA. This
        means there might be potential to merge the two indexes.
    */
    DECLARE @Mergeable_ExtraColMax  tinyint = 3;

    -- Mergeable
    IF OBJECT_ID('tempdb..#idx_merge','U') IS NOT NULL DROP TABLE #idx_merge; --SELECT * FROM #idx_merge
    SELECT MergeIntoID = x.IndexID, MergeFromID = y.IndexID, y.ExtraColCount
    INTO #idx_merge
    FROM #idx x
        CROSS APPLY (
            SELECT i.IndexID, c.ExtraColCount
            FROM #idx i
                CROSS APPLY (
                    SELECT ExtraColCount = sys.fn_numberOf1InVarBinary(~x.AllColBitmap1 & i.AllColBitmap1)
                                         + sys.fn_numberOf1InVarBinary(~x.AllColBitmap2 & i.AllColBitmap2)
                                         + sys.fn_numberOf1InVarBinary(~x.AllColBitmap3 & i.AllColBitmap3)
                ) c
            WHERE 1=1
                /* Minimum match criteria */
                AND i.[object_id] = x.[object_id] AND i.index_id <> x.index_id -- Don't match itself
                AND i.has_filter = x.has_filter AND COALESCE(i.filter_definition, '') = COALESCE(x.filter_definition, '') -- If it's filtered, make sure they match
                /* Restrictions */
                /* Clustered indexes, primary keys and unique constraints do not support include columns, so they are not
                   eligible to consider for merging and are excluded from both sides. */
                AND i.IndexType = 'NONCLUSTERED' AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
                AND x.IndexType = 'NONCLUSTERED' AND x.is_primary_key = 0 AND x.is_unique_constraint = 0
                /*  If the left index is unique (right index unique status doesn't matter), then any matching indexes must
                    have the same keys. Non-unique indexes can be merged into a unique index, but not the other way around.
                    If both indexes are non-unique, then the left keys must be covering the right index keys. */
                AND ((x.is_unique = 1 AND x.PhysKeyColIDs = i.PhysKeyColIDs) OR (x.is_unique = 0 AND i.is_unique = 0 AND x.PhysKeyColIDs LIKE i.PhysKeyColIDs + '%'))
                AND ((x.PhysKeyColIDs = i.PhysKeyColIDs AND x.PhysInclColIDs <> '') OR x.PhysKeyColIDs <> i.PhysKeyColIDs) /* Prevent some covering indexe matches from popping up */
                --
                AND (c.ExtraColCount >= 1 AND c.ExtraColCount <= @Mergeable_ExtraColMax) /*  Includes are off by small number */
        ) y;

    /*  If there are any duplicate entries, we want to pick the one which results in the least amount of columns being added. */
    DELETE x
    FROM (
        SELECT rn = ROW_NUMBER() OVER (PARTITION BY x.DedupName ORDER BY m.ExtraColCount)
        FROM #idx_merge m
            CROSS APPLY (SELECT DedupName = IIF(m.MergeIntoID < m.MergeFromID, CONCAT(m.MergeIntoID, '_', m.MergeFromID), CONCAT(m.MergeFromID, '_', m.MergeIntoID))) x
    ) x
    WHERE x.rn > 1;
------------------------------------------------------------------------------
--RETURN
------------------------------------------------------------------------------
-- Output / Results
------------------------------------------------------------------------------
    -- Duplicate
    SELECT 'Duplicate indexes';
    SELECT i.FQIN, i.SchemaName, i.ObjectName, i.ObjectType, i.filter_definition
        , N'█' [██], id.DupeGroupID, id.DupeGroupCount
        , N'█' [██], i.IndexName, i.IndexType, i.is_unique, i.is_primary_key, i.is_unique_constraint
        , N'█' [██], i.PhysKeyColIDs, i.PhysInclColIDs
        , N'█' [██], i.OrigKeyColIDs, i.OrigInclColIDs
        , N'█' [██], i.ClstKeyColIDs
        , N'█' [██], ti.data_space_id, ti.[ignore_dup_key], ti.fill_factor, ti.is_padded, ti.[allow_row_locks], ti.[allow_page_locks], ti.has_filter, ti.filter_definition, ti.auto_created
    FROM #idx_dupes id
        JOIN #idx i ON i.IndexID = id.IndexID
        JOIN #target_indexes ti ON ti.IndexID = i.IndexID
    ORDER BY i.ObjectName, id.DupeGroupID, i.IndexType, i.is_unique DESC;

    -- Covered
    SELECT 'Covered indexes';
    SELECT mi.SchemaName, mi.ObjectName, mi.ObjectType, mi.filter_definition
        , N'█' [██], mi.IndexID, mi.IndexName, mi.IndexType, mi.is_unique, mi.is_primary_key, mi.is_unique_constraint
                   , mi.PhysKeyColIDs, mi.PhysInclColIDs
        --         , mi.OrigKeyColIDs, mi.OrigInclColIDs
                   , mi.ObjColCount, mi.CurrIdxColCount, x.ColDiffCount
        , N'█ Covers --> █' [██], mf.IndexID, mf.IndexName, mf.IndexType, mf.is_unique, mf.is_primary_key, mf.is_unique_constraint
                   , mf.PhysKeyColIDs, mf.PhysInclColIDs
        --         , mf.OrigKeyColIDs, mf.OrigInclColIDs
        --         , mf.FQIN
        , N'█' [██], [ExtraCols - {col_id}[column_name]] datatype] = ec.ExtraCols
    FROM #idx_cover o
        JOIN #idx mi ON mi.IndexID = o.MergeIntoID
        JOIN #idx mf ON mf.IndexID = o.MergeFromID
        LEFT JOIN #idx_cover_extra_cols ec ON ec.MatchID = o.MatchID
        CROSS APPLY (SELECT ColDiffCount = mi.CurrIdxColCount - mf.CurrIdxColCount) x
    WHERE 1=1
        AND NOT EXISTS (SELECT * FROM #idx_cover c WHERE c.MergeFromID = o.MergeIntoID) -- Prevent hierarchies by excluding any parent indexes who also have parents
        AND x.ColDiffCount <= 5 -- Exclude matches when the column difference is significant. If an index has 40 columns, and the matching index has 2, then it's unlikely a candidate to drop
    ORDER BY mi.SchemaName, mi.ObjectName, mi.filter_definition, mi.IndexName, mf.IndexName;

    -- Mergeable
    SELECT 'Mergeable indexes';
    SELECT mi.SchemaName, mi.ObjectName, mi.ObjectType, mi.filter_definition
        , N'█' [██], mi.IndexName, mi.IndexType, mi.is_unique, mi.is_primary_key, mi.is_unique_constraint, mi.PhysKeyColIDs, mi.PhysInclColIDs
        , N'█ Can merge with --> █' [██], mf.IndexName, mf.IndexType, mf.is_unique, mf.is_primary_key, mf.is_unique_constraint, mf.PhysKeyColIDs, mf.PhysInclColIDs, m.ExtraColCount
    FROM #idx_merge m
        JOIN #idx mi ON mi.IndexID = m.MergeIntoID
        JOIN #idx mf ON mf.IndexID = m.MergeFromID
    ORDER BY mi.SchemaName, mi.ObjectName, mi.filter_definition, mi.IndexName, mf.IndexName;
------------------------------------------------------------------------------

------------------------------------------------------------------------------
