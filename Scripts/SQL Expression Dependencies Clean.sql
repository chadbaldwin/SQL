------------------------------------------------------------

------------------------------------------------------------
DROP TABLE IF EXISTS ##dependencies;
CREATE TABLE ##dependencies (
    ID int NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    poID int NOT NULL,
    poDatabase nvarchar(128) NOT NULL,
    poSchema nvarchar(128) NOT NULL,
    poName nvarchar(128) NOT NULL,
    poType nvarchar(128) NOT NULL,
    coID int NULL,
    coServer nvarchar(128) NULL,
    coDatabase nvarchar(128) NULL,
    coSchema nvarchar(128) NULL,
    coName nvarchar(128) NOT NULL,
    coType nvarchar(128) NULL,

    INDEX ix (coDatabase, coSchema, coName)
);
------------------------------------------------------------

------------------------------------------------------------
INSERT INTO ##dependencies (poID, poDatabase, poSchema, poName, poType, coID, coServer, coDatabase, coSchema, coName, coType)
SELECT d.referencing_id, DB_NAME(), s.[name], o.[name], o.[type_desc] -- Using sys table identifiers to clean up casing differences
    , d.referenced_id, d.referenced_server_name, COALESCE(d.referenced_database_name, DB_NAME()), d.referenced_schema_name, d.referenced_entity_name, co.[type_desc]
FROM sys.sql_expression_dependencies d
    JOIN sys.objects o ON o.[object_id] = d.referencing_id -- Inner join safe here because referencing ID is always populated - NOT NULL
    JOIN sys.schemas s ON s.[schema_id] = o.[schema_id]
    LEFT JOIN sys.objects co ON co.[object_id] = d.referenced_id -- Needs to be left join because referenced object is not always recognized - Also, only local database objects will have an id populated
WHERE 1=1
    AND o.is_ms_shipped = 0
    AND d.referencing_minor_id = 0 AND d.referenced_minor_id = 0 -- We only care about object to object references
    AND d.referencing_class_desc <> 'TYPE' AND d.referenced_class_desc <> 'TYPE'
    AND o.[type_desc] NOT IN ('REPLICATION_FILTER_PROCEDURE')
    AND NOT (s.[name] = 'dbo' AND o.[name] LIKE 'sp[_]MSupd[_]%')
    AND NOT (s.[name] = 'dbo' AND o.[name] LIKE 'sp[_]MSdel[_]%')
    AND NOT (s.[name] = 'dbo' AND o.[name] LIKE 'sp[_]MSins[_]%')
    AND NOT (s.[name] = 'dbo' AND o.[name] IN ('sp_alterdiagram','sp_creatediagram','sp_dropdiagram','sp_helpdiagramdefinition','sp_helpdiagrams','sp_renamediagram','sp_upgraddiagrams','','','','',''))
UNION
-- Triggers are special because SQL doesn't include the relationship of `dbo.TableA` to `dbo.trgTableA`, so we manually include them
SELECT op.[object_id], DB_NAME(), OBJECT_SCHEMA_NAME(op.[object_id]), op.[name], op.[type_desc]
    , ot.[object_id], NULL, DB_NAME(), OBJECT_SCHEMA_NAME(ot.[object_id]), ot.[name], ot.[type_desc]
FROM sys.triggers t
    JOIN sys.objects op ON op.[object_id] = t.parent_id
    JOIN sys.objects ot ON ot.[object_id] = t.[object_id];
----------------------------------------------------------------------

----------------------------------------------------------------------
-- Fill in missing schema where coID is available - if we know the object ID then we confidently know the schema
UPDATE d SET d.coSchema = OBJECT_SCHEMA_NAME(d.coID)
FROM ##dependencies d
WHERE d.coServer IS NULL
    AND d.coID IS NOT NULL AND d.coSchema IS NULL
    AND d.coDatabase = DB_NAME();

-- Fill in missing object IDs - attempt 1 - use provided schema - accurate/reliable
UPDATE d SET d.coID = x.coID
FROM ##dependencies d
    CROSS APPLY (SELECT coID = OBJECT_ID(CONCAT(QUOTENAME(d.coDatabase), '.', QUOTENAME(d.coSchema), '.', QUOTENAME(d.coName)))) x
WHERE d.coServer IS NULL
    /* Child object id is missing, but we have the child schema and object name.
       In theory we should be able to get the child object id from that
       If we can't, then the object is gone and we're victims of deferred name resolution.
       For example...usp_ProcA calls usp_ProcB which creates table dbo.TableA, and then usp_ProcA uses that table and drops it when its done. */
    AND d.coID IS NULL AND d.coSchema IS NOT NULL
    AND x.coID IS NOT NULL;
------------------------------------------------------------

------------------------------------------------------------
-- Fill in missing object IDs and schema
------------------------------------------------------------
/*
   This is where we start getting into the "best attempt" status. The actual logic
   used by SQL Server to determine the schema of an object reference when one is missing
   is different than the logic we're using here. The actual logic is caller dependent.

   In *most* databases though...this is fine as *most* of the time, people aren't writing
   SQL code which purposely has a missing schema and is relying on the users default schema
   to pick the correct object.

   If you *are* doing this...well, maybe don't use this code then.
*/

-- Attempt 2 - inherit parent schema
-- If a the proc `accounting.usp_GetInvoices` is referencing table `Invoices`, it's _probably_ also in the `accounting` schema
UPDATE d SET d.coID = x.coID, d.coSchema = d.poSchema
FROM ##dependencies d
    CROSS APPLY (SELECT coID = OBJECT_ID(CONCAT(QUOTENAME(d.coDatabase), '.', QUOTENAME(d.poSchema), '.', QUOTENAME(d.coName)))) x
WHERE d.coServer IS NULL
    AND d.coID IS NULL AND d.coSchema IS NULL
    AND x.coID IS NOT NULL;

-- Attempt 3 - use `..` schema shortcut
-- Hail mary - if the schema is *still* missing at this point...just try the wretched `..` syntax shortcut and hope for the best
UPDATE d SET d.coID = x.coID, d.coSchema = OBJECT_SCHEMA_NAME(x.coID)
FROM ##dependencies d
    CROSS APPLY (SELECT coID = OBJECT_ID(CONCAT(QUOTENAME(d.coDatabase), '..', QUOTENAME(d.coName)))) x
WHERE d.coServer IS NULL
    AND d.coID IS NULL AND d.coSchema IS NULL
    AND x.coID IS NOT NULL;

/* By this point we should have all schemas filled in for all valid objects
   If we don't, you can use this query to check...And then manually look at the code
   Chances are, they're not true matches. It's just some table alias that happens to
   match a real table.
*/
/*
SELECT N'█ Parent Object ->' [█], d.poSchema, d.poName, d.poType
    , N'█ Child Object ->' [█], d.coDatabase, d.coSchema, d.coName
    , N'█ Fuzzy match ->' [█], [schema_name] = SCHEMA_NAME(o.[schema_id]), o.[name], o.[type_desc]
FROM ##dependencies d
    JOIN sys.objects o ON o.[name] = d.coName -- Loose join simply on name just to see what shows up
WHERE d.coServer IS NULL
    AND d.coSchema IS NULL
    AND d.coName NOT IN ('DELETED','INSERTED')
*/
------------------------------------------------------------

------------------------------------------------------------
-- Remove invalid objects
------------------------------------------------------------
/*
   Example: legacy objects that were dropped but their references weren't, aliases detected as objects, etc

   This is probably a debatable step because there are scenarios (described earlier) where
   we are dealing with a deferred name resolution situation. Perhaps a table is created
   and destroyed regularly and some proc is referencing that table. It will show up as an
   "invalid object" and get removed by this step.

   So the decision to have this step may be revisited in the future.
*/
DELETE d
FROM ##dependencies d
WHERE d.coServer IS NULL
    AND d.coID IS NULL;
------------------------------------------------------------

------------------------------------------------------------
-- Fill in missing object types
------------------------------------------------------------
-- For current database
UPDATE d SET d.coType = o.[type_desc]
FROM ##dependencies d
    JOIN sys.objects o ON o.[object_id] = d.coID
WHERE d.coServer IS NULL
    AND d.coDatabase = DB_NAME()
    AND d.coType IS NULL;

-- For other databases
UPDATE d SET d.coType = x.coType
FROM ##dependencies d
    CROSS APPLY (SELECT FQON = CONCAT(QUOTENAME(d.coDatabase), '.', QUOTENAME(d.coSchema), '.', QUOTENAME(d.coName))) n
    CROSS APPLY (
        SELECT coType = CASE
                            -- Object names are unique, so thankfully we can just rely on whether these return an object id or not.
                            WHEN OBJECT_ID(n.FQON, 'U') IS NOT NULL THEN 'USER_TABLE'
                            WHEN OBJECT_ID(n.FQON, 'P') IS NOT NULL THEN 'SQL_STORED_PROCEDURE'
                            WHEN OBJECT_ID(n.FQON, 'V') IS NOT NULL THEN 'VIEW'
                            WHEN OBJECT_ID(n.FQON, 'X') IS NOT NULL THEN 'EXTENDED_STORED_PROCEDURE'
                            -- TODO: fill in rest as needed
                            ELSE NULL
                        END
    ) x
WHERE d.coServer IS NULL
    AND d.coType IS NULL
    AND x.coType IS NOT NULL;
------------------------------------------------------------

------------------------------------------------------------
-- Quick cleanup for identifier casing
------------------------------------------------------------
UPDATE d
SET d.poDatabase = DB_NAME(DB_ID(d.poDatabase))
    , d.poSchema = OBJECT_SCHEMA_NAME(d.poID, DB_ID(d.poDatabase))
    , d.poName = OBJECT_NAME(d.poID, DB_ID(d.poDatabase))
FROM ##dependencies d
WHERE poID IS NOT NULL;

UPDATE d
SET d.coDatabase = DB_NAME(DB_ID(d.coDatabase))
    , d.coSchema = OBJECT_SCHEMA_NAME(coID, DB_ID(d.coDatabase))
    , d.coName = OBJECT_NAME(d.coID, DB_ID(d.coDatabase))
FROM ##dependencies d
WHERE d.coID IS NOT NULL
    AND d.coServer IS NULL;
------------------------------------------------------------

------------------------------------------------------------
SELECT *
FROM ##dependencies;
