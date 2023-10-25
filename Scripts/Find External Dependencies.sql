/* NOTES:
	Needs to be updated to include the list of external database references.

	In order to do this, need to restructure the query so that it's not using DISTINCT.

	Instead needs to be using a GROUP BY, and then maybe use STRING_AGG and STRING_SPLIT
	to expand, check, update, sort and concat for each iteration of the loop.

	Otherwise it will cause the dataset to increase in size which kills the recursion.
*/

WITH find_dependencies AS (
	-- List of objects referencing an external database
	-- Root node
	SELECT DISTINCT Referencing_ObjectName = o.[name]
		, Referencing_ObjectSchema = OBJECT_SCHEMA_NAME(o.[object_id])
		, Referencing_ObjectType = x.ObjectType
	FROM sys.sql_expression_dependencies d
		JOIN sys.objects o ON o.[object_id] = d.referencing_id
		CROSS APPLY (SELECT ObjectType = CASE o.[type] WHEN 'P' THEN 'Stored Procedures' WHEN 'V' THEN 'Views' WHEN 'FN' THEN 'Functions' WHEN 'TF' THEN 'Functions' ELSE NULL END) x
	WHERE d.is_ambiguous = 0
		AND d.referenced_database_name NOT IN ('msdb','master',DB_NAME())
		AND o.[type] NOT IN ('TR')
	UNION ALL
	-- Dependencies
	SELECT g.referencing_entity_name, g.referencing_schema_name, x.ObjectType
	FROM find_dependencies d
		CROSS APPLY sys.dm_sql_referencing_entities(CONCAT(d.Referencing_ObjectSchema,'.',d.Referencing_ObjectName), 'OBJECT') g
		JOIN sys.objects o ON o.[object_id] = g.referencing_id
		CROSS APPLY (SELECT ObjectType = CASE o.[type] WHEN 'P' THEN 'Stored Procedures' WHEN 'V' THEN 'Views' WHEN 'FN' THEN 'Functions' WHEN 'TF' THEN 'Functions' ELSE NULL END) x
	WHERE o.[type] NOT IN ('TR')
)
SELECT DISTINCT d.Referencing_ObjectName, d.Referencing_ObjectSchema, d.Referencing_ObjectType
	, FilePath = CONCAT('.\',d.Referencing_ObjectSchema,'\',d.Referencing_ObjectType,'\',d.Referencing_ObjectName,'.sql')
FROM find_dependencies d
