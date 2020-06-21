------------------------------------------------------------------------------
-- Usage:
------------------------------------------------------------------------------
/*
	EXEC dbo.usp_SchemaSearch
	--Basic options
		@Search					= 'SearchCriteria',		-- Your search criteria
		@DBName					= NULL,					-- Simple Database filter...Limits everything to only this database
	--Additional options
		@SearchObjContents		= 1,					-- Whether or not you want to search the actual code of a proc, function, view, etc
		@ANDSearch				= NULL,					-- Second search criteria
		@ANDSearch2				= NULL,					-- Third search criteria
		@WholeOnly				= 1,					-- Exclude partial matches...for example, a search of "Entity" will not match with "EntityOpportunity"
		@BaseFilePath			= 'C:\PathToFiles'		-- Provides a base path of where your files are stored, for example, git or SVN --TODO: currently, it's based on the folder/file structure that I like to use. Maybe in the future it can be parameter or table driven
	--Advanced/Beta features:
		@FindReferences			= 0,					-- Warning...this can take a while to run -- Dependent on @SearchObjContents = 1...Provides a first level dependency. Finds all places where each of your search results are mentioned
		@CacheObjects			= 1,					-- Allows you to cache the object definitions to a temp table in your current session. Helps if you are trying to run this many times over and over with no DB filter
		@DBIncludeFilterList	= 'Test%',				-- Advanced Database filter...you can provde a comma separated list of LIKE statements to Include only matching DB's
		@DBExcludeFilterList	= '%[_]Old,%[_]Backup'	-- Advanced Database filter...you can provde a comma separated list of LIKE statements to Exclude any matching DB's

	/*
	Notes: 
		- Database filters are not applied to searching Jobs / Job Steps since the contents of a job step doesn't always match its assigned DB.
	*/
*/
------------------------------------------------------------------------------

------------------------------------------------------------------------------
IF OBJECT_ID('dbo.usp_SchemaSearch') IS NOT NULL DROP PROCEDURE dbo.usp_SchemaSearch;
GO
-- =============================================
-- Author:		Chad Baldwin
-- Create date: 2015-04-13
-- Description:	Searches object names, object contents (whole word and partial), Table names, Column Names, Job Step code, etc
--				Basically...it's SQL Search, but better :)

-- Notes:
	-- TODO - Need to figure out how to search calculated columns
-- =============================================
CREATE PROCEDURE dbo.usp_SchemaSearch (
	@Search					varchar(200),
	@DBName					varchar(50)		= NULL,
	@ANDSearch				varchar(200)	= NULL,
	@ANDSearch2				varchar(200)	= NULL,
	@WholeOnly				bit				= 0,
	@SearchObjContents		bit				= 1,
	@FindReferences			bit				= 1,
	@BaseFilePath			varchar(100)	= '.', --Defaulting to . for default base path
	@Debug					bit				= 0,
	--Beta feature -- Allow user to pass in a list of include/exclude filter lists for the DB -- For now this is in addition to the DBName filter...that can be the simple param, these can be for advanced users I guess?
	@DBIncludeFilterList	varchar(200)	= NULL,
	@DBExcludeFilterList	varchar(200)	= NULL,
	--Beta feature -- Still figuring out how to make this intuitive to a new user of this tool
	@CacheObjects			bit				= 0,
	@SuppressErrors			bit				= 0
)
AS
BEGIN;
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	/*
		DECLARE
			@Search					varchar(200)	= 'TestTest',
			@DBName					varchar(50)		= NULL,
			@ANDSearch				varchar(200)	= NULL,
			@ANDSearch2				varchar(200)	= NULL,
			@WholeOnly				bit				= 0,
			@SearchObjContents		bit				= 1,
			@FindReferences			bit				= 1,
			@BaseFilePath			varchar(100)	= '.',
			@Debug					bit				= 0,
			@DBIncludeFilterList	varchar(200)	= NULL,
			@DBExcludeFilterList	varchar(200)	= NULL,
			@CacheObjects			bit				= 0,
			@SuppressErrors			bit				= 0
	--*/
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	RAISERROR('Object chaching precheck',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		--If caching is enabled, but the schema hasn't been created, stop here and provide the code needed to create the necessary #tables.
		IF (@CacheObjects = 1 AND OBJECT_ID('tempdb..#Objects') IS NULL)
		BEGIN;
			SELECT x.String
			FROM (VALUES  (1, 'Run the below script prior to running this proc.')
						, (2, 'After SchemaSearch finishes running you can')
						, (3, 'query the results. If you run SchemaSearch')
						, (4, 'again, it will re-use the existing data in')
						, (5, 'the table as a cache. If a database is missing,')
						, (6, 'it will be added (but not updated) to the table.')
			) x(ID,String)
			ORDER BY x.ID;

			SELECT Query = 'IF OBJECT_ID(''tempdb..#Objects'')		IS NOT NULL DROP TABLE #Objects			--SELECT * FROM #Objects'
							+ CHAR(13)+CHAR(10)
							+ 'CREATE TABLE #Objects (ID int IDENTITY(1,1) NOT NULL, [Database] nvarchar(128) NOT NULL, SchemaName nvarchar(32) NOT NULL, ObjectName varchar(512) NOT NULL, [Type_Desc] varchar(100) NOT NULL, [Type] char(2) NOT NULL, [Definition] varchar(MAX) NULL, FilePath varchar(512) NULL)';
			RETURN;
		END;

		--If caching was used and then later turned off...we want to clear the table if the user forgets to drop it
		IF (@CacheObjects = 0 AND OBJECT_ID('tempdb..#Objects') IS NOT NULL)
			TRUNCATE TABLE #Objects;
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	RAISERROR('Parameter prep / check',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		IF (@Search = '')
			THROW 51000, 'Must Provide a Search Criteria', 1;

		-- Clean parameters
		SET @DBName					= REPLACE(NULLIF(@DBName,'')		,'_','[_]');
		SET @Search					= REPLACE(@Search					,'_','[_]');
		SET @ANDSearch				= REPLACE(NULLIF(@ANDSearch,'')		,'_','[_]');
		SET @ANDSearch2				= REPLACE(NULLIF(@ANDSearch2,'')	,'_','[_]');
		SET @DBIncludeFilterList	= NULLIF(@DBIncludeFilterList,'');
		SET @DBExcludeFilterList	= NULLIF(@DBExcludeFilterList, '');

		DECLARE	@PartSearch			varchar(512)	=			 '%' + @Search     + '%',
				@ANDPartSearch		varchar(512)	=			 '%' + @ANDSearch  + '%',
				@ANDPartSearch2		varchar(512)	=			 '%' + @ANDSearch2 + '%',
				@WholeSearch		varchar(512)	=  '%[^0-9A-Z_]' + @Search     + '[^0-9A-Z_]%',
				@ANDWholeSearch		varchar(512)	=  '%[^0-9A-Z_]' + @ANDSearch  + '[^0-9A-Z_]%',
				@ANDWholeSearch2	varchar(512)	=  '%[^0-9A-Z_]' + @ANDSearch2 + '[^0-9A-Z_]%',
				@CRLF				char(2)			= CHAR(13)+CHAR(10),
				@ErrorSeverity		tinyint			= IIF(@SuppressErrors = 0, 16, 1); -- 16 - Throws error, continues execution, 1 - message only with error info

		SELECT 'SearchCriteria: ', CONCAT('''', @Search, '''', ' AND ''' + @ANDSearch + '''', ' AND ''' + @ANDSearch2 + '''');
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	RAISERROR('DB Filtering',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		--Parse DB Filter lists
		DECLARE @Delimiter nvarchar(255) = ',';
		IF OBJECT_ID('tempdb..#DBFilters') IS NOT NULL DROP TABLE #DBFilters; --SELECT * FROM #DBFilters
		WITH
			E1(N) AS (SELECT x.y FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) x(y)),
			E2(N) AS (SELECT 1 FROM E1 a CROSS APPLY E1 b CROSS APPLY E1 c),
			cteTally(N) AS (SELECT 0 UNION ALL SELECT ROW_NUMBER() OVER (ORDER BY 1/0) FROM E2)
		SELECT x.DBFilter, FilterText = LTRIM(RTRIM(SUBSTRING(x.List, cteStart.N1, COALESCE(NULLIF(CHARINDEX(@Delimiter, x.List, cteStart.N1),0) - cteStart.N1, 8000))))
		INTO #DBFilters
		FROM (SELECT DBFilter = 'Include', List = @DBIncludeFilterList UNION SELECT 'Exclude', @DBExcludeFilterList) x
			CROSS APPLY (SELECT N1 = t.N + 1 FROM cteTally t WHERE SUBSTRING(x.List, t.N, 1) = @Delimiter OR t.N = 0) cteStart;
		------------------------------------------------------------------------------
	
		------------------------------------------------------------------------------
		--Populate table with a list of all databases user has access to
		DECLARE @DBs table (ID int IDENTITY(1,1) NOT NULL, DBName varchar(100) NOT NULL, HasAccess bit NOT NULL, DBOnline bit NOT NULL);
		INSERT INTO @DBs
		SELECT DBName	= d.[name]
			, HasAccess	= HAS_PERMS_BY_NAME(d.[name], 'DATABASE', 'ANY')
			, DBOnline	= IIF(d.[state] = 0, 1, 0) --IIF([status] & 512 <> 512, 1, 0) --old way to check
		FROM [master].sys.databases d
		WHERE d.[name] NOT IN ('master','tempdb','model','msdb','distribution','sysdb') --exclude system databases --TODO: may need to eventaully add option to search system databases for those who put custom objects in master, or add an override...aka, if 'master' is explicitly provided in @DBName or @DBIncludeFilterList, search it anyways
			AND (d.[name] = @DBName OR @DBName IS NULL)
			AND (
				    (    EXISTS (SELECT * FROM #DBFilters dbf WHERE d.[name] LIKE dbf.FilterText AND dbf.DBFilter = 'Include') OR @DBIncludeFilterList IS NULL)
				AND (NOT EXISTS (SELECT * FROM #DBFilters dbf WHERE d.[name] LIKE dbf.FilterText AND dbf.DBFilter = 'Exclude') OR @DBExcludeFilterList IS NULL)
			)
		ORDER BY HasAccess DESC, DBOnline DESC;

		--TODO - killing the proc if more than 50 DB's...add a parameter to let them run anyways? Similar to the bring the hurt parameters in the blitz procs
		IF ((SELECT COUNT(*) FROM @DBs WHERE HasAccess = 1 AND DBOnline = 1) > 50)
		BEGIN;
			RAISERROR('That''s a lot of databases...Might not be a good idea to run this',0,1) WITH NOWAIT;
			RETURN;
		END;

		--Output list of Databases and Access/Online info
		SELECT DBName, HasAccess, DBOnline FROM @DBs ORDER BY DBName;

		IF EXISTS(SELECT * FROM @DBs db WHERE db.HasAccess = 0)
			RAISERROR('WARNING: Not all databases can be scanned due to permissions',@ErrorSeverity,1) WITH NOWAIT;

		IF EXISTS(SELECT * FROM @DBs db WHERE db.DBOnline = 0)
			RAISERROR('WARNING: Not all databases can be scanned due to being offline',@ErrorSeverity,1) WITH NOWAIT;

		--After this, inaccessible DB's are not needed in the table, easier to just remove them from the table than to explicitly filter them every time later on.
		DELETE d FROM @DBs d WHERE d.HasAccess = 0 OR d.DBOnline = 0;
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	RAISERROR('Temp table prep',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		IF OBJECT_ID('tempdb..#ObjectContents')	IS NOT NULL DROP TABLE #ObjectContents;		--SELECT * FROM #ObjectContents
		CREATE TABLE #ObjectContents(ID int IDENTITY(1,1) NOT NULL, ObjectID int NOT NULL, [Database] nvarchar(128) NOT NULL, SchemaName nvarchar(32) NOT NULL, ObjectName varchar(512) NOT NULL, [Type_Desc] varchar(100) NOT NULL, MatchQuality varchar(100) NOT NULL, FilePath varchar(512) NULL, [Definition] varchar(MAX) NULL);

		--We only want to re-create this table if it's missing and caching is disabled
		IF (@CacheObjects = 0 OR OBJECT_ID('tempdb..#Objects') IS NULL)
		BEGIN;
			IF OBJECT_ID('tempdb..#Objects')	IS NOT NULL DROP TABLE #Objects;			--SELECT * FROM #Objects
			CREATE TABLE #Objects	(ID int IDENTITY(1,1) NOT NULL, [Database] nvarchar(128) NOT NULL, SchemaName nvarchar(32) NOT NULL, ObjectName varchar(512) NOT NULL, [Type_Desc] varchar(100) NOT NULL, [Type] char(2) NOT NULL, [Definition] varchar(MAX) NULL, FilePath varchar(512) NULL);
		END;

		IF OBJECT_ID('tempdb..#Columns')		IS NOT NULL DROP TABLE #Columns;			--SELECT * FROM #Columns
		CREATE TABLE #Columns		(ID int IDENTITY(1,1) NOT NULL, [Database] nvarchar(128) NOT NULL, SchemaName nvarchar(32) NOT NULL, TableName sysname NOT NULL, ColumnName sysname NOT NULL, DataType nvarchar(128) NOT NULL, [MaxLength] int NULL, [Precision] int NULL, Scale int NULL);

		IF OBJECT_ID('tempdb..#SQL')			IS NOT NULL DROP TABLE #SQL;				--SELECT * FROM #SQL
		CREATE TABLE #SQL			(ID int IDENTITY(1,1) NOT NULL, [Database] nvarchar(128) NOT NULL, SQLCode varchar(MAX) NOT NULL);
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	RAISERROR('Job / Job Step searching',0,1) WITH NOWAIT; -- for now, not limiting to DB filters as the contents of the filter doesn't always run within the specified DB for that Step...because people like to be tricky.
	------------------------------------------------------------------------------
	IF (NOT EXISTS (SELECT * FROM sys.fn_my_permissions ('msdb.dbo.sysjobs', 'OBJECT') WHERE [permission_name] = 'SELECT')
		OR NOT EXISTS (SELECT * FROM sys.fn_my_permissions ('msdb.dbo.sysjobsteps', 'OBJECT') WHERE [permission_name] = 'SELECT'))
	BEGIN;
		RAISERROR('WARNING: You do not have permission to search SQL Agent jobs',@ErrorSeverity,1) WITH NOWAIT;
	END;
	ELSE
	BEGIN;
		IF OBJECT_ID('tempdb..#JobStepContents') IS NOT NULL DROP TABLE #JobStepContents; --SELECT * FROM #JobStepContents
		SELECT DBName		= s.[database_name]
			, JobName		= j.[name]
			, StepID		= s.step_id
			, StepName		= s.step_name
			, [Enabled]		= j.[enabled]
			, StepCode		= s.command
			, StepCodeXML	= CONVERT(xml, '<?query --'+@CRLF+s.command+@CRLF+'--?>')
			, JobID			= j.job_id
			, IsRunning		= COALESCE(r.IsRunning, 0)
			, NextRunDate	= n.Next_Run_Date
		INTO #JobStepContents
		FROM msdb.dbo.sysjobs j
			JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
			OUTER APPLY ( --Check to see if its currently running
				SELECT IsRunning = 1
				FROM msdb.dbo.sysjobactivity ja
				WHERE ja.job_id = j.job_id
					AND ja.start_execution_date IS NOT NULL
					AND ja.stop_execution_date IS NULL
					AND ja.session_id = (SELECT TOP (1) ss.session_id FROM msdb.dbo.syssessions ss ORDER BY ss.agent_start_date DESC)
			) r
			OUTER APPLY ( --Get the scheduled time of it's next run...not always accurate due to the latency of updates to the sysjobschedules table, but it's better than nothin I guess
				SELECT Next_Run_Date = MAX(y.NextRunDateTime)
				FROM msdb.dbo.sysjobschedules js WITH(NOLOCK)
					CROSS APPLY (SELECT NextRunTime	= STUFF(STUFF(RIGHT('000000' + CONVERT(varchar(8), js.next_run_time), 6), 5, 0, ':'), 3, 0, ':')) x
					CROSS APPLY (SELECT NextRunDateTime = CASE WHEN j.[enabled] = 0 THEN NULL WHEN js.next_run_date = 0 THEN CONVERT(datetime, '1900-01-01') ELSE CONVERT(datetime, CONVERT(char(8), js.next_run_date, 112) + ' ' + x.NextRunTime) END) y
				WHERE j.job_id = js.job_id
			) n;
		------------------------------------------------------------------------------
	
		------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#JobStepNames_Results') IS NOT NULL DROP TABLE #JobStepNames_Results; --SELECT * FROM #JobStepNames_Results
		SELECT c.DBName, c.JobName, c.StepID, c.StepName, c.[Enabled], c.JobID, c.IsRunning, c.NextRunDate, StepCode = c.StepCodeXML
		INTO #JobStepNames_Results
		FROM #JobStepContents c
		WHERE (	   (c.JobName	LIKE @PartSearch AND c.JobName	LIKE COALESCE(@ANDPartSearch, c.JobName)	AND c.JobName	LIKE COALESCE(@ANDPartSearch2, c.JobName))
				OR (c.StepName	LIKE @PartSearch AND c.StepName	LIKE COALESCE(@ANDPartSearch, c.StepName)	AND c.StepName	LIKE COALESCE(@ANDPartSearch2, c.StepName))
			);

		IF (@@ROWCOUNT > 0)
		BEGIN;
			SELECT 'Job/Step - Names';
			SELECT DBName, JobName, StepID, StepName, [Enabled], StepCode, JobID, IsRunning, NextRunDate FROM #JobStepNames_Results ORDER BY JobName, StepID;
		END;
		ELSE SELECT 'Job/Step - Names', 'NO RESULTS FOUND';
		------------------------------------------------------------------------------
	
		------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#JobStepContents_Results') IS NOT NULL DROP TABLE #JobStepContents_Results; --SELECT * FROM #JobStepContents_Results
		SELECT s.DBName, s.JobName, s.StepID, s.StepName, s.[Enabled], s.JobID, s.IsRunning, s.NextRunDate, StepCode = s.StepCodeXML
		INTO #JobStepContents_Results
		FROM #JobStepContents s
		WHERE s.StepCode LIKE @PartSearch
			AND (s.StepCode LIKE @ANDPartSearch  OR @ANDPartSearch  IS NULL)
			AND (s.StepCode LIKE @ANDPartSearch2 OR @ANDPartSearch2 IS NULL);

		IF (@@ROWCOUNT > 0)
		BEGIN;
			SELECT 'Job step - Contents';
			SELECT DBName, JobName, StepID, StepName, [Enabled], StepCode, JobID, IsRunning, NextRunDate FROM #JobStepContents_Results ORDER BY JobName, StepID;
		END;
		ELSE SELECT 'Job step - Contents', 'NO RESULTS FOUND';
	END;
	------------------------------------------------------------------------------

	------------------------------------------------------------------------------
	RAISERROR('DB Looping',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		--Loop through each database to grab objects
		--TODO: Maybe in the future use sp_MSforeachdb or BrentOzar's sp_foreachdb, for now, I like not having dependent procs/functions
		DECLARE @i int = 1, @SQL nvarchar(MAX) = '', @DB varchar(100);
		WHILE (1=1)
		BEGIN;
			SELECT @DB = DBName FROM @DBs WHERE ID = @i;
			IF (@@ROWCOUNT = 0) BREAK;

			RAISERROR('	%s',0,1,@DB) WITH NOWAIT;

			SELECT @SQL = 'USE ' + @DB;

			--Object search does not have filter as it ended up being faster to grab everything and then filter, also helpful for caching
			IF (NOT EXISTS (SELECT * FROM #Objects WHERE [Database] = @DB))
			BEGIN;
				SELECT @SQL	= @SQL + '
					INSERT INTO #Objects ([Database], SchemaName, ObjectName, [Type_Desc], [Type], [Definition])
					SELECT DB_NAME(), SCHEMA_NAME(o.[schema_id]), o.[name], o.[type_desc], o.[type], ' + IIF(@SearchObjContents = 1, 'OBJECT_DEFINITION(o.[object_id])', 'NULL') + '
					FROM sys.objects o';
			END;

			SELECT @SQL	= @SQL + '
				INSERT INTO #Columns ([Database], SchemaName, TableName, ColumnName, DataType, [MaxLength], [Precision], Scale)
				SELECT DB_NAME(), OBJECT_SCHEMA_NAME(c.[object_id]), OBJECT_NAME(c.[object_id]), c.[name], LOWER(TYPE_NAME(c.system_type_id)), c.max_length, c.[precision], c.scale
				FROM sys.columns c
				WHERE c.[name] LIKE '''+@PartSearch+''''
					+ IIF(@ANDPartSearch  IS NOT NULL, ' AND c.[name] LIKE '''+@ANDPartSearch +'''', '')
					+ IIF(@ANDPartSearch2 IS NOT NULL, ' AND c.[name] LIKE '''+@ANDPartSearch2+'''', '');

			EXEC sys.sp_executesql @statement = @SQL;

			INSERT INTO #SQL ([Database], SQLCode) SELECT @DB, @SQL;
			SELECT @i += 1;
		END;
		RAISERROR('',0,1) WITH NOWAIT;

		--Remove system objects and objects we don't care about searching (ex: constraints, service queues, internal/system tables)
		--TODO: some people may want to search for these items in order to identify databases that have replication or diagramming objects, may need to handle as a parameter or something later
		RAISERROR('Deleting ignored/system objects',0,1) WITH NOWAIT;
		DELETE o
		FROM #Objects o
		WHERE LEFT(o.ObjectName, 9) IN ('sp_MSdel_', 'sp_MSins_', 'sp_MSupd_') --Exclude replication objects
			OR o.ObjectName IN ('fn_diagramobjects','sp_alterdiagram','sp_creatediagram','sp_dropdiagram','sp_helpdiagramdefinition','sp_helpdiagrams','sp_renamediagram','sp_upgraddiagrams') --Exclude diagramming objects
			OR o.[Type] NOT IN ('TT',			--Type table
								'FN','IF','TF',	--Functions
								'U',			--Tables
								'V',			--Views
								'P','PC',		--Procs
								'TR'			--Triggers
							);
		------------------------------------------------------------------------------
		--Populate file paths
		------------------------------------------------------------------------------
			UPDATE o SET
					o.FilePath = CONCAT(COALESCE(@BaseFilePath,'.'),'\',o.[Database],'\')
						+ CASE o.[Type_Desc]
							WHEN 'SQL_STORED_PROCEDURE'				THEN 'StoredProcedures\'					+ o.SchemaName + '.' + o.ObjectName + '.StoredProcedure.sql'
							WHEN 'VIEW'								THEN 'Views\'								+ o.SchemaName + '.' + o.ObjectName + '.View.sql'
							WHEN 'SQL_TABLE_VALUED_FUNCTION'		THEN 'Functions\Table-valued Functions\'	+ o.SchemaName + '.' + o.ObjectName + '.UserDefinedFunction.sql'
							WHEN 'SQL_INLINE_TABLE_VALUED_FUNCTION'	THEN 'Functions\Table-valued Functions\'	+ o.SchemaName + '.' + o.ObjectName + '.UserDefinedFunction.sql'
							WHEN 'SQL_SCALAR_FUNCTION'				THEN 'Functions\Scalar-valued Functions\'	+ o.SchemaName + '.' + o.ObjectName + '.UserDefinedFunction.sql'
							WHEN 'SQL_TRIGGER'						THEN 'Triggers\'							+ o.SchemaName + '.' + o.ObjectName + '.Trigger.sql'
							ELSE NULL
						END
			FROM #Objects o;
	END;
	------------------------------------------------------------------------------

	------------------------------------------------------------------------------
	RAISERROR('Column Name / Object Name Searches',0,1) WITH NOWAIT;
	------------------------------------------------------------------------------
	BEGIN;
		IF (EXISTS(SELECT * FROM #Columns))
		BEGIN;
			SELECT 'Columns (partial matches)';
		
			SELECT [Database], SchemaName, TableName, ColumnName
				, DataType = CONCAT(DataType
								,	CASE
										WHEN DataType IN ('datetime2','time')	THEN IIF(Scale = 7, NULL, CONCAT('(',Scale,')')) --scale of (7) is the default so it can be ignored, (0) is a valid value
										WHEN DataType IN ('datetimeoffset')		THEN CONCAT('(',Scale,')')
										WHEN DataType IN ('decimal','numeric')	THEN CONCAT('(',[Precision],',',Scale,')')
										WHEN DataType IN ('nchar','nvarchar')	THEN IIF(x.[MaxLength] = -1, '(MAX)', CONCAT('(',x.[MaxLength]/2,')'))
										WHEN DataType IN ('char','varchar')		THEN IIF(x.[MaxLength] = -1, '(MAX)', CONCAT('(',x.[MaxLength],')'))
										WHEN DataType IN ('binary','varbinary')	THEN IIF(x.[MaxLength] = -1, '(MAX)', CONCAT('(',x.[MaxLength],')'))
										ELSE NULL
									END)
			FROM #Columns x;
		END;
		ELSE SELECT 'Columns (partial matches)', 'NO RESULTS FOUND';
		------------------------------------------------------------------------------
	
		------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#ObjNames') IS NOT NULL DROP TABLE #ObjNames; --SELECT * FROM #ObjNames
		SELECT o.[Database], o.SchemaName, o.ObjectName, o.[Type_Desc], o.FilePath
		INTO #ObjNames
		FROM #Objects o
		WHERE o.ObjectName LIKE @PartSearch
			AND (@ANDPartSearch IS NULL OR o.ObjectName LIKE @ANDPartSearch)
			AND (@ANDPartSearch2 IS NULL OR o.ObjectName LIKE @ANDPartSearch2)
		ORDER BY o.[Database], o.SchemaName, o.[Type_Desc], o.ObjectName;

		--Covers all objects - Views, Procs, Functions, Triggers, Tables, Constraints
		IF (EXISTS(SELECT * FROM #ObjNames))
		BEGIN;
			SELECT 'Object - Names (partial matches)';

			SELECT o.[Database], o.SchemaName, o.ObjectName, o.[Type_Desc], o.FilePath
			FROM #ObjNames o
			ORDER BY o.[Database], o.SchemaName, o.[Type_Desc], o.ObjectName;
		END;
		ELSE SELECT 'Object - Names', 'NO RESULTS FOUND';
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	-- Object contents searches
	------------------------------------------------------------------------------
	IF (@SearchObjContents = 1)
	BEGIN;
		RAISERROR('Object contents searches',0,1) WITH NOWAIT;

		INSERT INTO #ObjectContents (ObjectID, [Database], SchemaName, ObjectName, [Type_Desc], MatchQuality, FilePath, [Definition])
		SELECT o.ID, o.[Database], o.SchemaName, o.ObjectName, o.[Type_Desc], 'Whole', o.FilePath, o.[Definition]
		FROM #Objects o
			JOIN @DBs db ON db.DBName = o.[Database] --This is only necessary because of Object Caching...if user changes DB filters, we don't want to search other DB's
		WHERE    '#'+o.[Definition]+'#' LIKE @WholeSearch
			AND ('#'+o.[Definition]+'#' LIKE @ANDWholeSearch  OR @ANDWholeSearch  IS NULL)
			AND ('#'+o.[Definition]+'#' LIKE @ANDWholeSearch2 OR @ANDWholeSearch2 IS NULL);

		IF (@WholeOnly = 0)
			INSERT INTO #ObjectContents (ObjectID, [Database], SchemaName, ObjectName, [Type_Desc], MatchQuality, FilePath, [Definition])
			SELECT o.ID, o.[Database], o.SchemaName, o.ObjectName, o.[Type_Desc], 'Partial', o.FilePath, o.[Definition]
			FROM #Objects o
				JOIN @DBs db ON db.DBName = o.[Database] --This is only necessary because of Object Caching...if user changes DB filters, we don't want to search other DB's
			WHERE    o.[Definition] LIKE @PartSearch
				AND (o.[Definition] LIKE @ANDPartSearch  OR @ANDPartSearch  IS NULL)
				AND (o.[Definition] LIKE @ANDPartSearch2 OR @ANDPartSearch2 IS NULL);

		RAISERROR('	Dedup search results',0,1) WITH NOWAIT; -- Whole matches get priority - Add some calculated fields
		IF OBJECT_ID('tempdb..#ObjectContentsResults') IS NOT NULL DROP TABLE #ObjectContentsResults; --SELECT * FROM #ObjectContentsResults
		SELECT ID = IDENTITY(int,1,1), o.ObjectID, o.[Database], o.SchemaName, o.ObjectName, o.[Type_Desc], o.MatchQuality, o.FilePath
			, QuickScript = CASE o.[Type_Desc] --This is mainly just to get a quick parsable snippet so that RedGate SQL Prompt will give you the hover popup to view its contents
								WHEN 'SQL_STORED_PROCEDURE'				THEN CONCAT('-- EXEC '					, o.[Database], '.', o.SchemaName, '.', o.ObjectName)
								WHEN 'VIEW'								THEN CONCAT('-- SELECT TOP(100) * FROM ', o.[Database], '.', o.SchemaName, '.', o.ObjectName)
								WHEN 'SQL_TABLE_VALUED_FUNCTION'		THEN CONCAT('-- SELECT TOP(100) * FROM ', o.[Database], '.', o.SchemaName, '.', o.ObjectName, '() x')
								WHEN 'SQL_INLINE_TABLE_VALUED_FUNCTION'	THEN CONCAT('-- SELECT TOP(100) * FROM ', o.[Database], '.', o.SchemaName, '.', o.ObjectName, '() x')
								WHEN 'SQL_SCALAR_FUNCTION'				THEN CONCAT('-- EXEC '					, o.[Database], '.', o.SchemaName, '.', o.ObjectName, '() x')
								WHEN 'SQL_TRIGGER'						THEN NULL --No action for triggers for now
								ELSE NULL
							END
			, CompleteObjectContents = CONVERT(xml, CONCAT('<?query --', @CRLF, REPLACE(REPLACE(o.[Definition],'<?','/*'),'?>','*/'), @CRLF, '--?>'))
		INTO #ObjectContentsResults
		FROM (
			SELECT ObjectID, [Database], SchemaName, ObjectName, [Type_Desc], MatchQuality, FilePath, [Definition]
				, RN = ROW_NUMBER() OVER (PARTITION BY [Database], ObjectName, [Type_Desc] ORDER BY IIF(MatchQuality = 'Whole', 0, 1)) --If a whole match is found, prefer that over partial match
			FROM #ObjectContents
		) o
		WHERE o.RN = 1;

		--Name match - if you search for something and we find an exact match for that name, separate it out
		IF (EXISTS (SELECT * FROM #ObjectContentsResults o WHERE o.ObjectName LIKE @Search))
		BEGIN;
			SELECT 'Object - Exact Name match';
			SELECT cr.[Database], cr.SchemaName, cr.ObjectName, cr.[Type_Desc], cr.MatchQuality, cr.QuickScript, cr.CompleteObjectContents, cr.FilePath
			FROM #ObjectContentsResults cr
			WHERE cr.ObjectName LIKE @Search --Exact match
			ORDER BY cr.[Database], cr.SchemaName, cr.[Type_Desc], cr.ObjectName;
		END;
		------------------------------------------------------------------------------
		
		------------------------------------------------------------------------------
			IF (EXISTS(SELECT * FROM #ObjectContentsResults WHERE ObjectName NOT LIKE @Search))
			BEGIN;
				SELECT 'Object - Contents'; --Covers all objects - Views, Procs, Functions, Triggers

				IF (@FindReferences = 1)
				BEGIN;
					RAISERROR('	Finding result references',0,1) WITH NOWAIT;

					-- Get references for search matches
					IF OBJECT_ID('tempdb..#ObjectReferences') IS NOT NULL DROP TABLE #ObjectReferences; --SELECT * FROM #ObjectReferences
					SELECT r.ID
						, [Label]		= IIF(x.CombName IS NOT NULL, '--- mentioned in --->>', NULL)
						, Ref_Name		= x.CombName
						, Ref_Type		= x.[Type_Desc]
						, Ref_FilePath	= x.FilePath
					INTO #ObjectReferences
					FROM #ObjectContentsResults r
						--TODO: Change mentioned in / called by code to use referencing entities dm query as a "whole match" so that it's more accurate as to which instance of the object is being referenced, but contintue to also do string matching as a "partial match"
						CROSS APPLY (SELECT SecondarySearch = '%EXEC%[^0-9A-Z_]' + REPLACE(r.ObjectName,'_','[_]') + '[^0-9A-Z_]%') ss --Whole search name, preceded by"EXEC" --Not perfect because it can match procs that have same name in multiple databases
						OUTER APPLY ( --Find all likely called by references, exact matches only -- Can cause left side dups
							SELECT x.CombName, x.[Type_Desc], x.FilePath
							FROM (
								--Procs/Triggers
								SELECT CombName = CONCAT(o.[Database],'.',o.SchemaName,'.',o.ObjectName), o.[Type_Desc], o.FilePath
								FROM #Objects o
								WHERE '#'+o.[Definition]+'#' LIKE ss.SecondarySearch --LIKE Search takes too long
									AND o.ObjectName <> r.ObjectName							--Dont include self
									AND r.[Type_Desc] = 'SQL_STORED_PROCEDURE'					--Reference
									AND o.[Type_Desc] IN ('SQL_STORED_PROCEDURE','SQL_TRIGGER')	--Referenced By
								UNION
								--Jobs
								SELECT CombName = CONCAT(jsc.JobName,' - ',jsc.StepID,') ', COALESCE(NULLIF(jsc.StepName,''),'''''')), 'JOB_STEP', NULL
								FROM #JobStepContents jsc --TODO: Known bug - If user doesn't have access to query SQL agent jobs, then the table won't be created and this will break
								WHERE '#'+jsc.StepCode+'#' LIKE ss.SecondarySearch --LIKE Search takes too long
							) x
						) x;

					--Output with references
					SELECT cr.[Database], cr.SchemaName, cr.ObjectName, cr.[Type_Desc], cr.MatchQuality
						 , r.[Label], r.Ref_Name, r.Ref_Type
						 , cr.QuickScript, cr.CompleteObjectContents, cr.FilePath
						 , r.Ref_FilePath
					FROM #ObjectContentsResults cr
						JOIN #ObjectReferences r ON cr.ID = r.ID
					WHERE cr.ObjectName NOT LIKE @Search
					ORDER BY cr.[Database], cr.SchemaName, cr.[Type_Desc], cr.ObjectName;
				END; ELSE
				BEGIN;
					--Output without references
					SELECT cr.[Database], cr.SchemaName, cr.ObjectName, cr.[Type_Desc], cr.MatchQuality, cr.QuickScript, cr.CompleteObjectContents, cr.FilePath
					FROM #ObjectContentsResults cr
					WHERE cr.ObjectName NOT LIKE @Search
					ORDER BY cr.[Database], cr.SchemaName, cr.[Type_Desc], cr.ObjectName;
				END;
			END;
			ELSE SELECT 'Object - Contents', 'NO RESULTS FOUND';
	END;
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	IF (@Debug = 1)
	BEGIN;
		SELECT 'DEBUG';
		SELECT WholeSearch = @WholeSearch, ANDWholeSearch = @ANDWholeSearch, ANDWholeSearch2 = @ANDWholeSearch2
			, PartSearch = @PartSearch, ANDPartSearch = @ANDPartSearch, ANDPartSearch2 = @ANDPartSearch2;
		SELECT [Database], SQLCode FROM #SQL;
	END;

	RAISERROR('Done',0,1) WITH NOWAIT;
END;
GO