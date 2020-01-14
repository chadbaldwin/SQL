# SQL
Various SQL stuff I'm working on...tools, scripts, etc

- **dbo.uf_DateCalc.UserDefinedFunction.sql** - Returns common date ranges based on today or a supplied seed date. Useful for reporting.
	- You can either use this as a view by not supplying a DateCode parameter and getting a list of all possible ranges. I like to use this for things like populating drop downs in SSRS.
	- Or you can supply a specific DateCode to only get a single record back. I like to use this in procs as an easy to read way of indicating the time range for that proc. It just makes for more readable code.

- **dbo.uf_DateRange.UserDefinedFunction.sql**

- **dbo.usp_SchemaSearch.StoredProcedure.sql**
	- This is my pride and joy. I use this probably 50 times a day. The easiest way to describe it is...it's a proc version of RedGate SQL Search. I actually use RedGate SQL Search as well...but I found there were many features that I felt were missing, so I decided to make my own.
	- A few of the features include...
		- Multiple search criteria (`@Search`, `@ANDSearch`, `@ANDSearch2`)
			- (AND joins only)...search "this" AND "that"
		- Partial and exact match option (`@WholeOnly`)
			- If `@WholeOnly` = true, similar to "match whole word", applies to all search parameters
		- Searches all object types...triggers, functions, procs, views, etc
		- Output the physical file path of the item (`@BaseFilePath`)
			- This makes it easy to quickly open the actual file
			- Example CTRL+C, CTRL+O, CTRL+V, Enter...I have a keyboard macro set up for it
			- As of now...it only supports one convention for file paths and names, which is the one I use...I hope eventually, I can add some functionality to make that configurable.
		- 2nd level of depth (`@FindReferences`)
			- Ability to find all the references to your search results
			- Example...you search for a keyword, the proc will return all procs that contain that keyword. You can then go one level deeper and enable @FindReferences, this will find all objects that reference those objects.
				- Limitations - only finds references for stored procedure results, and it only searches other stored procedures and job steps
		- Result caching (`@CacheObjects`, `@CacheOutputSchema`)
			- Many times if you are using this heavily to trace a large process, you may not want to hit every database to grab every object every time you run this proc. So there is an option to cache all objects into a temp table. This way each run, only the temp table is searched. You can also query the temp table manually for more complex searches of your own.
		- DB filter lists (`@DBName`, `@DBIncludeFilterList`, `@DBExcludeFilterList`)
			- The ability to provide both and inclusion and exclusion list of database name filters (using LIKE syntax, comma delimited)
			- For example...if you want to exclude all databases with '%test%,%backup%'. That will exclude all databases that have "test" or "backup" in the name
			- `@DBName` - included for simplicity / legacy but will likely be removed eventually. It's an exact match parameter. Filters the proc to only look at the one database, and is an exact match.
