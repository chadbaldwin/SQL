# SQL
Various SQL stuff I'm working on...tools, scripts, etc

- **dbo.uf_DateCalc.UserDefinedFunction.sql** - Returns common date ranges based on today or a supplied seed date. Useful for reporting.
	- You can either use this as a view by not supplying a DateCode parameter and getting a list of all possible ranges. I like to use this for things ilke populating drop downs in SSRS.
	- Or you can supply a specific DateCode to only get a single record back. I like to use this in procs as an easy to read way of indicating the time range for that proc. It just makes for more readable code.

- **dbo.uf_DateRange.UserDefinedFunction.sql**

- **dbo.usp_SchemaSearch.StoredProcedure.sql**
