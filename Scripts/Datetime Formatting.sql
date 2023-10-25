/*	Builds a table of all possible formatting options using CONVERT and FORMAT
	Useful for looking up what code you want to use to format your date values. */

DECLARE @dto	datetimeoffset,
		@dt2	datetime2,
		@dt		datetime,
		@sdt	smalldatetime,
		@d		date,
		@t		time;

SELECT @dto	= SYSDATETIMEOFFSET()
	, @dt2	= @dto
	, @dt	= @dto
	, @sdt	= @dto
	, @d	= @dto
	, @t	= @dto;

WITH cte_tally AS (SELECT ID = ROW_NUMBER() OVER (ORDER BY (SELECT 1)) FROM sys.messages)
SELECT x.FormatFunction, x.FormatCode, x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time]
FROM (
    SELECT FormatFunction = N'██ Source'
        , FormatCode = CONVERT(nvarchar(10), N'██')
        , [datetimeoffset] = CONVERT(nvarchar(60), @dto, 121)
        , [datetime2] = CONVERT(nvarchar(60), @dt2, 121)
        , [datetime] = CONVERT(nvarchar(60), @dt, 121)
        , [smalldatetime] = CONVERT(nvarchar(60), @sdt, 120)
        , [date] = CONVERT(nvarchar(60), @d, 121)
        , [time] = CONVERT(nvarchar(60), @t, 121)
        , ID = 0
    UNION ALL
    SELECT FormatFunction = N'TRY_CONVERT', FormatCode = CONVERT(nvarchar(10), t.ID), x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time], t.ID
    FROM cte_tally t
        CROSS APPLY (
            SELECT [datetimeoffset] = TRY_CONVERT(nvarchar(60), @dto, t.ID)
                ,  [datetime2] = TRY_CONVERT(nvarchar(60), @dt2, t.ID)
                ,  [datetime] = TRY_CONVERT(nvarchar(60), @dt, t.ID)
                ,  [smalldatetime] = TRY_CONVERT(nvarchar(60), @sdt, t.ID)
                ,  [date] = TRY_CONVERT(nvarchar(60), @d, t.ID)
                ,  [time] = TRY_CONVERT(nvarchar(60), @t, t.ID)
        ) x
    WHERE t.ID <= 135
        AND COALESCE(x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time]) IS NOT NULL
    UNION ALL
    SELECT FormatFunction = N'FORMAT', FormatCode = CONVERT(nvarchar(10), NCHAR(t.ID)), x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time], t.ID
    FROM cte_tally t
        CROSS APPLY (
            SELECT [datetimeoffset] = FORMAT(@dto, NCHAR(t.ID))
                ,  [datetime2] = FORMAT(@dt2, NCHAR(t.ID))
                ,  [datetime] = FORMAT(@dt, NCHAR(t.ID))
                ,  [smalldatetime] = FORMAT(@sdt, NCHAR(t.ID))
                ,  [date] = FORMAT(@d, NCHAR(t.ID))
                ,  [time] = FORMAT(@t, NCHAR(t.ID))
        ) x
    WHERE (t.ID BETWEEN 65 AND 90 OR t.ID BETWEEN 97 AND 122)
        AND COALESCE(x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time]) IS NOT NULL
) x
ORDER BY x.FormatFunction, IIF(x.FormatFunction = N'FORMAT', x.FormatCode, NULL), x.ID;
