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
    SELECT FormatFunction			= N'██ Source'
        , FormatCode				= CONVERT(nvarchar(10), N'██')
        , [datetimeoffset]			= CONVERT(nvarchar(60), @dto, 121)
        , [datetime2]				= CONVERT(nvarchar(60), @dt2, 121)
        , [datetime]				= CONVERT(nvarchar(60), @dt, 121)
        , [smalldatetime]			= CONVERT(nvarchar(60), @sdt, 120)
        , [date]					= CONVERT(nvarchar(60), @d, 121)
        , [time]					= CONVERT(nvarchar(60), @t, 121)
        , ID = 0
    UNION ALL
    SELECT FormatFunction = N'TRY_CONVERT', FormatCode = CONVERT(nvarchar(10), t.ID), x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time], t.ID
    FROM cte_tally t
        CROSS APPLY (
            SELECT [datetimeoffset]	= TRY_CONVERT(nvarchar(60), @dto, t.ID)
                ,  [datetime2]		= TRY_CONVERT(nvarchar(60), @dt2, t.ID)
                ,  [datetime]		= TRY_CONVERT(nvarchar(60), @dt, t.ID)
                ,  [smalldatetime]	= TRY_CONVERT(nvarchar(60), @sdt, t.ID)
                ,  [date]			= TRY_CONVERT(nvarchar(60), @d, t.ID)
                ,  [time]			= TRY_CONVERT(nvarchar(60), @t, t.ID)
        ) x
    WHERE t.ID <= 135
        AND COALESCE(x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time]) IS NOT NULL
    UNION ALL
    SELECT FormatFunction = N'FORMAT', FormatCode = CONVERT(nvarchar(10), NCHAR(t.ID)), x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time], t.ID
    FROM cte_tally t
        CROSS APPLY (
            SELECT [datetimeoffset]	= FORMAT(@dto, NCHAR(t.ID))
                ,  [datetime2]		= FORMAT(@dt2, NCHAR(t.ID))
                ,  [datetime]		= FORMAT(@dt, NCHAR(t.ID))
                ,  [smalldatetime]	= FORMAT(@sdt, NCHAR(t.ID))
                ,  [date]			= FORMAT(@d, NCHAR(t.ID))
                ,  [time]			= FORMAT(@t, NCHAR(t.ID))
        ) x
    WHERE (t.ID BETWEEN 65 AND 90 OR t.ID BETWEEN 97 AND 122)
        AND COALESCE(x.[datetimeoffset], x.[datetime2], x.[datetime], x.[smalldatetime], x.[date], x.[time]) IS NOT NULL
) x
ORDER BY x.FormatFunction, IIF(x.FormatFunction = N'FORMAT', x.FormatCode, NULL), x.ID;

/* Sample output

| FormatFunction | FormatCode | datetimeoffset                         | datetime2                           | datetime                            | smalldatetime                       | date                                | time               | 
|----------------|------------|----------------------------------------|-------------------------------------|-------------------------------------|-------------------------------------|-------------------------------------|--------------------| 
| ██ Source      | ██         | 2024-07-11 12:08:14.9550348 -04:00     | 2024-07-11 12:08:14.9550348         | 2024-07-11 12:08:14.957             | 2024-07-11 12:08:00                 | 2024-07-11                          | 12:08:14.9550348   | 
| FORMAT         | c          | NULL                                   | NULL                                | NULL                                | NULL                                | NULL                                | 12:08:14.9550348   | 
| FORMAT         | D          | Thursday, July 11, 2024                | Thursday, July 11, 2024             | Thursday, July 11, 2024             | Thursday, July 11, 2024             | Thursday, July 11, 2024             | NULL               | 
| FORMAT         | d          | 7/11/2024                              | 7/11/2024                           | 7/11/2024                           | 7/11/2024                           | 7/11/2024                           | NULL               | 
| FORMAT         | F          | Thursday, July 11, 2024 12:08:14 PM    | Thursday, July 11, 2024 12:08:14 PM | Thursday, July 11, 2024 12:08:14 PM | Thursday, July 11, 2024 12:08:00 PM | Thursday, July 11, 2024 12:00:00 AM | NULL               | 
| FORMAT         | f          | Thursday, July 11, 2024 12:08 PM       | Thursday, July 11, 2024 12:08 PM    | Thursday, July 11, 2024 12:08 PM    | Thursday, July 11, 2024 12:08 PM    | Thursday, July 11, 2024 12:00 AM    | NULL               | 
| FORMAT         | G          | 7/11/2024 12:08:14 PM                  | 7/11/2024 12:08:14 PM               | 7/11/2024 12:08:14 PM               | 7/11/2024 12:08:00 PM               | 7/11/2024 12:00:00 AM               | 0:12:08:14.9550348 | 
| FORMAT         | g          | 7/11/2024 12:08 PM                     | 7/11/2024 12:08 PM                  | 7/11/2024 12:08 PM                  | 7/11/2024 12:08 PM                  | 7/11/2024 12:00 AM                  | 12:08:14.9550348   | 
| FORMAT         | M          | July 11                                | July 11                             | July 11                             | July 11                             | July 11                             | NULL               | 
| FORMAT         | m          | July 11                                | July 11                             | July 11                             | July 11                             | July 11                             | NULL               | 
| FORMAT         | O          | 2024-07-11T12:08:14.9550348-04:00      | 2024-07-11T12:08:14.9550348         | 2024-07-11T12:08:14.9570000         | 2024-07-11T12:08:00.0000000         | 2024-07-11T00:00:00.0000000         | NULL               | 
| FORMAT         | o          | 2024-07-11T12:08:14.9550348-04:00      | 2024-07-11T12:08:14.9550348         | 2024-07-11T12:08:14.9570000         | 2024-07-11T12:08:00.0000000         | 2024-07-11T00:00:00.0000000         | NULL               | 
| FORMAT         | R          | Thu, 11 Jul 2024 16:08:14 GMT          | Thu, 11 Jul 2024 12:08:14 GMT       | Thu, 11 Jul 2024 12:08:14 GMT       | Thu, 11 Jul 2024 12:08:00 GMT       | Thu, 11 Jul 2024 00:00:00 GMT       | NULL               | 
| FORMAT         | r          | Thu, 11 Jul 2024 16:08:14 GMT          | Thu, 11 Jul 2024 12:08:14 GMT       | Thu, 11 Jul 2024 12:08:14 GMT       | Thu, 11 Jul 2024 12:08:00 GMT       | Thu, 11 Jul 2024 00:00:00 GMT       | NULL               | 
| FORMAT         | s          | 2024-07-11T12:08:14                    | 2024-07-11T12:08:14                 | 2024-07-11T12:08:14                 | 2024-07-11T12:08:00                 | 2024-07-11T00:00:00                 | NULL               | 
| FORMAT         | T          | 12:08:14 PM                            | 12:08:14 PM                         | 12:08:14 PM                         | 12:08:00 PM                         | 12:00:00 AM                         | 12:08:14.9550348   | 
| FORMAT         | t          | 12:08 PM                               | 12:08 PM                            | 12:08 PM                            | 12:08 PM                            | 12:00 AM                            | 12:08:14.9550348   | 
| FORMAT         | U          | NULL                                   | Thursday, July 11, 2024 4:08:14 PM  | Thursday, July 11, 2024 4:08:14 PM  | Thursday, July 11, 2024 4:08:00 PM  | Thursday, July 11, 2024 4:00:00 AM  | NULL               | 
| FORMAT         | u          | 2024-07-11 16:08:14Z                   | 2024-07-11 12:08:14Z                | 2024-07-11 12:08:14Z                | 2024-07-11 12:08:00Z                | 2024-07-11 00:00:00Z                | NULL               | 
| FORMAT         | Y          | July 2024                              | July 2024                           | July 2024                           | July 2024                           | July 2024                           | NULL               | 
| FORMAT         | y          | July 2024                              | July 2024                           | July 2024                           | July 2024                           | July 2024                           | NULL               | 
| TRY_CONVERT    | 1          | 07/11/24                               | 07/11/24                            | 07/11/24                            | 07/11/24                            | 07/11/24                            | NULL               | 
| TRY_CONVERT    | 2          | 24.07.11                               | 24.07.11                            | 24.07.11                            | 24.07.11                            | 24.07.11                            | NULL               | 
| TRY_CONVERT    | 3          | 11/07/24                               | 11/07/24                            | 11/07/24                            | 11/07/24                            | 11/07/24                            | NULL               | 
| TRY_CONVERT    | 4          | 11.07.24                               | 11.07.24                            | 11.07.24                            | 11.07.24                            | 11.07.24                            | NULL               | 
| TRY_CONVERT    | 5          | 11-07-24                               | 11-07-24                            | 11-07-24                            | 11-07-24                            | 11-07-24                            | NULL               | 
| TRY_CONVERT    | 6          | 11 Jul 24                              | 11 Jul 24                           | 11 Jul 24                           | 11 Jul 24                           | 11 Jul 24                           | NULL               | 
| TRY_CONVERT    | 7          | Jul 11, 24                             | Jul 11, 24                          | Jul 11, 24                          | Jul 11, 24                          | Jul 11, 24                          | NULL               | 
| TRY_CONVERT    | 8          | 12:08:14 -04:00                        | 12:08:14                            | 12:08:14                            | 12:08:00                            | NULL                                | 12:08:14           | 
| TRY_CONVERT    | 9          | Jul 11 2024 12:08:14.9550348PM -04:00  | Jul 11 2024 12:08:14.9550348PM      | Jul 11 2024 12:08:14:957PM          | Jul 11 2024 12:08:00:000PM          | Jul 11 2024                         | 12:08:14.9550348PM | 
| TRY_CONVERT    | 10         | 07-11-24                               | 07-11-24                            | 07-11-24                            | 07-11-24                            | 07-11-24                            | NULL               | 
| TRY_CONVERT    | 11         | 24/07/11                               | 24/07/11                            | 24/07/11                            | 24/07/11                            | 24/07/11                            | NULL               | 
| TRY_CONVERT    | 12         | 240711                                 | 240711                              | 240711                              | 240711                              | 240711                              | NULL               | 
| TRY_CONVERT    | 13         | 11 Jul 2024 12:08:14.9550348 -04:00    | 11 Jul 2024 12:08:14.9550348        | 11 Jul 2024 12:08:14:957            | 11 Jul 2024 12:08:00:000            | 11 Jul 2024                         | 12:08:14.9550348   | 
| TRY_CONVERT    | 14         | 12:08:14.9550348 -04:00                | 12:08:14.9550348                    | 12:08:14:957                        | 12:08:00:000                        | NULL                                | 12:08:14.9550348   | 
| TRY_CONVERT    | 20         | 2024-07-11 12:08:14 -04:00             | 2024-07-11 12:08:14                 | 2024-07-11 12:08:14                 | 2024-07-11 12:08:00                 | 2024-07-11                          | 12:08:14           | 
| TRY_CONVERT    | 21         | 2024-07-11 12:08:14.9550348 -04:00     | 2024-07-11 12:08:14.9550348         | 2024-07-11 12:08:14.957             | 2024-07-11 12:08:00.000             | 2024-07-11                          | 12:08:14.9550348   | 
| TRY_CONVERT    | 22         | 07/11/24 12:08:14 PM -04:00            | 07/11/24 12:08:14 PM                | 07/11/24 12:08:14 PM                | 07/11/24 12:08:00 PM                | 07/11/24                            | 12:08:14 PM        | 
| TRY_CONVERT    | 23         | 2024-07-11                             | 2024-07-11                          | 2024-07-11                          | 2024-07-11                          | 2024-07-11                          | NULL               | 
| TRY_CONVERT    | 24         | 12:08:14 -04:00                        | 12:08:14                            | 12:08:14                            | 12:08:00                            | NULL                                | 12:08:14           | 
| TRY_CONVERT    | 25         | 2024-07-11 12:08:14.9550348 -04:00     | 2024-07-11 12:08:14.9550348         | 2024-07-11 12:08:14.957             | 2024-07-11 12:08:00.000             | 2024-07-11                          | 12:08:14.9550348   | 
| TRY_CONVERT    | 100        | Jul 11 2024 12:08PM -04:00             | Jul 11 2024 12:08PM                 | Jul 11 2024 12:08PM                 | Jul 11 2024 12:08PM                 | Jul 11 2024                         | 12:08PM            | 
| TRY_CONVERT    | 101        | 07/11/2024                             | 07/11/2024                          | 07/11/2024                          | 07/11/2024                          | 07/11/2024                          | NULL               | 
| TRY_CONVERT    | 102        | 2024.07.11                             | 2024.07.11                          | 2024.07.11                          | 2024.07.11                          | 2024.07.11                          | NULL               | 
| TRY_CONVERT    | 103        | 11/07/2024                             | 11/07/2024                          | 11/07/2024                          | 11/07/2024                          | 11/07/2024                          | NULL               | 
| TRY_CONVERT    | 104        | 11.07.2024                             | 11.07.2024                          | 11.07.2024                          | 11.07.2024                          | 11.07.2024                          | NULL               | 
| TRY_CONVERT    | 105        | 11-07-2024                             | 11-07-2024                          | 11-07-2024                          | 11-07-2024                          | 11-07-2024                          | NULL               | 
| TRY_CONVERT    | 106        | 11 Jul 2024                            | 11 Jul 2024                         | 11 Jul 2024                         | 11 Jul 2024                         | 11 Jul 2024                         | NULL               | 
| TRY_CONVERT    | 107        | Jul 11, 2024                           | Jul 11, 2024                        | Jul 11, 2024                        | Jul 11, 2024                        | Jul 11, 2024                        | NULL               | 
| TRY_CONVERT    | 108        | 12:08:14 -04:00                        | 12:08:14                            | 12:08:14                            | 12:08:00                            | NULL                                | 12:08:14           | 
| TRY_CONVERT    | 109        | Jul 11 2024 12:08:14.9550348PM -04:00  | Jul 11 2024 12:08:14.9550348PM      | Jul 11 2024 12:08:14:957PM          | Jul 11 2024 12:08:00:000PM          | Jul 11 2024                         | 12:08:14.9550348PM | 
| TRY_CONVERT    | 110        | 07-11-2024                             | 07-11-2024                          | 07-11-2024                          | 07-11-2024                          | 07-11-2024                          | NULL               | 
| TRY_CONVERT    | 111        | 2024/07/11                             | 2024/07/11                          | 2024/07/11                          | 2024/07/11                          | 2024/07/11                          | NULL               | 
| TRY_CONVERT    | 112        | 20240711                               | 20240711                            | 20240711                            | 20240711                            | 20240711                            | NULL               | 
| TRY_CONVERT    | 113        | 11 Jul 2024 12:08:14.9550348 -04:00    | 11 Jul 2024 12:08:14.9550348        | 11 Jul 2024 12:08:14:957            | 11 Jul 2024 12:08:00:000            | 11 Jul 2024                         | 12:08:14.9550348   | 
| TRY_CONVERT    | 114        | 12:08:14.9550348 -04:00                | 12:08:14.9550348                    | 12:08:14:957                        | 12:08:00:000                        | NULL                                | 12:08:14.9550348   | 
| TRY_CONVERT    | 120        | 2024-07-11 12:08:14 -04:00             | 2024-07-11 12:08:14                 | 2024-07-11 12:08:14                 | 2024-07-11 12:08:00                 | 2024-07-11                          | 12:08:14           | 
| TRY_CONVERT    | 121        | 2024-07-11 12:08:14.9550348 -04:00     | 2024-07-11 12:08:14.9550348         | 2024-07-11 12:08:14.957             | 2024-07-11 12:08:00.000             | 2024-07-11                          | 12:08:14.9550348   | 
| TRY_CONVERT    | 126        | 2024-07-11T12:08:14.9550348-04:00      | 2024-07-11T12:08:14.9550348         | 2024-07-11T12:08:14.957             | 2024-07-11T12:08:00                 | 2024-07-11                          | 12:08:14.9550348   | 
| TRY_CONVERT    | 127        | 2024-07-11T16:08:14.9550348Z           | 2024-07-11T12:08:14.9550348         | 2024-07-11T12:08:14.957             | 2024-07-11T12:08:00                 | 2024-07-11                          | 12:08:14.9550348   | 
| TRY_CONVERT    | 130        |  5 محرم 1446 12:08:14.9550348PM -04:00 |  5 محرم 1446 12:08:14.9550348PM     |  5 محرم 1446 12:08:14:957PM         |  5 محرم 1446 12:08:00:000PM         |  5 محرم 1446                        | 12:08:14.9550348PM | 
| TRY_CONVERT    | 131        |  5/01/1446 12:08:14.9550348PM -04:00   |  5/01/1446 12:08:14.9550348PM       |  5/01/1446 12:08:14:957PM           |  5/01/1446 12:08:00:000PM           |  5/01/1446                          | 12:08:14.9550348PM | 
*/
