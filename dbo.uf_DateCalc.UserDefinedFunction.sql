IF OBJECT_ID('dbo.uf_DateCalc') IS NOT NULL DROP FUNCTION dbo.uf_DateCalc
GO
-- =============================================
-- Author:		Chad Baldwin
-- Create date: 2017-04-13
-- Description:	Dates
-- =============================================
CREATE FUNCTION dbo.uf_DateCalc (	
	@SeedDate DATE,
	@DateCode VARCHAR(5)
)
RETURNS @Return TABLE  (
	Code			VARCHAR(5),
	Label			VARCHAR(100),
	BeginDate		DATETIME,
	EndDate			DATETIME,
	EndOfDayDate	DATETIME
)
AS
BEGIN
	SELECT @SeedDate	= COALESCE(@SeedDate, GETDATE())

	INSERT INTO @Return (Code, Label, BeginDate, EndDate, EndOfDayDate)
	SELECT Code			= x.Code
		, Label			= CONVERT(VARCHAR(100), x.[Period])
		, BeginDate		= CONVERT(DATETIME, x.BeginDate)
		, EndDate		= CONVERT(DATETIME, x.EndDate)
		, EndOfDayDate	= x.EndDate + CONVERT(DATETIME, '23:59:59.997')
	FROM (
		SELECT Today = CONVERT(DATETIME, @SeedDate) --Reporting on last complete day
	) t
		CROSS APPLY (SELECT DiffYY	= DATEDIFF(yy, 0, t.Today)
						,   DiffMM	= DATEDIFF(mm, 0, t.Today)
						,   DiffWK	= DATEDIFF(wk, 0, t.Today)
						,   DiffDD	= DATEDIFF(dd, 0, t.Today)
						,   DiffQQ	= DATEDIFF(qq, 0, t.Today)
		) y
		CROSS APPLY (
			VALUES 	  ('D'		, 'Day'								, t.Today						, t.Today							)
					, ('M'		, 'Month'							, DATEADD(mm, y.DiffMM, 0)		, DATEADD(mm, y.DiffMM + 1, 0) - 1	)
					, ('MTD'	, 'Month To Date'					, DATEADD(mm, y.DiffMM, 0)		, t.Today							)
					, ('Q'		, 'Quarter'							, DATEADD(qq, y.DiffQQ, 0)		, DATEADD(qq, y.DiffQQ + 1, 0) - 1	)
					, ('QTD'	, 'Quarter to Date'					, DATEADD(qq, y.DiffQQ, 0)		, t.Today							)
					, ('YTD'	, 'Year To Date'					, DATEADD(yy, y.DiffYY, 0)		, t.Today							)

					, ('PD'		, 'Previous Day'					, t.Today - 1					, t.Today - 1						)
					, ('PW'		, 'Previous Week'					, DATEADD(wk, y.DiffWK - 1, 0)	, DATEADD(wk, y.DiffWK, 0) - 1		)
					, ('PM'		, 'Previous Month'					, DATEADD(mm, y.DiffMM - 1, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
					, ('PMTD'	, 'Previous Month to Date'			, DATEADD(mm, y.DiffMM - 1, 0)	, DATEADD(mm, -1, t.Today)			)
					, ('PQ'		, 'Previous Quarter'				, DATEADD(qq, y.DiffQQ - 1, 0)	, DATEADD(qq, y.DiffQQ, 0) - 1		)
					, ('PQTD'	, 'Previous Quarter to Date'		, DATEADD(qq, y.DiffQQ - 1, 0)	, DATEADD(qq, -1, t.Today)			)
					, ('PYQ'	, 'Previous Year Quarter'			, DATEADD(qq, y.DiffQQ - 4, 0)	, DATEADD(qq, y.DiffQQ - 3, 0) - 1	)
					, ('PYQTD'	, 'Previous Year Quarter to Date'	, DATEADD(qq, y.DiffQQ - 4, 0)	, DATEADD(yy, -1, t.Today)			)
					, ('PY'		, 'Previous Year'					, DATEADD(yy, y.DiffYY - 1, 0)	, DATEADD(yy, y.DiffYY, 0) - 1		)
					, ('PYTD'	, 'Previous Year to Date'			, DATEADD(yy, y.DiffYY - 1, 0)	, DATEADD(yy, -1, t.Today)			)

					, ('L7D'	, 'Last 7 days'						, t.Today - ( 7 - 1)			, t.Today							)
					, ('L14D'	, 'Last 14 days'					, t.Today - (14 - 1)			, t.Today							)
					, ('L21D'	, 'Last 21 days'					, t.Today - (21 - 1)			, t.Today							)
					, ('L28D'	, 'Last 28 days'					, t.Today - (28 - 1)			, t.Today							)
					, ('L30D'	, 'Last 30 days'					, t.Today - (30 - 1)			, t.Today							)
					, ('L60D'	, 'Last 60 days'					, t.Today - (60 - 1)			, t.Today							)
					, ('L90D'	, 'Last 90 days'					, t.Today - (90 - 1)			, t.Today							)

				--	, ('L2M'	, 'Last 2 months'					, DATEADD(mm, y.DiffMM - 2, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
				--	, ('L3M'	, 'Last 3 months'					, DATEADD(mm, y.DiffMM - 3, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
				--	, ('L4M'	, 'Last 4 months'					, DATEADD(mm, y.DiffMM - 4, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
				--	, ('L5M'	, 'Last 5 months'					, DATEADD(mm, y.DiffMM - 5, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
				--	, ('L6M'	, 'Last 6 months'					, DATEADD(mm, y.DiffMM - 6, 0)	, DATEADD(mm, y.DiffMM, 0) - 1		)
		) x(Code, [Period], BeginDate, EndDate)
	WHERE x.Code = @DateCode OR @DateCode IS NULL

	RETURN
END
GO