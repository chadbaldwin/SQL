IF OBJECT_ID('dbo.uf_DateRange') IS NOT NULL DROP FUNCTION dbo.uf_DateRange
GO
-- =============================================
-- Author:		Chad Baldwin
-- Create date: 2017-04-13
-- Description:	Dates
-- =============================================
CREATE FUNCTION dbo.uf_DateRange (	
	@DateFrom DATETIME,
	@DateTo DATETIME,
	@Increment VARCHAR(20)
)
RETURNS @Return TABLE  (
	BeginDate		DATETIME,
	EndDate			DATETIME
)
AS
BEGIN
	IF @Increment NOT IN ('year','yy','yyyy','quarter','qq','q','month','mm','m','week','wk','ww','day','dd','d','hour','hh','minute','mi','n','second','ss','s') RETURN

	DECLARE @TargetDate DATETIME = @DateTo,
			@LoopLimit INT = 5000,
			@Counter INT = 0

	DECLARE @DateRange TABLE (BeginDate DATETIME, EndDate DATETIME)

	WHILE (@Counter < @LoopLimit)
	BEGIN
		INSERT INTO @Return (BeginDate, EndDate)
		SELECT BeginDate	= 	CASE WHEN @Increment IN ('year'	  , 'yy', 'yyyy') THEN DATEADD(yy, DATEDIFF(yy, 0, @TargetDate), 0)
									 WHEN @Increment IN ('quarter', 'qq', 'q'   ) THEN DATEADD(qq, DATEDIFF(qq, 0, @TargetDate), 0)
									 WHEN @Increment IN ('month'  , 'mm', 'm'   ) THEN DATEADD(mm, DATEDIFF(mm, 0, @TargetDate), 0)
									 WHEN @Increment IN ('week'   , 'wk', 'ww'  ) THEN DATEADD(ww, DATEDIFF(ww, 0, @TargetDate), 0)
									 WHEN @Increment IN ('day'    , 'dd', 'd'   ) THEN DATEADD(dd, DATEDIFF(dd, 0, @TargetDate), 0)
									 WHEN @Increment IN ('hour'   , 'hh'        ) THEN DATEADD(hh, DATEDIFF(hh, 0, @TargetDate), 0)
									 WHEN @Increment IN ('minute' , 'mi', 'n'   ) THEN DATEADD(mi, DATEDIFF(mi, 0, @TargetDate), 0)
									 WHEN @Increment IN ('second' , 'ss', 's'   ) THEN DATEADD(ss, DATEDIFF(ss, 0, @TargetDate), 0)
								END
			, EndDate		= 	DATEADD(ms, -3,
								CASE WHEN @Increment IN ('year'	  , 'yy', 'yyyy') THEN DATEADD(yy, DATEDIFF(yy, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('quarter', 'qq', 'q'   ) THEN DATEADD(qq, DATEDIFF(qq, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('month'  , 'mm', 'm'   ) THEN DATEADD(mm, DATEDIFF(mm, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('week'   , 'wk', 'ww'  ) THEN DATEADD(ww, DATEDIFF(ww, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('day'    , 'dd', 'd'   ) THEN DATEADD(dd, DATEDIFF(dd, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('hour'   , 'hh'        ) THEN DATEADD(hh, DATEDIFF(hh, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('minute' , 'mi', 'n'   ) THEN DATEADD(mi, DATEDIFF(mi, 0, @TargetDate) + 1, 0)
									 WHEN @Increment IN ('second' , 'ss', 's'   ) THEN DATEADD(ss, DATEDIFF(ss, 0, @TargetDate) + 1, 0)
								END)		
		SET @TargetDate		=	CASE WHEN @Increment IN ('year'	  , 'yy', 'yyyy') THEN DATEADD(yy, -1, @TargetDate)
									 WHEN @Increment IN ('quarter', 'qq', 'q'   ) THEN DATEADD(qq, -1, @TargetDate)
									 WHEN @Increment IN ('month'  , 'mm', 'm'   ) THEN DATEADD(mm, -1, @TargetDate)
									 WHEN @Increment IN ('week'   , 'wk', 'ww'  ) THEN DATEADD(ww, -1, @TargetDate)
									 WHEN @Increment IN ('day'    , 'dd', 'd'   ) THEN DATEADD(dd, -1, @TargetDate)
									 WHEN @Increment IN ('hour'   , 'hh'        ) THEN DATEADD(hh, -1, @TargetDate)
									 WHEN @Increment IN ('minute' , 'mi', 'n'   ) THEN DATEADD(mi, -1, @TargetDate)
									 WHEN @Increment IN ('second' , 'ss', 's'   ) THEN DATEADD(ss, -1, @TargetDate)
								END
		IF @TargetDate <= @DateFrom BREAK
		SET @Counter += 1
	END

	RETURN
END
GO