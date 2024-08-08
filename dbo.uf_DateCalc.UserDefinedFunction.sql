CREATE OR ALTER FUNCTION dbo.uf_DateCalc (  
    @SeedDate date = NULL,
    @DateCode varchar(5) = NULL
)
RETURNS @Return table  (
    Code                varchar(5)      NOT NULL,
    [Label]             varchar(100)    NOT NULL,
    BeginDate           datetime2       NOT NULL,
    EndDateInclusiveDT  datetime        NOT NULL,
    EndDateInclusiveDT2 datetime2       NOT NULL,
    EndDateExclusive    datetime2       NOT NULL
)
AS
BEGIN;
    SELECT @SeedDate = COALESCE(@SeedDate, GETDATE());

    WITH cte_Dates AS (
        SELECT DiffDD = DATEDIFF(DAY    , 0, @SeedDate)
            ,  DiffWK = DATEDIFF(WEEK   , 0, @SeedDate)
            ,  DiffMM = DATEDIFF(MONTH  , 0, @SeedDate)
            ,  DiffQQ = DATEDIFF(QUARTER, 0, @SeedDate)
			,  DiffYY = DATEDIFF(YEAR   , 0, @SeedDate)
    )
    INSERT INTO @Return (Code, [Label], BeginDate, EndDateInclusiveDT, EndDateInclusiveDT2, EndDateExclusive)
    SELECT Code               = x.Code
        , [Label]             = CONVERT(varchar(100), x.[Period])
        , BeginDate           = CONVERT(datetime2, x.BeginDate)
        , EndDateInclusiveDT  = DATEADD(MILLISECOND,  -3, DATEADD(DAY, 1, CONVERT(datetime , x.EndDate)))
        , EndDateInclusiveDT2 = DATEADD(NANOSECOND, -100, DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))) -- +1 day -1 tick
        , EndDateExclusive    = DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))
    FROM cte_Dates t
        CROSS APPLY (
            VALUES    ('CD'   , 'Current Day'                  , @SeedDate                           , @SeedDate                                               )
                    , ('CM'   , 'Current Month'                , DATEADD(MONTH  , t.DiffMM    , 0)   , DATEADD(DAY    , -1, DATEADD(MONTH  , t.DiffMM + 1, 0)) )
                    , ('CMTD' , 'Current Month To Date'        , DATEADD(MONTH  , t.DiffMM    , 0)   , @SeedDate                                               )
                    , ('CQ'   , 'Current Quarter'              , DATEADD(QUARTER, t.DiffQQ    , 0)   , DATEADD(DAY    , -1, DATEADD(QUARTER, t.DiffQQ + 1, 0)) )
                    , ('CQTD' , 'Current Quarter to Date'      , DATEADD(QUARTER, t.DiffQQ    , 0)   , @SeedDate                                               )
                    , ('CY'   , 'Current Year'                 , DATEADD(YEAR   , t.DiffYY    , 0)   , DATEADD(DAY    , -1, DATEADD(YEAR   , t.DiffYY + 1, 0)) )
                    , ('CYTD' , 'Current Year To Date'         , DATEADD(YEAR   , t.DiffYY    , 0)   , @SeedDate                                               )

                    , ('PD'   , 'Previous Day'                 , DATEADD(DAY    , t.DiffDD - 1, 0)   , DATEADD(DAY    , -1, @SeedDate)                         )
            --      , ('PW'   , 'Previous Week'                , DATEADD(WEEK   , t.DiffWK - 1, 0)   , DATEADD(DAY    , -1, DATEADD(WEEK   , t.DiffWK    , 0)) ) -- Removed temporarily - for some reason defaults to Monday as first day of week, despite DATEFIRST setting
                    , ('PM'   , 'Previous Month'               , DATEADD(MONTH  , t.DiffMM - 1, 0)   , DATEADD(DAY    , -1, DATEADD(MONTH  , t.DiffMM    , 0)) )
                    , ('PMTD' , 'Previous Month to Date'       , DATEADD(MONTH  , t.DiffMM - 1, 0)   , DATEADD(MONTH  , -1, @SeedDate)                         )
                    , ('PQ'   , 'Previous Quarter'             , DATEADD(QUARTER, t.DiffQQ - 1, 0)   , DATEADD(DAY    , -1, DATEADD(QUARTER, t.DiffQQ    , 0)) )
                    , ('PQTD' , 'Previous Quarter to Date'     , DATEADD(QUARTER, t.DiffQQ - 1, 0)   , DATEADD(QUARTER, -1, @SeedDate)                         )
                    , ('PYQ'  , 'Previous Year Quarter'        , DATEADD(QUARTER, t.DiffQQ - 4, 0)   , DATEADD(DAY    , -1, DATEADD(QUARTER, t.DiffQQ - 3, 0)) )
                    , ('PYQTD', 'Previous Year Quarter to Date', DATEADD(QUARTER, t.DiffQQ - 4, 0)   , DATEADD(YEAR   , -1, @SeedDate)                         )
                    , ('PY'   , 'Previous Year'                , DATEADD(YEAR   , t.DiffYY - 1, 0)   , DATEADD(DAY    , -1, DATEADD(YEAR   , t.DiffYY    , 0)) )
                    , ('PYTD' , 'Previous Year to Date'        , DATEADD(YEAR   , t.DiffYY - 1, 0)   , DATEADD(YEAR   , -1, @SeedDate)                         )
        ) x(Code, [Period], BeginDate, EndDate)
    WHERE x.Code = @DateCode OR @DateCode IS NULL
    UNION
	-- Handling for P7D and L7D style date codes
	-- Supporting only days for now. Months requires a bit more work to calcualte the end of month value.
    SELECT Code               = UPPER(@DateCode)
        , [Label]             = CONCAT_WS(' ', CASE LEFT(@DateCode, 1) WHEN 'L' THEN 'Last' WHEN 'P' THEN 'Previous' ELSE NULL END, t.[Value], 'days')
        , BeginDate           = CONVERT(datetime2, x.BeginDate)
        , EndDateInclusiveDT  = DATEADD(MILLISECOND,  -3, DATEADD(DAY, 1, CONVERT(datetime , x.EndDate)))
        , EndDateInclusiveDT2 = DATEADD(NANOSECOND, -100, DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))) -- +1 day -1 tick
        , EndDateExclusive    = DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))
    FROM (
        SELECT [Value]    = CONVERT(int, SUBSTRING(@DateCode, 2, LEN(@DateCode)-2))
            ,  Multiplier = IIF(LEFT(@DateCode, 1) = 'P', 2, 1)
    ) t
        CROSS APPLY (
            SELECT BeginDate = DATEADD(DAY, -((t.[Value] * t.Multiplier)-1), @SeedDate)
                ,  EndDate   = DATEADD(DAY, -(t.[Value] * (t.Multiplier-1)), @SeedDate)
        ) x
    WHERE  @DateCode LIKE '[LP][0-9]D'
        OR @DateCode LIKE '[LP][0-9][0-9]D'
        OR @DateCode LIKE '[LP][0-9][0-9][0-9]D';

    RETURN;
END;
GO

-- SELECT * FROM dbo.uf_DateCalc(NULL, NULL);
