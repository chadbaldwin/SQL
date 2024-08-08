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
    SELECT @SeedDate    = COALESCE(@SeedDate, GETDATE());

    WITH cte_Dates AS (
        SELECT DiffYY   = DATEDIFF(yy, 0, @SeedDate)
            ,  DiffMM   = DATEDIFF(mm, 0, @SeedDate)
            ,  DiffWK   = DATEDIFF(wk, 0, @SeedDate)
            ,  DiffDD   = DATEDIFF(dd, 0, @SeedDate)
            ,  DiffQQ   = DATEDIFF(qq, 0, @SeedDate)
    )
    INSERT INTO @Return (Code, [Label], BeginDate, EndDateInclusiveDT, EndDateInclusiveDT2, EndDateExclusive)
    SELECT Code                 = x.Code
        , [Label]               = CONVERT(varchar(100), x.[Period])
        , BeginDate             = CONVERT(datetime2, x.BeginDate)
        , EndDateInclusiveDT    = DATEADD(MILLISECOND,  -3, DATEADD(DAY, 1, CONVERT(datetime , x.EndDate)))
        , EndDateInclusiveDT2   = DATEADD(NANOSECOND, -100, DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))) -- +1 day -1 tick
        , EndDateExclusive      = DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))
    FROM cte_Dates t
        CROSS APPLY (
            VALUES    ('CD'     , 'Current Day'                     , @SeedDate                         , @SeedDate                                     )
                    , ('CM'     , 'Current Month'                   , DATEADD(mm, t.DiffMM, 0)          , DATEADD(dd, -1, DATEADD(mm, t.DiffMM + 1, 0)) )
                    , ('CMTD'   , 'Current Month To Date'           , DATEADD(mm, t.DiffMM, 0)          , @SeedDate                                     )
                    , ('CQ'     , 'Current Quarter'                 , DATEADD(qq, t.DiffQQ, 0)          , DATEADD(dd, -1, DATEADD(qq, t.DiffQQ + 1, 0)) )
                    , ('CQTD'   , 'Current Quarter to Date'         , DATEADD(qq, t.DiffQQ, 0)          , @SeedDate                                     )
                    , ('CY'     , 'Current Year'                    , DATEADD(yy, t.DiffYY, 0)          , DATEADD(dd, -1, DATEADD(yy, t.DiffYY + 1, 0)) )
                    , ('CYTD'   , 'Current Year To Date'            , DATEADD(yy, t.DiffYY, 0)          , @SeedDate                                     )

                    , ('PD'     , 'Previous Day'                    , DATEADD(dd, -1, @SeedDate)        , DATEADD(dd, -1, @SeedDate)                    )
            --      , ('PW'     , 'Previous Week'                   , DATEADD(wk, y.DiffWK - 1, 0)      , DATEADD(dd, -1, DATEADD(wk, y.DiffWK, 0))     ) -- Removed temporarily - for some reason defaults to Monday as first day of week, despite DATEFIRST setting
                    , ('PM'     , 'Previous Month'                  , DATEADD(mm, t.DiffMM - 1, 0)      , DATEADD(dd, -1, DATEADD(mm, t.DiffMM, 0))     )
                    , ('PMTD'   , 'Previous Month to Date'          , DATEADD(mm, t.DiffMM - 1, 0)      , DATEADD(mm, -1, @SeedDate)                    )
                    , ('PQ'     , 'Previous Quarter'                , DATEADD(qq, t.DiffQQ - 1, 0)      , DATEADD(dd, -1, DATEADD(qq, t.DiffQQ, 0))     )
                    , ('PQTD'   , 'Previous Quarter to Date'        , DATEADD(qq, t.DiffQQ - 1, 0)      , DATEADD(qq, -1, @SeedDate)                    )
                    , ('PYQ'    , 'Previous Year Quarter'           , DATEADD(qq, t.DiffQQ - 4, 0)      , DATEADD(dd, -1, DATEADD(qq, t.DiffQQ - 3, 0)) )
                    , ('PYQTD'  , 'Previous Year Quarter to Date'   , DATEADD(qq, t.DiffQQ - 4, 0)      , DATEADD(yy, -1, @SeedDate)                    )
                    , ('PY'     , 'Previous Year'                   , DATEADD(yy, t.DiffYY - 1, 0)      , DATEADD(dd, -1, DATEADD(yy, t.DiffYY, 0))     )
                    , ('PYTD'   , 'Previous Year to Date'           , DATEADD(yy, t.DiffYY - 1, 0)      , DATEADD(yy, -1, @SeedDate)                    )
        ) x(Code, [Period], BeginDate, EndDate)
    WHERE x.Code = @DateCode OR @DateCode IS NULL
    UNION
    SELECT Code                 = UPPER(@DateCode)
        , [Label]               = CONCAT_WS(' ', CASE LEFT(@DateCode, 1) WHEN 'L' THEN 'Last' WHEN 'P' THEN 'Previous' ELSE NULL END, t.[Value], 'days')
        , BeginDate             = CONVERT(datetime2, x.BeginDate)
        , EndDateInclusiveDT    = DATEADD(MILLISECOND,  -3, DATEADD(DAY, 1, CONVERT(datetime , x.EndDate)))
        , EndDateInclusiveDT2   = DATEADD(NANOSECOND, -100, DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))) -- +1 day -1 tick
        , EndDateExclusive      = DATEADD(DAY, 1, CONVERT(datetime2, x.EndDate))
    FROM (
        SELECT [Value]          = CONVERT(int, SUBSTRING(@DateCode, 2, LEN(@DateCode)-2))
            ,  Multiplier       = IIF(LEFT(@DateCode, 1) = 'P', 2, 1)
    ) t
        CROSS APPLY (
            SELECT BeginDate    = DATEADD(DAY  , -((t.[Value] * t.Multiplier)-1), @SeedDate)
                ,  EndDate      = DATEADD(DAY  , -(t.[Value] * (t.Multiplier-1)), @SeedDate)
        ) x
    WHERE  @DateCode LIKE '[LP][0-9]D'
        OR @DateCode LIKE '[LP][0-9][0-9]D'
        OR @DateCode LIKE '[LP][0-9][0-9][0-9]D';

    RETURN;
END;
GO
