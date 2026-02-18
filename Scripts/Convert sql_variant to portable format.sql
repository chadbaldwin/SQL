/*
  Working with sql_variant in conjuction with .NET, bcp, etc can be very frustating because no matter what you do,
  something somewhere will get rounded, truncated or altered in some way. While not a perfect solution; This script
  is intended to be an attempt at converting sql_variant data into a portable format encoded as a string. Unfortunately
  it's not completely portable in the sense that you can infer the exact datatype, precision, scale and length. But,
  for the most part, it at least produces a lossless string representation of the original value.
*/
SELECT x.*, v.*
    , CASE 
        WHEN v.BaseType = 'date'                  THEN FORMAT(CONVERT(date, x.[value]), 'yyyy-MM-dd')
        WHEN v.BaseType = 'datetime'              THEN FORMAT(CONVERT(datetime, x.[value]), 'yyyy-MM-dd HH:mm:ss.fff')
        WHEN v.BaseType = 'smalldatetime'         THEN FORMAT(CONVERT(smalldatetime, x.[value]), 'yyyy-MM-dd HH:mm')
        WHEN v.BaseType = 'datetime2'             THEN LEFT(FORMAT(CONVERT(datetime2, x.[value]), 'o'), v.[Precision])
        WHEN v.BaseType = 'datetimeoffset'        THEN STUFF(FORMAT(CONVERT(datetimeoffset, x.[value]), 'o'), v.[Precision]-6, 34-v.[Precision], '')
        WHEN v.BaseType = 'time'                  THEN LEFT(CONVERT(nvarchar(16), x.[value], 114), v.[Precision])
        WHEN v.BaseType IN ('float','real')       THEN CONVERT(nvarchar(MAX), CONVERT(float, x.[value]), 3)
        WHEN v.BaseType = 'varbinary'             THEN CONVERT(nvarchar(MAX), CONVERT(varbinary(MAX), x.[value]), 1)
        WHEN v.BaseType = 'binary'                THEN CONVERT(nvarchar(MAX), CONVERT(varbinary(MAX), LEFT(CONVERT(varbinary(MAX), x.[value]), v.[MaxLength])), 1)
        WHEN v.BaseType IN ('money','smallmoney') THEN CONVERT(nvarchar(MAX), CONVERT(money, x.[value]), 2)
        ELSE CONVERT(nvarchar(MAX), x.[value]) -- Tested OK: bigint, int, smallint, tinyint, bit, decimal, numeric, char, nchar, varchar, nvarchar, xml, uniqueidentifier
      END
FROM (
              SELECT DT = 'bigint'           , [value] = CONVERT(sql_variant, CONVERT(bigint           , 1234567891011))
    UNION ALL SELECT DT = 'nvarchar(20)'     , [value] = CONVERT(sql_variant, CONVERT(nvarchar(20)     , N'asdf'))
    UNION All SELECT DT = 'varchar(20)'      , [value] = CONVERT(sql_variant, CONVERT(varchar(20)      , 'asdf'))
    UNION All SELECT DT = 'datetime'         , [value] = CONVERT(sql_variant, CONVERT(datetime         , GETDATE()))
    UNION All SELECT DT = 'datetime2'        , [value] = CONVERT(sql_variant, CONVERT(datetime2        , SYSUTCDATETIME()))
    UNION All SELECT DT = 'datetime2(3)'     , [value] = CONVERT(sql_variant, CONVERT(datetime2(3)     , SYSUTCDATETIME()))
    UNION All SELECT DT = 'datetime2(0)'     , [value] = CONVERT(sql_variant, CONVERT(datetime2(0)     , SYSUTCDATETIME()))
    UNION All SELECT DT = 'smalldatetime'    , [value] = CONVERT(sql_variant, CONVERT(smalldatetime    , SYSUTCDATETIME()))
    UNION All SELECT DT = 'datetimeoffset'   , [value] = CONVERT(sql_variant, CONVERT(datetimeoffset   , SYSDATETIMEOFFSET()))
    UNION All SELECT DT = 'datetimeoffset(3)', [value] = CONVERT(sql_variant, CONVERT(datetimeoffset(3), SYSDATETIMEOFFSET()))
    UNION All SELECT DT = 'datetimeoffset(0)', [value] = CONVERT(sql_variant, CONVERT(datetimeoffset(0), SYSDATETIMEOFFSET()))
    UNION ALL SELECT DT = 'date'             , [value] = CONVERT(sql_variant, CONVERT(date             , GETDATE()))
    UNION ALL SELECT DT = 'time'             , [value] = CONVERT(sql_variant, CONVERT(time             , GETDATE()))
    UNION ALL SELECT DT = 'time(3)'          , [value] = CONVERT(sql_variant, CONVERT(time(3)          , GETDATE()))
    UNION ALL SELECT DT = 'time(0)'          , [value] = CONVERT(sql_variant, CONVERT(time(0)          , GETDATE()))
    UNION ALL SELECT DT = 'bit'              , [value] = CONVERT(sql_variant, CONVERT(bit              , 1))
    UNION ALL SELECT DT = 'float'            , [value] = CONVERT(sql_variant, CONVERT(float            , 4.6789678967896789678967896789))
    UNION ALL SELECT DT = 'real'             , [value] = CONVERT(sql_variant, CONVERT(real             , 4.6789678967896789678967896789))
    UNION ALL SELECT DT = 'decimal(20,12)'   , [value] = CONVERT(sql_variant, CONVERT(decimal(20,12)   , 12345678.912345678912))
    UNION ALL SELECT DT = 'decimal(38,38)'   , [value] = CONVERT(sql_variant, CONVERT(decimal(38,38)   , 0.12345678912345678912345678912345678912))
    UNION ALL SELECT DT = 'varbinary(10)'    , [value] = CONVERT(sql_variant, CONVERT(varbinary(10)    , 'abcdefgh'))
    UNION ALL SELECT DT = 'binary(10)'       , [value] = CONVERT(sql_variant, CONVERT(binary(10)       , 'abcdefgh'))
    UNION ALL SELECT DT = 'char(10)'         , [value] = CONVERT(sql_variant, CONVERT(char(10)         , 'abcdefgh'))
    UNION ALL SELECT DT = 'money'            , [value] = CONVERT(sql_variant, CONVERT(money            , '$3.0011'))
    UNION ALL SELECT DT = 'smallmoney'       , [value] = CONVERT(sql_variant, CONVERT(smallmoney       , '$3.0011'))
    UNION ALL SELECT DT = 'uniqueidentifier' , [value] = CONVERT(sql_variant, CONVERT(uniqueidentifier , NEWID()))
) x
    CROSS APPLY (
        SELECT BaseType   = CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(x.[value], 'BaseType'))
            , [Precision] = CONVERT(int          , SQL_VARIANT_PROPERTY(x.[value], 'Precision'))
            , Scale       = CONVERT(int          , SQL_VARIANT_PROPERTY(x.[value], 'Scale'))
            , TotalBytes  = CONVERT(int          , SQL_VARIANT_PROPERTY(x.[value], 'TotalBytes'))
            , Collation   = CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(x.[value], 'Collation'))
            , [MaxLength] = CONVERT(int          , SQL_VARIANT_PROPERTY(x.[value], 'MaxLength'))
    ) v
