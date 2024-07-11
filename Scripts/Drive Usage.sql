DECLARE @barwidth int = 50;
SELECT x.Volume, x.Mount
    , Capacity  = RIGHT('   ' + FORMAT(x.Capacity  / POWER(1024.0, l.CapacityLog) , 'N2'), 7) + ' ' + CHOOSE(l.CapacityLog , 'KB', 'MB', 'GB', 'TB')
    , UsedSpace = RIGHT('   ' + FORMAT(x.UsedSpace / POWER(1024.0, l.UsedSpaceLog), 'N2'), 7) + ' ' + CHOOSE(l.UsedSpaceLog, 'KB', 'MB', 'GB', 'TB')
    , FreeSpace = RIGHT('   ' + FORMAT(x.FreeSpace / POWER(1024.0, l.FreeSpaceLog), 'N2'), 7) + ' ' + CHOOSE(l.FreeSpaceLog, 'KB', 'MB', 'GB', 'TB')
    , PctUsed   = RIGHT('   ' + FORMAT(1 - x.PctFree, 'P1'), 7)
    , PctFree   = RIGHT('   ' + FORMAT(x.PctFree    , 'P1'), 7)
    , Chart     = REPLICATE(N'█', CONVERT(int, FLOOR((1 - x.PctFree) * @barwidth))) + REPLICATE(N'▒', CONVERT(int, CEILING(x.PctFree * @barwidth)))
FROM (
    SELECT Volume   = vs.logical_volume_name
        , Mount     = vs.volume_mount_point
        , Capacity  = MAX(vs.total_bytes)
        , UsedSpace = MAX(vs.total_bytes) - MAX(vs.available_bytes)
        , FreeSpace = MAX(vs.available_bytes)
        , PctFree   = MAX(vs.available_bytes) / (MAX(vs.total_bytes) * 1.0)
    FROM sys.master_files mf
        CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) vs
    GROUP BY vs.volume_mount_point, vs.logical_volume_name
) x
    CROSS APPLY (
        SELECT CapacityLog  = FLOOR(LOG(x.Capacity , 1024))
            ,  UsedSpaceLog = FLOOR(LOG(x.UsedSpace, 1024))
            ,  FreeSpaceLog = FLOOR(LOG(x.FreeSpace, 1024))
    ) l
ORDER BY x.Volume;

/* Sample output

| Volume   | Mount          | Capacity   | UsedSpace  | FreeSpace  | PctUsed | PctFree | Chart                                              | 
|----------|----------------|------------|------------|------------|---------|---------|----------------------------------------------------| 
| DATA01   | D:\\DATA01\\   |   97.66 TB |   62.01 TB |   35.65 TB |  63.5 % |  36.5 % | ███████████████████████████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| DATA02   | D:\\DATA02\\   |  117.66 TB |   58.09 TB |   59.56 TB |  49.4 % |  50.6 % | ████████████████████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| DATA03   | D:\\DATA03\\   |   97.66 TB |   22.94 TB |   74.71 TB |  23.5 % |  76.5 % | ███████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| DATA04   | D:\\DATA04\\   |  102.54 TB |   51.29 TB |   51.25 TB |  50.0 % |  50.0 % | █████████████████████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| DATA05   | D:\\DATA05\\   |   48.83 TB |    4.52 TB |   44.31 TB |   9.3 % |  90.7 % | ████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| LOGS01   | D:\\LOGS01\\   |   15.00 TB |    2.62 TB |   12.38 TB |  17.5 % |  82.5 % | ████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| LOGS02   | D:\\LOGS02\\   |   15.00 TB |    4.35 TB |   10.65 TB |  29.0 % |  71.0 % | ██████████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| LOGS03   | D:\\LOGS03\\   |   15.00 TB |  238.94 GB |   14.77 TB |   1.6 % |  98.4 % | ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| LOGS04   | D:\\LOGS04\\   |   15.00 TB |  502.47 GB |   14.51 TB |   3.3 % |  96.7 % | █▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| LOGS05   | D:\\LOGS05\\   |   14.65 TB |  168.61 GB |   14.48 TB |   1.1 % |  98.9 % | ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ | 
| TEMPDB01 | D:\\TEMPDB01\\ |   99.87 GB |   87.24 GB |   12.63 GB |  87.4 % |  12.6 % | ███████████████████████████████████████████▒▒▒▒▒▒▒ | 

*/
