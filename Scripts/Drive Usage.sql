DECLARE @barwidth int = 50;
SELECT x.Volume, x.Mount
	, Capacity  = RIGHT('   ' + FORMAT(x.Capacity  / POWER(1024.0, l.CapacityLog) , 'N2'), 7) + ' ' + CHOOSE(l.CapacityLog , 'KB', 'MB', 'GB', 'TB')
	, FreeSpace = RIGHT('   ' + FORMAT(x.FreeSpace / POWER(1024.0, l.FreeSpaceLog), 'N2'), 7) + ' ' + CHOOSE(l.FreeSpaceLog, 'KB', 'MB', 'GB', 'TB')
	, PctUsed	= RIGHT('   ' + FORMAT(1 - x.PctFree, 'P1'), 7)
	, PctFree	= RIGHT('   ' + FORMAT(x.PctFree    , 'P1'), 7)
	, Chart		= REPLICATE(N'█', CONVERT(int, FLOOR((1 - x.PctFree) * @barwidth))) + REPLICATE(N'▒', CONVERT(int, CEILING(x.PctFree * @barwidth)))
FROM (
	SELECT Volume	= vs.logical_volume_name
		, Mount		= vs.volume_mount_point
		, Capacity	= AVG(vs.total_bytes)
		, FreeSpace	= AVG(vs.available_bytes)
		, PctFree	= AVG(vs.available_bytes) / (AVG(vs.total_bytes) * 1.0)
	FROM sys.master_files mf
		CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) vs
	GROUP BY vs.volume_mount_point, vs.logical_volume_name
) x
	CROSS APPLY (SELECT CapacityLog  = FLOOR(LOG(x.Capacity , 1024)),  FreeSpaceLog = FLOOR(LOG(x.FreeSpace, 1024))) l
ORDER BY x.Volume
