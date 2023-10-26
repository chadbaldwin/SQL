SELECT JobName		= j.[name]
	, Category		= c.[name]
	, [Owner]		= SUSER_SNAME(j.owner_sid)
	, [Enabled]		= IIF(j.[enabled] = 1, 'Yes', 'No')
	, Scheduled		= IIF(s.[enabled] = 1, 'Yes', 'No')
	, [Description]	= NULLIF(j.[description], 'No description available.')
	, Occurs		= CHOOSE(LOG(s.freq_type,2)+1,'Once', NULL, 'Daily', 'Weekly', 'Monthly', 'Monthly relative', 'When SQL Server Agent starts', 'Start whenever the CPU(s) become idle')
	, Occurs_detail	= CHOOSE(LOG(s.freq_type,2)+1
							, NULL
							, NULL
							, CONCAT('Every ', s.freq_interval, ' day(s)')																--"Every 10 day(s)"
							, CONCAT('Every ', s.freq_recurrence_factor, ' weeks(s) on '												--"EVery 3 week(s) on SMT_T_S"
									, IIF(s.freq_interval &  1 =  1, 'S', '_'), IIF(s.freq_interval &  2 =  2, 'M', '_')
									, IIF(s.freq_interval &  4 =  4, 'T', '_'), IIF(s.freq_interval &  8 =  8, 'W', '_')
									, IIF(s.freq_interval & 16 = 16, 'T', '_'), IIF(s.freq_interval & 32 = 32, 'F', '_')
									, IIF(s.freq_interval & 64 = 64, 'S', '_')
								)
							, CONCAT('Day ', s.freq_interval,' of every ',s.freq_recurrence_factor,' month(s)')							--"Day 5 of every 3 months(s)"
							, CONCAT('The '
									, CHOOSE(LOG(s.freq_relative_interval,2)+1, '1st','2nd','3rd','4th','Last'), ' '					--"The 1st"
									, CHOOSE(s.freq_interval, 'Sun','Mon','Tue','Wed','Thu','Fri','Sat','Day','Weekday','Weekend Day')	--"Sun"
									, ' of every ', s.freq_recurrence_factor, ' month(s)'												--"of every 3 months(s)"
								)
						)
	, Frequency		= 'Occurs ' + CHOOSE(LOG(NULLIF(s.freq_subday_type,0),2)+1															--"Occurs"
										, CONCAT('once at ', x.StartTime)																--"once at 04:00:00"
										, REPLACE(y.FrequencyString, '#interval#', 'Second')
										, REPLACE(y.FrequencyString, '#interval#', 'Minute')
										, REPLACE(y.FrequencyString, '#interval#', 'Hour')
									)
	, AvgDuration	= jh.AvgDuration
	, Next_Run_Date	= CONVERT(datetime, IIF(js.next_run_date = 0, '1900-01-01', CONVERT(char(8), js.next_run_date, 112) + ' ' + x.NextRunTime))
FROM msdb.dbo.sysjobs j
	LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
	LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
	JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
	LEFT JOIN (SELECT job_id, AvgDuration = AVG(DATEDIFF(ss, 0, CONVERT(time, STUFF(STUFF(RIGHT(CONCAT('000000', run_duration), 6), 5, 0, ':'), 3, 0, ':')))) FROM msdb.dbo.sysjobhistory WHERE step_id = 0 GROUP BY job_id) jh ON jh.job_id = j.job_id
	CROSS APPLY (SELECT StartTime	= STUFF(STUFF(RIGHT(CONCAT('000000', s.active_start_time), 6), 5, 0, ':'), 3, 0, ':')
					, EndTime		= STUFF(STUFF(RIGHT(CONCAT('000000', s.active_end_time	), 6), 5, 0, ':'), 3, 0, ':')
					, NextRunTime	= STUFF(STUFF(RIGHT(CONCAT('000000', js.next_run_time	), 6), 5, 0, ':'), 3, 0, ':')
				) x
	CROSS APPLY (SELECT FrequencyString = CONCAT('every ',s.freq_subday_interval,' #interval#(s) between ',x.StartTime,' and ',x.EndTime)) y --"every 4 #interval#(s) between 01:00:00 and 17:00:00"
ORDER BY j.[name]
