/*
	Query for finding active&blocking sessions in a SQL Sever Env.
	Useful to macro for quick verification that no critical jobs are being blocked etc.
*/
SELECT
	dm_exec_sessions.session_id,
	dm_os_waiting_tasks.blocking_session_id,
	dm_exec_sessions.open_transaction_count,
	dm_exec_sessions.status,
	dm_os_waiting_tasks.wait_type,
	CAST(CAST(dm_os_waiting_tasks.wait_duration_ms AS DECIMAL(18,4)) / CAST(1000.00 AS DECIMAL(18,4)) AS DECIMAL(18,4)) AS waitDurationInSeconds,
	dm_exec_sessions.original_login_name,
	SqlCode.queryWindowText,
	dm_exec_sessions.last_request_start_time,
	CAST(CAST(dm_exec_sessions.total_elapsed_time AS DECIMAL(18,4)) / CAST(1000.00 AS DECIMAL(18,4)) AS DECIMAL(18,4)) AS totalElapsedTimeInSeconds,
	dm_exec_sessions.cpu_time,
	dm_exec_sessions.memory_usage
FROM
	sys.dm_exec_sessions
	LEFT OUTER JOIN sys.dm_os_waiting_tasks
		ON dm_os_waiting_tasks.session_id = dm_exec_sessions.session_id
	LEFT OUTER JOIN sys.dm_exec_connections
		ON dm_exec_sessions.session_id = dm_exec_connections.session_id
	OUTER APPLY
	(
		SELECT
			dm_exec_sql_text.text AS queryWindowText
		FROM
			sys.dm_exec_sql_text (dm_exec_connections.most_recent_sql_handle)
	) AS SqlCode
ORDER BY
	CASE
		WHEN
			dm_os_waiting_tasks.blocking_session_id IS NOT NULL
		THEN
			0
		ELSE
			1
	END,
	CASE
		WHEN
			EXISTS(
				SELECT NULL
				FROM
					sys.dm_os_waiting_tasks AS INNERdm_os_waiting_tasks
				WHERE
					INNERdm_os_waiting_tasks.blocking_session_id = dm_exec_sessions.session_id
			)
		THEN
			0
		ELSE
			1
	END,
	session_id
