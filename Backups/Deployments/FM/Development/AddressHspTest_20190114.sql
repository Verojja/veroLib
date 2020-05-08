SET NOEXEC OFF;
USE ClaimSearch_Dev

BEGIN TRANSACTION 

/**
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.Address')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.V_ActiveFMLocationOfLoss')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.V_ActiveFMNonLocationOfLoss')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
SELECT * FROM dbo.AddressActivityLog;
--**/

/*Test*/

DECLARE @paramone DATETIME2(2) = CAST('2013-08-01' AS DATETIME2(0));
DECLARE @paramTwo BIT = 1;

EXEC dbo.hsp_UpdateInsertFMAddress
--@paramone,@paramTwo;


--Select statement
/**
/*FastCounts & log section*/
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.Address')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.V_ActiveFMLocationOfLoss')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
SELECT
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name) AS qualifiedObjectName,
	CASE
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(9 AS BIGINT)
		THEN
			/*Observe how this pattern repeats.... there has to be a recursive way to code this... no time to do it right now.*/
			STUFF(
				CAST(
					STUFF(
						CAST(
							STUFF(
								CAST(
									SUM(partitions.rows) AS VARCHAR(30)
								),
								LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,','
							) AS VARCHAR(30)
						),
						LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,','
					) AS VARCHAR(30)
				),
				LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-8,0,','
			)
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(6 AS BIGINT)
		THEN
			STUFF(CAST(STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',') AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-5,0,',')
		WHEN
			LEN(CAST(SUM(partitions.rows) AS VARCHAR(30))) > CAST(3 AS BIGINT)
		THEN
			STUFF(CAST(SUM(partitions.rows) AS VARCHAR(30)),LEN(CAST(SUM(partitions.rows) AS VARCHAR(30)))-2,0,',')
		ELSE
			CAST(SUM(partitions.rows) AS VARCHAR(30))
	END numberOfRows
FROM
	sys.all_objects
	INNER JOIN sys.partitions
		ON all_objects.object_id = partitions.object_id
WHERE
	all_objects.object_id = OBJECT_ID('dbo.V_ActiveFMNonLocationOfLoss')
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)	
--**/
SELECT * FROM dbo.AddressActivityLog;

SELECT
	COUNT(*)
FROM
	dbo.Address
	
SELECT
	COUNT(*)
FROM
	dbo.V_ActiveFMLocationOfLoss

SELECT
	COUNT(*)
FROM
	dbo.V_ActiveFMNonLocationOfLoss

COMMIT TRANSACTION;