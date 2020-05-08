USE ClaimSearch

DECLARE
	@columnA VARCHAR(1000) = 'AddressKey',
	@columnB VARCHAR(1000) = 'ALLCLMROWID'
	/*Note: If using columnC to find 3-way-map you will need to uncomment lines 58 and 69*//*
	,@columnC VARCHAR(1000) = '';
	--*/

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
	EXISTS
	(
		SELECT NULL
		FROM
			INFORMATION_SCHEMA.COLUMNS AS ColumnA
			INNER JOIN INFORMATION_SCHEMA.COLUMNS AS ColumnB
				ON ColumnA.TABLE_SCHEMA = ColumnB.TABLE_SCHEMA
					AND ColumnA.TABLE_NAME = ColumnB.TABLE_NAME
					AND ColumnA.ORDINAL_POSITION <> ColumnB.ORDINAL_POSITION
			/*
			INNER JOIN INFORMATION_SCHEMA.COLUMNS AS ColumnC
				ON ColumnA.TABLE_SCHEMA = ColumnC.TABLE_SCHEMA
					AND ColumnA.TABLE_NAME = ColumnC.TABLE_NAME
					AND ColumnA.ORDINAL_POSITION <> ColumnC.ORDINAL_POSITION
					AND ColumnB.ORDINAL_POSITION <> ColumnC.ORDINAL_POSITION
			--*/
		WHERE
			all_objects.object_id = OBJECT_ID(CAST((ColumnA.TABLE_SCHEMA + '.' + ColumnA.TABLE_NAME) AS VARCHAR(1000)))
			AND ColumnA.COLUMN_NAME LIKE '%' + @columnA + '%'
			AND ColumnB.COLUMN_NAME LIKE '%' + @columnB + '%'
			/*
			AND ColumnC.COLUMN_NAME LIKE '%' + @columnC + '%'
			--*/
	)
GROUP BY
	(SCHEMA_NAME(all_objects.schema_id) + '.' + all_objects.name)
	