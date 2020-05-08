/*
	Query takes advantage of sys views to find an object (table or optionally view as well)
	that contains two columns (useful for finding Mapping tables).
*/
;WITH Constants (
		columnValueOne,
		columnValueTwo,
		columnValueThree
) AS (
	VALUES
		('%regoff%','%zip%','%_st_%')
)
SELECT DISTINCT
	ColumnOne.table_schema || '.' || ColumnOne.table_name AS qualifiedObjectName,
	Constants.columnValueOne || ':' || ColumnOne.COLUMN_NAME || '; ' ||
	Constants.columnValueTwo || ':' || ColumnTwo.COLUMN_NAME || '; ' ||
	Constants.columnValueThree || ':' || ColumnThree.COLUMN_NAME || '; '
FROM	
	INFORMATION_SCHEMA.COLUMNS AS ColumnOne
	INNER JOIN INFORMATION_SCHEMA.COLUMNS AS ColumnTwo
		ON ColumnOne.table_schema = ColumnTwo.table_schema
			AND ColumnOne.table_name = ColumnTwo.table_name
	INNER JOIN INFORMATION_SCHEMA.COLUMNS AS ColumnThree
		ON ColumnOne.table_schema = ColumnThree.table_schema
			AND ColumnOne.table_name = ColumnThree.table_name
	INNER JOIN Constants
		ON 1=1 /*Hack to include the "variables"*/
WHERE
	ColumnOne.COLUMN_NAME LIKE Constants.columnValueOne
	AND ColumnTwo.COLUMN_NAME LIKE Constants.columnValueTwo
	AND ColumnThree.COLUMN_NAME LIKE Constants.columnValueThree;
	
/*
	"natb.pyrtab"
*/
