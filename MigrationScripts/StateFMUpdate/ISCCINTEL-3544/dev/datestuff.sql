--'dbo.CLT00001'
--SELECT TOP 10
--	Date_Insert
--FROM
--dbo.CLT00001

DECLARE @somedateFilter DATE = GETDATE(),
		@somedateASINT INT = 20200817;
/*
20190817
20190328
20190411
20190328
20191021
20190329
20190328
20190331
20190328
20190328
1234567890
*/
SELECT
	@somedateFilter AS somedateFilter,
	@somedateASINT,
	CASE
		WHEN
			@somedateFilter > CAST(CAST(@somedateASINT AS VARCHAR(8)) AS DATE)
		THEN
			'@somedateASINT is smaller'
		ELSE
			'else'
	END AS test