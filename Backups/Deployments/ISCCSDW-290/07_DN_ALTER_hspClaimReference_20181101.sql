SET NOEXEC OFF;

BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCSDW-290
Date: 2018-08-16
Author: Robert David Warner
Description: Mechanism for data-refresh of the Fact Table.
			Originally, the table was being droppped,re-created, reinserted into.
			Since the transactionDate value is derived from transactionId the sproc
			  could throw an error if transactionId had poluted data.
			  Consider TRY_CONVERT error handleing -- NOTE: TRY_CONVERT is a SQL2012 artifact.
			
			Performance: NOLOCK on CLT200,CLT201 and CLT220 risks dirty read.
						 TABLOCKX on target for insert to prevent more granular write lock (an attempt at table-compression / log-minimization)
						   however, we likely wont reap the benefits of this change while db recovery is set to "simple" instead of "bulk".
						   
						 Indexes on temp tables appeared to contribute to massively bloated logical read on insert (400,000,000 in worst case).
						 SELECT INTO vs. on temp table (when NO IDENTITY exists) contributes to log-minimization.
						 SET based operation on insert (IE: "INSERT ..FROM SOURCE EXCEPT TARGET") performs better when insert greater than 50,000,000.
						   and is comparable at all other levels.
************************************************/
ALTER PROCEDURE DecisionNet.hsp_UpdateInsertClaimReference
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	DECLARE @maxDateFilter DATE = CAST(CAST((YEAR(GETDATE())-4) AS CHAR(4)) +'0101' AS DATE) /*Replace with DateFromParts in higher versions of SQLServer*/;
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));

	SELECT
		CAST(DuplicateDataSetPerformanceHackCLT00200.I_TRNS AS CHAR(26)) AS transactionId,
		DuplicateDataSetPerformanceHackCLT00200.I_USR AS userId,
		DuplicateDataSetPerformanceHackCLT00200.T_USR_RFRNC AS claimReferenceNumber,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_SSSN)),'') AS VARCHAR(31)) AS iSSSNCode,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_CUST)),'') AS CHAR(4)) AS companySoldToCode,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_REGOFF)),'') AS CHAR(5)) AS officeSoldToCode,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_CUST_SHP_TO)),'') AS CHAR(4)) AS companyShippedToCode,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_REGOFF_SHP_TO)),'') AS CHAR(5)) AS officeShippedToCode,
		NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.C_RSLT_VEND)),'') AS rsltVendCode,
		NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_VEN_TRNS)),'') AS vendorTransactionID,
		NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00200.I_OTH_PRD)),'') AS otherProductCode
		INTO #IndexedTempCLT200Data
	FROM
		(/*Notes on DuplicateDataSetPerformanceHack: CLT00200 contains some (small) number of duplicate records where the only delta is the row#.
			performance of rowNumber/partition is noticeably better than using DISTINCT*/
			SELECT
				CLT00200.I_TRNS, CLT00200.I_USR, CLT00200.T_USR_RFRNC,
				ROW_NUMBER() OVER(
					PARTITION BY CLT00200.I_TRNS, CLT00200.I_USR, CLT00200.T_USR_RFRNC
						ORDER BY CLT00200.Date_Insert DESC
				) AS uniqueInstanceValue,
				CLT00200.I_SSSN, CLT00200.I_CUST, CLT00200.I_REGOFF, CLT00200.I_CUST_SHP_TO ,CLT00200.I_REGOFF_SHP_TO ,CLT00200.C_RSLT_VEND, CLT00200.I_VEN_TRNS, CLT00200.I_OTH_PRD
			FROM
				[ClaimSearch_PROD].dbo.CLT00200 WITH (NOLOCK)
			WHERE
				CAST(CAST(CLT00200.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
				/*Currently it appears as though trunc-and-reload is occuring for all rows of CLT00200.
					Adding an additional filter to exclude data past last4Years could improve performance
				*/
				AND CAST(LEFT(CLT00200.I_TRNS,10) AS DATE) >= @maxDateFilter
		) AS DuplicateDataSetPerformanceHackCLT00200
	WHERE
		DuplicateDataSetPerformanceHackCLT00200.uniqueInstanceValue = 1
	--OPTION (RECOMPILE);
	
	SELECT
		CAST(DuplicateDataSetPerformanceHackCLT00201.I_TRNS AS CHAR(26)) AS transactionId,
		DuplicateDataSetPerformanceHackCLT00201.I_USR AS userId,
		DuplicateDataSetPerformanceHackCLT00201.I_VEND AS vendorId,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00201.C_ISO_TRNS)),'') AS CHAR(4)) AS productTransactionCode,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00201.F_MTCH)),'') AS BIT) AS isMatched,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00201.F_BILL)),'') AS CHAR(1)) AS isBilled,
		NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00201.C_VEND_TRNS)),'') AS vendorTransactionCode,
		CASE
			WHEN
				LEN(DuplicateDataSetPerformanceHackCLT00201.D_BILL_RUN) = 25
			THEN
				CAST(
					SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_BILL_RUN,1,10)
					+ ' '
					+ REPLACE((SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_BILL_RUN,12,8)),'.',':')
					+ (SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_BILL_RUN,20,8))
					AS DATETIME2(5)
				)
			ELSE
				CAST(NULL AS DATETIME2(5))
		END AS dateBilledRun,
		CASE
			WHEN
				LEN(DuplicateDataSetPerformanceHackCLT00201.D_SRCH) = 26
			THEN
				CAST(
					SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_SRCH,1,10)
					+ ' '
					+ REPLACE((SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_SRCH,12,8)),'.',':')
					+ (SUBSTRING(DuplicateDataSetPerformanceHackCLT00201.D_SRCH,20,8))
					AS DATETIME2(6)
				)
			ELSE
				CAST(NULL AS DATETIME2(6))
		END AS dateSearched,
		DuplicateDataSetPerformanceHackCLT00201.uniqueInstanceValue
	INTO #IndexedTempCLT201Data
	FROM
		(/*Notes on DuplicateDataSetPerformanceHack: CLT00201 contains some number of duplicate records where the only delta is the row#.
			performance of rowNumber/partition is noticeably better than using DISTINCT*/
			SELECT
				CLT00201.I_TRNS, CLT00201.I_USR,
				ROW_NUMBER() OVER(
					PARTITION BY CLT00201.I_TRNS, CLT00201.I_USR
						ORDER BY CLT00201.Date_Insert DESC
				) AS uniqueInstanceValue,
				CLT00201.I_VEND, CLT00201.C_ISO_TRNS, CLT00201.F_MTCH, CLT00201.F_BILL, CLT00201.C_VEND_TRNS, CLT00201.D_BILL_RUN, CLT00201.D_SRCH
			FROM 
				[ClaimSearch_Prod].dbo.CLT00201 WITH (NOLOCK)
			WHERE
				CAST(CAST(CLT00201.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
				/*Currently it appears as though trunc-and-reload is occuring for all rows of CLT00201.
					Adding an additional filter to exclude data past last4Years could improve performance
				*/
				AND CAST(LEFT(CLT00201.I_TRNS,10) AS DATE) >= @maxDateFilter
		) AS DuplicateDataSetPerformanceHackCLT00201
	
	SELECT
		CAST(DuplicateDataSetPerformanceHackCLT00220.I_TRNS AS CHAR(26)) AS transactionId,
		DuplicateDataSetPerformanceHackCLT00220.I_USR AS userId,
		DuplicateDataSetPerformanceHackCLT00220.T_USR_RFRNC AS claimReferenceNumber,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00220.C_ORDR_STUS)),'') AS TINYINT) AS orderStatus,
		NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00220.C_RPT_TYP)),'') AS reportType,
		CAST(NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHackCLT00220.C_MTRO_STUS)),'') AS CHAR(1)) AS mtroStatusCode,
		CAST(DuplicateDataSetPerformanceHackCLT00220.A_ADDL_CHRG AS DECIMAL(5,2)) AS additionalCharge,
		CASE
			WHEN
				LEN(DuplicateDataSetPerformanceHackCLT00220.D_BILL) = 26
			THEN
				CAST(
					SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_BILL,1,10)
					+ ' '
					+ REPLACE((SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_BILL,12,8)),'.',':')
					+ (SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_BILL,20,8))
					AS DATETIME2(6)
				)
			ELSE
				CAST(NULL AS DATETIME2(6))
		END AS dateBilled,
		CASE
			WHEN
				LEN(DuplicateDataSetPerformanceHackCLT00220.D_DELETE) = 26
			THEN
				CAST(
					SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_DELETE,1,10)
					+ ' '
					+ REPLACE((SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_DELETE,12,8)),'.',':')
					+ (SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_DELETE,20,8))
					AS DATETIME2(6)
				)
			ELSE
				CAST(NULL AS DATETIME2(6))
		END AS dateDeleted,
		CASE
			WHEN
				LEN(DuplicateDataSetPerformanceHackCLT00220.D_FILL) = 26
			THEN
				CAST(
					SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_FILL,1,10)
					+ ' '
					+ REPLACE((SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_FILL,12,8)),'.',':')
					+ (SUBSTRING(DuplicateDataSetPerformanceHackCLT00220.D_FILL,20,8))
					AS DATETIME2(6)
				)
			ELSE
				CAST(NULL AS DATETIME2(6))
		END AS dateFilled
		INTO #IndexedTempCLT220Data
	FROM
		(/*Notes on DuplicateDataSetPerformanceHack: CLT00220 contains some number of duplicate records where the only delta is the row#.
			performance of rowNumber/partition is noticeably better than using DISTINCT*/
			SELECT
				CLT00220.I_TRNS,
				CLT00220.I_USR,
				CLT00220.T_USR_RFRNC,
				ROW_NUMBER() OVER(
					PARTITION BY CLT00220.I_TRNS, CLT00220.I_USR,CLT00220.T_USR_RFRNC
						ORDER BY CLT00220.Date_Insert DESC
				) AS uniqueInstanceValue,
				CLT00220.C_ORDR_STUS, CLT00220.C_RPT_TYP, CLT00220.C_MTRO_STUS, CLT00220.A_ADDL_CHRG,
				CLT00220.D_BILL, CLT00220.D_DELETE, CLT00220.D_FILL
			FROM 
				[ClaimSearch_Prod].dbo.CLT00220 WITH (NOLOCK)
			WHERE
				CAST(CAST(CLT00220.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
				/*Currently it appears as though trunc-and-reload is occuring for all rows of CLT00220.
					Adding an additional filter to exclude data past last4Years could improve performance
				*/
				AND CAST(LEFT(CLT00220.I_TRNS,10) AS DATE) >= @maxDateFilter
		) AS DuplicateDataSetPerformanceHackCLT00220
	WHERE
		DuplicateDataSetPerformanceHackCLT00220.uniqueInstanceValue = 1
	--OPTION (RECOMPILE);

	UPDATE DecisionNet.ClaimReference WITH (TABLOCKX)
		SET
			ClaimReference.iSSSNCode = SOURCE.iSSSNCode,
			ClaimReference.companySoldToCode = SOURCE.companySoldToCode,
			ClaimReference.officeSoldToCode = SOURCE.officeSoldToCode,
			ClaimReference.companyShippedToCode = SOURCE.companyShippedToCode,
			ClaimReference.officeShippedToCode = SOURCE.officeShippedToCode,
			ClaimReference.vendorId = SOURCE.vendorId,
			ClaimReference.rsltVendCode = SOURCE.rsltVendCode,
			ClaimReference.orderStatus = SOURCE.orderStatus,
			ClaimReference.reportType = SOURCE.reportType,
			ClaimReference.productTransactionCode = SOURCE.productTransactionCode,
			ClaimReference.VendorTransactionID = SOURCE.VendorTransactionID,
			ClaimReference.isMatched = SOURCE.isMatched,
			ClaimReference.isBilled = SOURCE.isBilled,
			ClaimReference.mtroStatusCode = SOURCE.mtroStatusCode,
			ClaimReference.vendorTransactionCode = SOURCE.vendorTransactionCode,
			ClaimReference.additionalCharge = SOURCE.additionalCharge,
			ClaimReference.otherProductCode = SOURCE.otherProductCode,
			ClaimReference.dateBilled = SOURCE.dateBilled,
			ClaimReference.dateBilledRun = SOURCE.dateBilledRun,
			ClaimReference.dateDeleted = SOURCE.dateDeleted,
			ClaimReference.dateSearched = SOURCE.dateSearched,
			ClaimReference.dateFilled = SOURCE.dateFilled,
			ClaimReference.dateInserted = SOURCE.dateInserted
	FROM
		(
			SELECT
				#IndexedTempCLT200Data.transactionId, #IndexedTempCLT200Data.userId, #IndexedTempCLT200Data.claimReferenceNumber, #IndexedTempCLT201Data.uniqueInstanceValue,
				#IndexedTempCLT200Data.iSSSNCode, #IndexedTempCLT200Data.companySoldToCode, #IndexedTempCLT200Data.officeSoldToCode, #IndexedTempCLT200Data.companyShippedToCode, #IndexedTempCLT200Data.officeShippedToCode, #IndexedTempCLT201Data.vendorId,
				#IndexedTempCLT200Data.rsltVendCode, #IndexedTempCLT220Data.orderStatus, #IndexedTempCLT220Data.reportType, #IndexedTempCLT201Data.productTransactionCode, #IndexedTempCLT200Data.VendorTransactionID, #IndexedTempCLT201Data.isMatched, #IndexedTempCLT201Data.isBilled, #IndexedTempCLT220Data.mtroStatusCode, #IndexedTempCLT201Data.vendorTransactionCode,
				#IndexedTempCLT220Data.additionalCharge, #IndexedTempCLT200Data.otherProductCode,
				#IndexedTempCLT220Data.dateBilled, #IndexedTempCLT201Data.dateBilledRun, #IndexedTempCLT220Data.dateDeleted, #IndexedTempCLT201Data.dateSearched, #IndexedTempCLT220Data.dateFilled, 
				@dateInserted AS dateInserted
			FROM
				#IndexedTempCLT200Data
				INNER JOIN #IndexedTempCLT201Data
					ON #IndexedTempCLT200Data.transactionId = #IndexedTempCLT201Data.transactionId
						AND #IndexedTempCLT200Data.userId = #IndexedTempCLT201Data.userId
				LEFT OUTER JOIN #IndexedTempCLT220Data
					ON #IndexedTempCLT200Data.transactionId = #IndexedTempCLT220Data.transactionId
					AND #IndexedTempCLT200Data.userId = #IndexedTempCLT220Data.userId
					/*Joining on T_USR_RFRNC vs. I_SSSN excludes 24 matches (20180816)*/
					AND #IndexedTempCLT200Data.claimReferenceNumber = #IndexedTempCLT220Data.claimReferenceNumber
		) AS SOURCE
	WHERE
		SOURCE.transactionId = ClaimReference.transactionId
		AND Source.userId = ClaimReference.userId
		AND Source.claimReferenceNumber = ClaimReference.claimReferenceNumber
		AND Source.uniqueInstanceValue = ClaimReference.uniqueInstanceValue
		AND 
		(
			ISNULL(ClaimReference.iSSSNCode,'') <> ISNULL(SOURCE.iSSSNCode,'')
			OR ISNULL(ClaimReference.companySoldToCode,'') <> ISNULL(SOURCE.companySoldToCode,'')
			OR ISNULL(ClaimReference.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			OR ISNULL(ClaimReference.companyShippedToCode,'') <> ISNULL(SOURCE.companyShippedToCode,'')
			OR ISNULL(ClaimReference.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
			OR ISNULL(ClaimReference.vendorId,'') <> ISNULL(SOURCE.vendorId,'')
			OR ISNULL(ClaimReference.rsltVendCode,'') <> ISNULL(SOURCE.rsltVendCode,'')
			OR ISNULL(ClaimReference.orderStatus,'') <> ISNULL(SOURCE.orderStatus,'')
			OR ISNULL(ClaimReference.reportType,'') <> ISNULL(SOURCE.reportType,'')
			OR ISNULL(ClaimReference.productTransactionCode,'') <> ISNULL(SOURCE.productTransactionCode,'')
			OR ISNULL(ClaimReference.VendorTransactionID,'') <> ISNULL(SOURCE.VendorTransactionID,'')
			OR ISNULL(ClaimReference.isMatched,'') <> ISNULL(SOURCE.isMatched,'')
			OR ISNULL(ClaimReference.isBilled,'') <> ISNULL(SOURCE.isBilled,'')
			OR ISNULL(ClaimReference.mtroStatusCode,'') <> ISNULL(SOURCE.mtroStatusCode,'')
			OR ISNULL(ClaimReference.vendorTransactionCode,'') <> ISNULL(SOURCE.vendorTransactionCode,'')
			OR ISNULL(ClaimReference.additionalCharge,-.01) <> ISNULL(SOURCE.additionalCharge,-.01)
			OR ISNULL(ClaimReference.otherProductCode,'') <> ISNULL(SOURCE.otherProductCode,'')
			OR ISNULL(ClaimReference.dateBilled,'') <> ISNULL(SOURCE.dateBilled,'')
			OR ISNULL(ClaimReference.dateBilledRun,'') <> ISNULL(SOURCE.dateBilledRun,'')
			OR ISNULL(ClaimReference.dateDeleted,'') <> ISNULL(SOURCE.dateDeleted,'')
			OR ISNULL(ClaimReference.dateSearched,'') <> ISNULL(SOURCE.dateSearched,'')
			OR ISNULL(ClaimReference.dateFilled,'') <> ISNULL(SOURCE.dateFilled,'')
		)
	--OPTION (RECOMPILE);
	
	INSERT INTO DecisionNet.ClaimReference WITH (TABLOCKX)
	(
		transactionId, userId, claimReferenceNumber, uniqueInstanceValue,
		iSSSNCode, companySoldToCode, officeSoldToCode, companyShippedToCode, officeShippedToCode, vendorId, rsltVendCode, orderStatus,
		reportType, productTransactionCode, VendorTransactionID, isMatched, isBilled, mtroStatusCode, vendorTransactionCode,
		additionalCharge, otherProductCode, dateBilled, dateBilledRun, dateDeleted, dateSearched, dateFilled,
		dateInserted
	)
	SELECT
		SOURCE.transactionId, SOURCE.userId, SOURCE.claimReferenceNumber, SOURCE.uniqueInstanceValue, SOURCE.iSSSNCode, SOURCE.companySoldToCode, SOURCE.officeSoldToCode, SOURCE.companyShippedToCode, SOURCE.officeShippedToCode, SOURCE.vendorId,
		SOURCE.rsltVendCode, SOURCE.orderStatus, SOURCE.reportType, SOURCE.productTransactionCode, SOURCE.VendorTransactionID, SOURCE.isMatched, SOURCE.isBilled, SOURCE.mtroStatusCode, SOURCE.vendorTransactionCode,
		SOURCE.additionalCharge, SOURCE.otherProductCode,
		SOURCE.dateBilled, SOURCE.dateBilledRun, SOURCE.dateDeleted, SOURCE.dateSearched, SOURCE.dateFilled, @dateInserted AS dateInserted 
	FROM		
		(
			SELECT
				#IndexedTempCLT200Data.transactionId, #IndexedTempCLT200Data.userId, #IndexedTempCLT200Data.claimReferenceNumber, #IndexedTempCLT201Data.uniqueInstanceValue, #IndexedTempCLT200Data.iSSSNCode, #IndexedTempCLT200Data.companySoldToCode, #IndexedTempCLT200Data.officeSoldToCode, #IndexedTempCLT200Data.companyShippedToCode, #IndexedTempCLT200Data.officeShippedToCode, #IndexedTempCLT201Data.vendorId,
				#IndexedTempCLT200Data.rsltVendCode, #IndexedTempCLT220Data.orderStatus, #IndexedTempCLT220Data.reportType, #IndexedTempCLT201Data.productTransactionCode, #IndexedTempCLT200Data.VendorTransactionID, #IndexedTempCLT201Data.isMatched, #IndexedTempCLT201Data.isBilled, #IndexedTempCLT220Data.mtroStatusCode, #IndexedTempCLT201Data.vendorTransactionCode,
				#IndexedTempCLT220Data.additionalCharge, #IndexedTempCLT200Data.otherProductCode,
				#IndexedTempCLT220Data.dateBilled, #IndexedTempCLT201Data.dateBilledRun, #IndexedTempCLT220Data.dateDeleted, #IndexedTempCLT201Data.dateSearched, #IndexedTempCLT220Data.dateFilled
			FROM
				#IndexedTempCLT200Data
				INNER JOIN #IndexedTempCLT201Data
					ON #IndexedTempCLT200Data.transactionId = #IndexedTempCLT201Data.transactionId
						AND #IndexedTempCLT200Data.userId = #IndexedTempCLT201Data.userId
				LEFT OUTER JOIN #IndexedTempCLT220Data
					ON #IndexedTempCLT200Data.transactionId = #IndexedTempCLT220Data.transactionId
					AND #IndexedTempCLT200Data.userId = #IndexedTempCLT220Data.userId
					/*Joining on T_USR_RFRNC vs. I_SSSN excludes 24 matches (20180816)*/
					AND #IndexedTempCLT200Data.claimReferenceNumber = #IndexedTempCLT220Data.claimReferenceNumber
		) AS SOURCE
		LEFT OUTER JOIN DecisionNet.ClaimReference
			ON SOURCE.transactionId = ClaimReference.transactionId
			AND Source.userId = ClaimReference.userId
			AND Source.claimReferenceNumber = ClaimReference.claimReferenceNumber
			AND Source.uniqueInstanceValue = ClaimReference.uniqueInstanceValue
	WHERE
		ClaimReference.transactionId IS NULL;
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

----SELECT COUNT(*) FROM DecisionNet.ClaimReference;
----TRUNCATE TABLE DecisionNet.ClaimReference;

----EXEC DecisionNet.hsp_UpdateInsertClaimReference
----	@dateFilterParam = '20140101';

--SELECT
--	NULL AS 'dateBilledRun IS NULL',
--	COUNT(*)
--FROM DecisionNet.ClaimReference
--WHERE
--	ClaimReference.dateBilledRun IS NULL

--SELECT
--	NULL AS 'didn''t bill or bill IS NULL',
--	COUNT(*)
--FROM DecisionNet.ClaimReference
--WHERE
--	ISNULL(ClaimReference.isBilled,'N') = 'N'
	
--EXEC DecisionNet.hsp_UpdateInsertClaimReference
--	@dateFilterParam = '20181107';
	
--SELECT
--	NULL AS 'dateBilledRun IS NULL',
--	COUNT(*)
--FROM DecisionNet.ClaimReference
--WHERE
--	ClaimReference.dateBilledRun IS NULL

--SELECT
--	NULL AS 'didn''t bill or bill IS NULL',
--	COUNT(*)
--FROM DecisionNet.ClaimReference
--WHERE
--	ISNULL(ClaimReference.isBilled,'N') = 'N'
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;


/*
SQL Server parse and compile time: 
   CPU time = 57 ms, elapsed time = 57 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.
Table 'CLT00200'. Scan count 65, logical reads 2055188, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 759689 ms,  elapsed time = 127122 ms.

(46835126 row(s) affected)
Table 'CLT00201'. Scan count 65, logical reads 542657, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 598105 ms,  elapsed time = 98303 ms.

(41935573 row(s) affected)
Table 'CLT00220'. Scan count 65, logical reads 964289, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 356699 ms,  elapsed time = 51919 ms.

(19433799 row(s) affected)
Table 'Raw_CLT0201N_2018'. Scan count 65, logical reads 218068, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 435249 ms,  elapsed time = 86201 ms.

(41334085 row(s) affected)
Table 'ClaimReference'. Scan count 1, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 1 ms.

(0 row(s) affected)
Table 'ClaimReference'. Scan count 1, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 1 ms.

(0 row(s) affected)
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT220Data______________________________________________________________________________________________000000004741'. Scan count 1, logical reads 216399, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT200Data______________________________________________________________________________________________00000000473D'. Scan count 1, logical reads 701374, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT201Data______________________________________________________________________________________________00000000473F'. Scan count 1, logical reads 476532, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'ClaimReference'. Scan count 1, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 610765 ms,  elapsed time = 623227 ms.

(41862559 row(s) affected)

 SQL Server Execution Times:
   CPU time = 2791727 ms,  elapsed time = 1020293 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.


(1 row(s) affected)

(1 row(s) affected)

(76160 row(s) affected)

(66523 row(s) affected)

(32739 row(s) affected)

(41334085 row(s) affected)

(3586154 row(s) affected)

(0 row(s) affected)

(0 row(s) affected)

(1 row(s) affected)

(1 row(s) affected)
ROLLBACK

SQL Server parse and compile time: 
   CPU time = 0 ms, elapsed time = 0 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.
Table 'CLT00200'. Scan count 65, logical reads 2060566, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 79064 ms,  elapsed time = 3148 ms.

(77118 row(s) affected)
Table 'CLT00201'. Scan count 65, logical reads 534028, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 62524 ms,  elapsed time = 1378 ms.

(69073 row(s) affected)
Table 'CLT00220'. Scan count 65, logical reads 14076216, physical reads 0, read-ahead reads 30, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 416889 ms,  elapsed time = 14575 ms.

(26918 row(s) affected)
Table 'ClaimReference'. Scan count 1, logical reads 1007957, physical reads 0, read-ahead reads 7, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT220Data______________________________________________________________________________________________000000006390'. Scan count 1, logical reads 20790973, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT200Data______________________________________________________________________________________________00000000638C'. Scan count 1, logical reads 1169, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT201Data______________________________________________________________________________________________00000000638D'. Scan count 1, logical reads 713, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 459562 ms,  elapsed time = 459742 ms.

(0 row(s) affected)
Table 'Worktable'. Scan count 1, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'ClaimReference'. Scan count 1, logical reads 1007957, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT200Data______________________________________________________________________________________________00000000638C'. Scan count 1, logical reads 1169, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT201Data______________________________________________________________________________________________00000000638D'. Scan count 1, logical reads 713, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#IndexedTempCLT220Data______________________________________________________________________________________________000000006390'. Scan count 1, logical reads 301, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 24343 ms,  elapsed time = 24338 ms.

(0 row(s) affected)

 SQL Server Execution Times:
   CPU time = 1139144 ms,  elapsed time = 530070 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.


*/