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
Date: 2018-07-23
Author: Robert David Warner
Description: Mechanism for data-refresh of the Expenditure Table.
			Originally, the table was being droppped,re-created, reinserted into.
			Current behavior now is Upsert, through Merge syntax.
			
			Performance:
			  Refactored to use DuplicateDataSetPerformanceHack WindofFunction
				vs. DISTINCT. for slightly improved performance.
			  Changed to SELECT INTO for logminimization and droped index from
				temp table (no reason than observed slight -and unintuative- performance increase).
************************************************/
ALTER PROCEDURE DecisionNet.hsp_UpdateInsertExpenditure
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));
	
	SELECT /*There are Currently(20180730) 14,493 EXACT DUPLICATE datarows in MPV00202*/
		MPV00202.N_INV_NO AS invoiceNumber,
		MPV00202.N_INV_LN_ITM AS lineItemNumber,
		MPV00200.invoiceDate AS invoiceDate,
		CAST(
			SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,1,10)
			+ ' '
			+ REPLACE((SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
			+ (SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,20,8))
			AS DATETIME2(5)
		) AS invoiceExecutionDate,
		CAST(SUBSTRING(MPV00202.I_CST_SOLD,1,4) AS CHAR(4)) AS companySoldToCode,
		CAST(SUBSTRING(MPV00202.I_CST_SOLD,5,5) AS CHAR(5)) AS officeSoldToCode,
		/*consider including more granular data
				office_sold.[MCITY] as I_REGOFF_SOLD_CITY,
				office_sold.[MST] AS I_REGOFF_SOLD_STATE,
				office_sold.[MZIP] AS I_REGOFF_SOLD_ZIP,
			*/
		CAST(SUBSTRING(MPV00202.I_CST_SHP,1,4) AS CHAR(4)) AS companyShippedToCode,
		CAST(SUBSTRING(MPV00202.I_CST_SHP,5,5) AS CHAR(5)) AS officeShippedToCode,
		/*consider including more granular data
				office_shipped.[MCITY] as I_REGOFF_SHIPPED_CITY,
				office_shipped.[MST] AS I_REGOFF_SHIPPED_STATE,
				office_shipped.[MZIP] AS I_REGOFF_SHIPPED_ZIP,
			*/
		CAST(MPV00202.I_PRD AS CHAR(9)) AS productCode,
		MPV00202.A_LN_ITM_QTY AS lineItemQuantity,
		MPV00202.A_LN_ITM_UNIT AS lineItemUnitCost,
		MPV00202.A_LN_ITM_TAX AS lineItemTax,
		@dateInserted AS dateInserted
		INTO #TempMPVData
	FROM
		(
			SELECT
				DuplicateDataSetPerformanceHackCLT00200.N_INV_NO, DuplicateDataSetPerformanceHackCLT00200.N_INV_LN_ITM,
				DuplicateDataSetPerformanceHackCLT00200.I_CST_SOLD, DuplicateDataSetPerformanceHackCLT00200.I_CST_SHP, DuplicateDataSetPerformanceHackCLT00200.I_LN_ITM_USR, DuplicateDataSetPerformanceHackCLT00200.I_PRD, DuplicateDataSetPerformanceHackCLT00200.T_PRD_DSC, DuplicateDataSetPerformanceHackCLT00200.A_LN_ITM_QTY,
				DuplicateDataSetPerformanceHackCLT00200.A_LN_ITM_UNIT, DuplicateDataSetPerformanceHackCLT00200.A_LN_ITM_EXTN, DuplicateDataSetPerformanceHackCLT00200.A_LN_ITM_NET_EXTN ,DuplicateDataSetPerformanceHackCLT00200.A_LN_ITM_TAX ,DuplicateDataSetPerformanceHackCLT00200.D_LN_ITM_EXRCT_RUN,
				DuplicateDataSetPerformanceHackCLT00200.F_LN_ITM_SLS_TAXB, DuplicateDataSetPerformanceHackCLT00200.I_UB_ORD_NO, DuplicateDataSetPerformanceHackCLT00200.C_PRD_TYP, DuplicateDataSetPerformanceHackCLT00200.C_PRD_ST, DuplicateDataSetPerformanceHackCLT00200.X, DuplicateDataSetPerformanceHackCLT00200.Date_Insert
			FROM
				(/*Notes on DuplicateDataSetPerformanceHack: CLT00200 contains some (small) number of duplicate records where the only delta is the row#.
					performance of rowNumber/partition is noticeably better than using DISTINCT*/
					SELECT
						INNERMPV00202.N_INV_NO , INNERMPV00202.N_INV_LN_ITM,
						ROW_NUMBER() OVER(
							PARTITION BY INNERMPV00202.N_INV_NO , INNERMPV00202.N_INV_LN_ITM
								ORDER BY INNERMPV00202.MPV202ROWID /*not really relevant what we sort by, since rows are identical*/
						) AS uniqueInstanceValue,
						INNERMPV00202.I_CST_SOLD, INNERMPV00202.I_CST_SHP, INNERMPV00202.I_LN_ITM_USR, INNERMPV00202.I_PRD, INNERMPV00202.T_PRD_DSC, INNERMPV00202.A_LN_ITM_QTY,
						INNERMPV00202.A_LN_ITM_UNIT, INNERMPV00202.A_LN_ITM_EXTN, INNERMPV00202.A_LN_ITM_NET_EXTN ,INNERMPV00202.A_LN_ITM_TAX ,INNERMPV00202.D_LN_ITM_EXRCT_RUN,
						INNERMPV00202.F_LN_ITM_SLS_TAXB, INNERMPV00202.I_UB_ORD_NO, INNERMPV00202.C_PRD_TYP, INNERMPV00202.C_PRD_ST, INNERMPV00202.X, INNERMPV00202.Date_Insert
					FROM
						[ClaimSearch_Prod].dbo.MPV00202 AS INNERMPV00202 WITH (NOLOCK)
					WHERE
						CAST(CAST(INNERMPV00202.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
				) AS DuplicateDataSetPerformanceHackCLT00200
			WHERE
				DuplicateDataSetPerformanceHackCLT00200.uniqueInstanceValue = 1
		) AS MPV00202
		CROSS APPLY
		(
			SELECT TOP(1) /*It appears as though the relationship between MPV00200.D_INV_DT AND N_INV_NO is 1:1
							and performance of an undordered top(1) seems  marginarlly better (in elapsed time) than cross applied rankfunction*/
				MPV00200.D_INV_DT AS invoiceDate
			FROM
				[ClaimSearch_Prod].dbo.MPV00200 WITH (NOLOCK)
			WHERE
				MPV00200.N_INV_NO = MPV00202.N_INV_NO
				AND MPV00200.D_INV_DT > (CAST((CAST((YEAR(GETDATE())-4) AS CHAR(4)) + '0101') AS DATE))
		) AS MPV00200;
	
	/*TODO write update based on match case*/
	UPDATE DecisionNet.Expenditure
	SET
		Expenditure.invoiceDate = SOURCE.invoiceDate,
		Expenditure.invoiceExecutionDate = SOURCE.invoiceExecutionDate,
		Expenditure.companySoldToCode = SOURCE.companySoldToCode,
		Expenditure.officeSoldToCode = SOURCE.officeSoldToCode,
		Expenditure.companyShippedToCode = SOURCE.companyShippedToCode,
		Expenditure.officeShippedToCode = SOURCE.officeShippedToCode,
		Expenditure.productCode = SOURCE.productCode,
		Expenditure.lineItemQuantity = SOURCE.lineItemQuantity,
		Expenditure.lineItemUnitCost = SOURCE.lineItemUnitCost,
		Expenditure.lineItemTax = SOURCE.lineItemTax,
		Expenditure.dateInserted = SOURCE.dateInserted
	FROM
		#TempMPVData AS SOURCE
	WHERE
		Expenditure.invoiceNumber = SOURCE.invoiceNumber
		AND Expenditure.lineItemNumber = SOURCE.lineItemNumber
		AND
		(
			Expenditure.invoiceDate <> SOURCE.invoiceDate
			OR Expenditure.invoiceExecutionDate <> SOURCE.invoiceExecutionDate
			OR Expenditure.companySoldToCode <> SOURCE.companySoldToCode
			OR ISNULL(Expenditure.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			OR Expenditure.companyShippedToCode <> SOURCE.companyShippedToCode
			OR ISNULL(Expenditure.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
			OR Expenditure.productCode <> SOURCE.productCode
			OR Expenditure.lineItemQuantity <> SOURCE.lineItemQuantity
			OR Expenditure.lineItemUnitCost <> SOURCE.lineItemUnitCost
			OR Expenditure.lineItemTax <> SOURCE.lineItemTax
			/*Dont update if the only difference is the dateInserted
				OR TARGET.dateInserted <> SOURCE.dateInserted
			*/
		);
		
	/*TODO write insert based on set based exlcusion logic*/
	ALTER TABLE DecisionNet.Expenditure
		DROP CONSTRAINT PK_Expenditure_invoiceNumber_lineItemNumber;
	
	INSERT INTO DecisionNet.Expenditure
	(
		invoiceNumber,
		lineItemNumber,
		invoiceDate,
		invoiceExecutionDate,
		companySoldToCode,
		officeSoldToCode,
		companyShippedToCode,
		officeShippedToCode,
		productCode,
		lineItemQuantity,
		lineItemUnitCost,
		lineItemTax,
		dateInserted
	)
	SELECT
		SOURCE.invoiceNumber,
		SOURCE.lineItemNumber,
		SOURCE.invoiceDate,
		SOURCE.invoiceExecutionDate,
		SOURCE.companySoldToCode,
		SOURCE.officeSoldToCode,
		SOURCE.companyShippedToCode,
		SOURCE.officeShippedToCode,
		SOURCE.productCode,
		SOURCE.lineItemQuantity,
		SOURCE.lineItemUnitCost,
		SOURCE.lineItemTax,
		SOURCE.dateInserted
	FROM
		#TempMPVData AS SOURCE
	EXCEPT
	SELECT
		Expenditure.invoiceNumber,
		Expenditure.lineItemNumber,
		Expenditure.invoiceDate,
		Expenditure.invoiceExecutionDate,
		Expenditure.companySoldToCode,
		Expenditure.officeSoldToCode,
		Expenditure.companyShippedToCode,
		Expenditure.officeShippedToCode,
		Expenditure.productCode,
		Expenditure.lineItemQuantity,
		Expenditure.lineItemUnitCost,
		Expenditure.lineItemTax,
		@dateInserted
	FROM
		DecisionNet.Expenditure;
		
	ALTER TABLE DecisionNet.Expenditure
		ADD CONSTRAINT PK_Expenditure_invoiceNumber_lineItemNumber
			PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber);
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

SELECT COUNT(*) FROM DecisionNet.Expenditure;
TRUNCATE TABLE DecisionNet.Expenditure;

EXEC DecisionNet.hsp_UpdateInsertExpenditure
	@dateFilterParam =  '20140101';

SELECT COUNT(*)
FROM DecisionNet.Expenditure;

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*

*/