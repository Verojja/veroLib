SET NOEXEC OFF;
/*
	This script will create the stored procedure for the Expenditure Dimension Data Refresh.
		
	Execution of the sproc this this script creates relies on data from tables in ClaimSearch_Prod:
		[ClaimSearch_Prod].dbo.MPV00200
		[ClaimSearch_Prod].dbo.MPV00202
*/

BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 275) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
*/
/************************************************************************************************************************************************/	
/******************************************************Objects Required for indipendent testing**************************************************/	
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--ROLLBACK TRANSACTION;
	--SET NOEXEC ON;
	--END
	--GO
	--CREATE SCHEMA DecisionNet
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--ROLLBACK TRANSACTION;
	--SET NOEXEC ON;
	--END
	--GO
	--CREATE TABLE DecisionNet.Expenditure
	--(
	--	invoiceNumber VARCHAR(22) NOT NULL,
	--	lineItemNumber INT NOT NULL,
	--	invoiceDate DATE NOT NULL,
	--	companySoldToCode CHAR(4) NOT NULL,
	--	officeSoldToCode CHAR(5) NULL,
	--	companyShippedToCode CHAR(4) NOT NULL,
	--	officeShippedToCode CHAR(5) NULL,
	--	productCode CHAR(9) NOT NULL,
	--	productTransactionTypeCode AS LEFT(productCode,4),
	--	productTransactionCode AS RIGHT(productCode,4),
	--	lineItemQuantity INT NOT NULL,
	--	lineItemUnitCost DECIMAL(17,2) NOT NULL,
	--	lineItemTax DECIMAL(17,2) NOT NULL,
	--	dateInserted DATE NOT NULL,
	--	CONSTRAINT PK_Expenditure_invoiceNumber_lineItemNumber
	--		PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber)
	--);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE NONCLUSTERED INDEX NIX_Expenditure_productTransactionCode
	--	ON DecisionNet.Expenditure (productTransactionCode)
	--		INCLUDE (companySoldToCode, companyShippedToCode, lineItemUnitCost);
	--CREATE NONCLUSTERED INDEX NIX_Expenditure_invoiceDate
	--	ON DecisionNet.Expenditure (invoiceDate)
	--		INCLUDE (productCode, productTransactionTypeCode, productTransactionCode);
	--CREATE NONCLUSTERED INDEX NIX_Expenditure_companySoldToCode
	--	ON DecisionNet.Expenditure (companySoldToCode)
	--		INCLUDE (officeSoldToCode);
	--CREATE NONCLUSTERED INDEX NIX_Expenditure_companyShippedToCode
	--	ON DecisionNet.Expenditure (companyShippedToCode)
	--		INCLUDE (officeShippedToCode);
/************************************************************************************************************************************************/	
/************************************************************************************************************************************************/	
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
			Execution = 1.5 min against Expenditure Table with ONLY PK/ClusteredIndex
			Execution = 2.5 - 3.5 min against Expenditure Table with additional NIX(s)
				Worth testing whether or not the the added NIX improves seek magnitudinally.
				Also worth testing whether or not drop/re-create is viable.
************************************************/
CREATE PROCEDURE DecisionNet.hsp_UpdateInsertExpenditure
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	CREATE TABLE #IndexedTempMPVData
	(
		invoiceNumber VARCHAR(22) NOT NULL,
		lineItemNumber INT NOT NULL,
		invoiceDate DATE NOT NULL,
		companySoldToCode CHAR(4) NOT NULL,
		officeSoldToCode CHAR(5) NOT NULL,
		companyShippedToCode CHAR(4) NOT NULL,
		officeShippedToCode CHAR(5) NOT NULL,
		productCode CHAR(9) NOT NULL,
		lineItemQuantity INT NOT NULL,
		lineItemUnitCost decimal(17,2) NOT NULL,
		lineItemTax decimal(17,2) NOT NULL,
		dateInserted DATE NOT NULL,
		CONSTRAINT PK_IndexedTempMPVData_invoiceNumber_lineItemNumber
			PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber)
	);
	INSERT INTO #IndexedTempMPVData
	(
		invoiceNumber,
		lineItemNumber,
		invoiceDate,
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
	SELECT DISTINCT /*There are Currently(20180730) 14,493 EXACT DUPLICATE datarows in MPV00202*/
		MPV00202.N_INV_NO AS invoiceNumber,
		MPV00202.N_INV_LN_ITM AS lineItemNumber,
		MPV00200.invoiceDate AS invoiceDate,
		SUBSTRING(MPV00202.I_CST_SOLD,1,4) AS companySoldToCode,
		SUBSTRING(MPV00202.I_CST_SOLD,5,5) AS officeSoldToCode,
		/*consider including more granular data
				office_sold.[MCITY] as I_REGOFF_SOLD_CITY,
				office_sold.[MST] AS I_REGOFF_SOLD_STATE,
				office_sold.[MZIP] AS I_REGOFF_SOLD_ZIP,
			*/
		SUBSTRING(MPV00202.I_CST_SHP,1,4) AS companyShippedToCode,
		SUBSTRING(MPV00202.I_CST_SHP,5,5) AS officeShippedToCode,
		/*consider including more granular data
				office_shipped.[MCITY] as I_REGOFF_SHIPPED_CITY,
				office_shipped.[MST] AS I_REGOFF_SHIPPED_STATE,
				office_shipped.[MZIP] AS I_REGOFF_SHIPPED_ZIP,
			*/
		MPV00202.I_PRD AS productCode,
		MPV00202.A_LN_ITM_QTY AS lineItemQuantity,
		MPV00202.A_LN_ITM_UNIT AS lineItemUnitCost,
		MPV00202.A_LN_ITM_TAX AS lineItemTax,
		/*If we need the date value from MPV202
		CAST(
			SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,1,10)
			+ ' '
			+ REPLACE((SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
			+ (SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,20,8))
			AS DATETIME2(5)
		) AS dateLineItemExractRun,*/
		@dateInserted
	FROM
		[ClaimSearch_Prod].dbo.MPV00202
		CROSS APPLY
		(
			SELECT TOP(1) /*It appears as though the relationship between MPV00200.D_INV_DT AND N_INV_NO is 1:1
							and performance of an undordered top(1) seems  marginarlly better (in elapsed time) than cross applied rankfunction*/
				MPV00200.D_INV_DT AS invoiceDate
			FROM
				[ClaimSearch_Prod].dbo.MPV00200
			WHERE
				MPV00200.N_INV_NO = MPV00202.N_INV_NO
		) AS MPV00200;
		
	MERGE INTO DecisionNet.Expenditure AS TARGET
	USING
	(
		SELECT
			#IndexedTempMPVData.invoiceNumber,
			#IndexedTempMPVData.lineItemNumber,
			#IndexedTempMPVData.invoiceDate,
			#IndexedTempMPVData.companySoldToCode,
			#IndexedTempMPVData.officeSoldToCode,
			#IndexedTempMPVData.companyShippedToCode,
			#IndexedTempMPVData.officeShippedToCode,
			#IndexedTempMPVData.productCode,
			#IndexedTempMPVData.lineItemQuantity,
			#IndexedTempMPVData.lineItemUnitCost,
			#IndexedTempMPVData.lineItemTax,
			#IndexedTempMPVData.dateInserted
		FROM
			#IndexedTempMPVData
	) AS SOURCE
		ON TARGET.invoiceNumber = SOURCE.invoiceNumber
		AND TARGET.lineItemNumber = SOURCE.lineItemNumber
	WHEN MATCHED
		AND
		(
			TARGET.invoiceDate <> SOURCE.invoiceDate
			OR TARGET.companySoldToCode <> SOURCE.companySoldToCode
			OR TARGET.officeSoldToCode <> SOURCE.officeSoldToCode
			OR TARGET.companyShippedToCode <> SOURCE.companyShippedToCode
			OR TARGET.officeShippedToCode <> SOURCE.officeShippedToCode
			OR TARGET.productCode <> SOURCE.productCode
			OR TARGET.lineItemQuantity <> SOURCE.lineItemQuantity
			OR TARGET.lineItemUnitCost <> SOURCE.lineItemUnitCost
			OR TARGET.lineItemTax <> SOURCE.lineItemTax
			/*Dont update if the only difference is the dateInserted
				OR TARGET.dateInserted <> SOURCE.dateInserted
			*/
		)
	THEN UPDATE
	SET
		TARGET.invoiceDate = SOURCE.invoiceDate,
		TARGET.companySoldToCode = SOURCE.companySoldToCode,
		TARGET.officeSoldToCode = SOURCE.officeSoldToCode,
		TARGET.companyShippedToCode = SOURCE.companyShippedToCode,
		TARGET.officeShippedToCode = SOURCE.officeShippedToCode,
		TARGET.productCode = SOURCE.productCode,
		TARGET.lineItemQuantity = SOURCE.lineItemQuantity,
		TARGET.lineItemUnitCost = SOURCE.lineItemUnitCost,
		TARGET.lineItemTax = SOURCE.lineItemTax,
		TARGET.dateInserted = SOURCE.dateInserted
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
	(
		invoiceNumber,
		lineItemNumber,
		invoiceDate,
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
	VALUES
	(
		SOURCE.invoiceNumber,
		SOURCE.lineItemNumber,
		SOURCE.invoiceDate,
		SOURCE.companySoldToCode,
		SOURCE.officeSoldToCode,
		SOURCE.companyShippedToCode,
		SOURCE.officeShippedToCode,
		SOURCE.productCode,
		SOURCE.lineItemQuantity,
		SOURCE.lineItemUnitCost,
		SOURCE.lineItemTax,
		SOURCE.dateInserted
	);
	/*
	WHEN NOT MATCHED BY SOURCE
	THEN
		Do NOT Remove from Target.
		DO NOTHING.
	*/
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
	How are permissions for the Execution of this sproc being controlled?
*/
--EXEC DecisionNet.hsp_UpdateInsertExpenditure;
--SELECT * FROM DecisionNet.Expenditure;

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/