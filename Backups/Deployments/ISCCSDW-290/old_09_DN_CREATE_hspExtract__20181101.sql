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
Description: Mechanism for data-refresh of the ExtractTable.
				Allows for only a single table to exist in the Vader environment
				and a more efficient trunct and reload to Vader from an up-to-date extract-table.
				
			Performance: No current notes.
************************************************/
ALTER PROCEDURE DecisionNet.hsp_UpdateInsertClaimReferenceExtract
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));
	SELECT
		ClaimReference.transactionId,
		ClaimReference.userId,
		ClaimReference.claimReferenceNumber,
		ClaimReference.uniqueInstanceValue,
		CAST(ClaimReference.transactionDate AS DATE) AS transactionDate,
		ClaimReference.companySoldToCode,
		ClaimReference.officeSoldToCode,
		ClaimReference.companyShippedToCode,
		ClaimReference.officeShippedToCode,
		Product.productTransactionCode,
		CAST(ClaimReference.dateBilledRun AS DATE) AS dateBilledRun,
		CAST(ClaimReference.dateFilled AS DATE) AS dateFilled,
		CASE
			WHEN
				Product.billableMatchCode IN ('I','Y')
				AND ClaimReference.isBilled = 'Y' 
			THEN
				CAST('Y' AS CHAR(1))
			ELSE
				CAST('N' AS CHAR(1))
		END AS originalBillFlag,
		CASE
			WHEN
				(
					ClaimReference.isMatched = 1
					AND Product.billableMatchCode IN ('I','Y')
				)
				OR
				(
					ClaimReference.isMatched = 0
					AND Product.billableNonMatchCode IN ('I','Y')
				)
			THEN
				CAST('Y' AS CHAR(1))
			ELSE
				CAST('N' AS CHAR(1))
		END AS alternateBillFlag,
		ClaimReference.isBilled,
		PYRTAB.NAME AS userName,
		PYRTAB.I_CUST AS userCustomerCode,
		PYRTAB.I_REGOFF AS userOfficeCode,
		Lookup_JobClass.Jobclass_Desc AS userJobClassDescription,
		PYRTAB.ADDR1 AS userAddressLine1,
		PYRTAB.CITY AS userCity,
		PYRTAB.STATE AS userState,
		PYRTAB.ZIPCODE AS userZipCode,
		CAST((Product.productHierarchyLvl1 + '/' + Product.productHierarchyLvl2 + '/' + Product.productHierarchyLvl3) AS VARCHAR(227)) AS concatendatedProductHierarchyLvl,
		Product.productTransactionDescription,
		Product.productGroupId,
		ProductGroup.productGroupName,
		Expenditure.itemQuantity AS lineItemQuantity,
		Expenditure.lineItemUnitCost AS lineItemCost,
		CAST(ROUND(Expenditure.itemTax / Expenditure.itemQuantity,2) AS DECIMAL(17,2)) AS unitTax,
		Expenditure.invoiceDate,
		Expenditure.invoiceNumber,
		Expenditure.productCode,
		ClaimReference.vendorId		
		INTO #TempClaimReferenceExtract
	FROM
		DecisionNet.ClaimReference WITH (NOLOCK)
		INNER JOIN DecisionNet.Product WITH (NOLOCK)
			ON Product.productTransactionCode = ClaimReference.productTransactionCode
		INNER JOIN DecisionNet.ProductGroup WITH (NOLOCK)
			ON ProductGroup.productGroupId = Product.productGroupId
		OUTER APPLY
		(
			SELECT
				RIGHT(InnerProduct.recentlyObservedProductCode,4) AS productTransactionCode
			FROM
				DecisionNet.Product AS InnerProduct WITH(NOLOCK)
			WHERE
				InnerProduct.productTransactionCode <> RIGHT(InnerProduct.recentlyObservedProductCode,4)
				AND InnerProduct.productTransactionCode = ClaimReference.ProductTransactionCode
		) AlternateProductCode
		LEFT OUTER JOIN
		(
			SELECT
				INNERExpenditure.invoiceNumber,
				/*INNERExpenditure.lineItemNumber,*/
				INNERExpenditure.invoiceDate,
				INNERExpenditure.invoiceExecutionDate,
				INNERExpenditure.companySoldToCode,
				INNERExpenditure.officeSoldToCode,
				INNERExpenditure.companyShippedToCode,
				INNERExpenditure.officeShippedToCode,
				INNERExpenditure.productCode,
				INNERExpenditure.productTransactionTypeCode,
				INNERExpenditure.productTransactionCode,
				SUM(INNERExpenditure.lineItemQuantity) AS itemQuantity,
				INNERExpenditure.lineItemUnitCost,
				SUM(INNERExpenditure.lineItemTax) AS itemTax,
				INNERExpenditure.dateInserted
			FROM
				DecisionNet.Expenditure AS INNERExpenditure WITH(NOLOCK)
			GROUP BY
				INNERExpenditure.invoiceNumber,
				INNERExpenditure.invoiceDate,
				INNERExpenditure.invoiceExecutionDate,
				INNERExpenditure.companySoldToCode,
				INNERExpenditure.officeSoldToCode,
				INNERExpenditure.companyShippedToCode,
				INNERExpenditure.officeShippedToCode,
				INNERExpenditure.productCode,
				INNERExpenditure.productTransactionTypeCode,
				INNERExpenditure.productTransactionCode,
				INNERExpenditure.lineItemUnitCost,
				INNERExpenditure.dateInserted
		) AS Expenditure
			ON Expenditure.companyShippedToCode = ClaimReference.companyShippedToCode
				AND Expenditure.officeShippedToCode = ClaimReference.officeShippedToCode
				AND Expenditure.invoiceExecutionDate = ClaimReference.dateBilledRun
				AND
				(
					Expenditure.productTransactionCode = ClaimReference.productTransactionCode
					OR
					(
						Expenditure.productTransactionCode = AlternateProductCode.productTransactionCode
						AND NOT EXISTS
						(
							SELECT NULL
							FROM
								DecisionNet.Expenditure AS INNERExpenditure WITH(NOLOCK)
								WHERE INNERExpenditure.productTransactionCode = ClaimReference.productTransactionCode
								AND  INNERExpenditure.companyShippedToCode = ClaimReference.companyShippedToCode
								AND INNERExpenditure.officeShippedToCode = ClaimReference.officeShippedToCode
								AND INNERExpenditure.invoiceExecutionDate = ClaimReference.dateBilledRun
						)
					)
				)
		LEFT OUTER JOIN [ClaimSearch_Prod].dbo.PYRTAB WITH (NOLOCK)
			ON PYRTAB.I_USR = ClaimReference.userId
		LEFT OUTER JOIN [ClaimSearch_Prod].dbo.Lookup_JobClass WITH (NOLOCK)
			ON Lookup_JobClass.Jobclass_Name = PYRTAB.JOBCLASS
	WHERE
		/*
			SQL Server Execution Times: (for a CROSS APPLY of MAX date between the three tables)
			CPU time = 1164672 ms,  elapsed time = 1170162 ms.
			SQL Server Execution Times: (for current implimentation: IE the following CASE-check)
			CPU time = 1023062 ms,  elapsed time = 1118084 ms.
		*/
		CASE
			WHEN
				ISNULL(ClaimReference.dateInserted,'00010101') > ISNULL(Product.dateInserted,'00010101')
			THEN
				CASE
					WHEN
						ISNULL(ClaimReference.dateInserted,'00010101') > ISNULL(Expenditure.dateInserted,'00010101')
					THEN
						ClaimReference.dateInserted
					ELSE
						Expenditure.dateInserted
				END
			ELSE
				CASE
					WHEN
						ISNULL(Product.dateInserted,'00010101') > ISNULL(Expenditure.dateInserted,'00010101')
					THEN
						Product.dateInserted
					ELSE
						Expenditure.dateInserted
				END
		END >= @dateFilterParam
	OPTION (RECOMPILE);

	UPDATE DecisionNet.ClaimReferenceExtract WITH (TABLOCKX)
		SET
			ClaimReferenceExtract.transactionDate = SOURCE.transactionDate,
			ClaimReferenceExtract.companySoldToCode = SOURCE.companySoldToCode,
			ClaimReferenceExtract.officeSoldToCode = SOURCE.officeSoldToCode,
			ClaimReferenceExtract.companyShippedToCode = SOURCE.companyShippedToCode,
			ClaimReferenceExtract.officeShippedToCode = SOURCE.officeShippedToCode,
			ClaimReferenceExtract.productTransactionCode = SOURCE.productTransactionCode,
			ClaimReferenceExtract.dateBilledRun = SOURCE.dateBilledRun,
			ClaimReferenceExtract.dateFilled = SOURCE.dateFilled,
			ClaimReferenceExtract.originalBillFlag = SOURCE.originalBillFlag,
			ClaimReferenceExtract.alternateBillFlag = SOURCE.alternateBillFlag,
			ClaimReferenceExtract.isBilled = SOURCE.isBilled,
			ClaimReferenceExtract.userName = SOURCE.userName,
			ClaimReferenceExtract.userCustomerCode = SOURCE.userCustomerCode,
			ClaimReferenceExtract.userOfficeCode = SOURCE.userOfficeCode,
			ClaimReferenceExtract.userJobClassDescription = SOURCE.userJobClassDescription,
			ClaimReferenceExtract.userAddressLine1 = SOURCE.userAddressLine1,
			ClaimReferenceExtract.userCity = SOURCE.userCity,
			ClaimReferenceExtract.userState = SOURCE.userState,
			ClaimReferenceExtract.userZipCode = SOURCE.userZipCode,
			ClaimReferenceExtract.concatendatedProductHierarchyLvl = SOURCE.concatendatedProductHierarchyLvl,
			ClaimReferenceExtract.productTransactionDescription = SOURCE.productTransactionDescription,
			ClaimReferenceExtract.productGroupId = SOURCE.productGroupId,
			ClaimReferenceExtract.productGroupName = SOURCE.productGroupName,
			ClaimReferenceExtract.lineItemQuantity = SOURCE.lineItemQuantity,
			ClaimReferenceExtract.lineItemCost = SOURCE.lineItemCost,
			ClaimReferenceExtract.unitTax = SOURCE.unitTax,
			ClaimReferenceExtract.invoiceDate = SOURCE.invoiceDate,
			ClaimReferenceExtract.invoiceNumber = SOURCE.invoiceNumber,
			ClaimReferenceExtract.productCode = SOURCE.productCode,
			ClaimReferenceExtract.vendorId = SOURCE.vendorId,
			ClaimReferenceExtract.dateInserted = @dateInserted
	FROM
		#TempClaimReferenceExtract AS SOURCE
	WHERE
		SOURCE.transactionId = ClaimReferenceExtract.transactionId
		AND Source.userId = ClaimReferenceExtract.userId
		AND Source.claimReferenceNumber = ClaimReferenceExtract.claimReferenceNumber
		AND Source.uniqueInstanceValue = ClaimReferenceExtract.uniqueInstanceValue
		AND 
		(
			ISNULL(ClaimReferenceExtract.companySoldToCode,'') <> ISNULL(SOURCE.companySoldToCode,'')
			OR ISNULL(ClaimReferenceExtract.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			OR ISNULL(ClaimReferenceExtract.companyShippedToCode,'') <> ISNULL(SOURCE.companyShippedToCode,'')
			OR ISNULL(ClaimReferenceExtract.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
			OR ISNULL(ClaimReferenceExtract.productTransactionCode,'') <> ISNULL(SOURCE.productTransactionCode,'')
			OR ISNULL(ClaimReferenceExtract.dateBilledRun,'00010101') <> ISNULL(SOURCE.dateBilledRun,'00010101')
			OR ISNULL(ClaimReferenceExtract.dateFilled,'00010101') <> ISNULL(SOURCE.dateFilled,'00010101')
			OR ISNULL(ClaimReferenceExtract.originalBillFlag,'') <> ISNULL(SOURCE.originalBillFlag,'')
			OR ISNULL(ClaimReferenceExtract.alternateBillFlag,'') <> ISNULL(SOURCE.alternateBillFlag,'')
			OR ISNULL(ClaimReferenceExtract.isBilled,'') <> ISNULL(SOURCE.isBilled,'')
			OR ISNULL(ClaimReferenceExtract.userName,'') <> ISNULL(SOURCE.userName,'')
			OR ISNULL(ClaimReferenceExtract.userCustomerCode,'') <> ISNULL(SOURCE.userCustomerCode,'')
			OR ISNULL(ClaimReferenceExtract.userOfficeCode,'') <> ISNULL(SOURCE.userOfficeCode,'')
			OR ISNULL(ClaimReferenceExtract.userJobClassDescription,'') <> ISNULL(SOURCE.userJobClassDescription,'')
			OR ISNULL(ClaimReferenceExtract.userAddressLine1,'') <> ISNULL(SOURCE.userAddressLine1,'')
			OR ISNULL(ClaimReferenceExtract.userCity,'') <> ISNULL(SOURCE.userCity,'')
			OR ISNULL(ClaimReferenceExtract.userState,'') <> ISNULL(SOURCE.userState,'')
			OR ISNULL(ClaimReferenceExtract.userZipCode,'') <> ISNULL(SOURCE.userZipCode,'')
			OR ISNULL(ClaimReferenceExtract.concatendatedProductHierarchyLvl,'') <> ISNULL(SOURCE.concatendatedProductHierarchyLvl,'')
			OR ISNULL(ClaimReferenceExtract.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
			OR ISNULL(ClaimReferenceExtract.productGroupId,0) <> ISNULL(SOURCE.productGroupId,0)
			OR ISNULL(ClaimReferenceExtract.productGroupName,'') <> ISNULL(SOURCE.productGroupName,'')
			OR ISNULL(ClaimReferenceExtract.lineItemQuantity,0) <> ISNULL(SOURCE.lineItemQuantity,0)
			OR ISNULL(ClaimReferenceExtract.lineItemCost,-.01) <> ISNULL(SOURCE.lineItemCost,-.01)
			OR ISNULL(ClaimReferenceExtract.unitTax,-.01) <> ISNULL(SOURCE.unitTax,-.01)
			OR ISNULL(ClaimReferenceExtract.invoiceDate,'00010101') <> ISNULL(SOURCE.invoiceDate,'00010101')
			OR ISNULL(ClaimReferenceExtract.invoiceNumber,'') <> ISNULL(SOURCE.invoiceNumber,'')
			OR ISNULL(ClaimReferenceExtract.productCode,'') <> ISNULL(SOURCE.productCode,'')
			OR ISNULL(ClaimReferenceExtract.vendorId,0) <> ISNULL(SOURCE.vendorId,0)
		)
	OPTION (RECOMPILE);
	
	INSERT INTO DecisionNet.ClaimReferenceExtract WITH (TABLOCKX)
	(
		transactionId, userId, claimReferenceNumber, uniqueInstanceValue, transactionDate, companySoldToCode, officeSoldToCode, companyShippedToCode, officeShippedToCode, productTransactionCode, dateBilledRun, dateFilled,
		originalBillFlag, alternateBillFlag, isBilled, userName, userCustomerCode, userOfficeCode, userJobClassDescription, userAddressLine1, userCity, userState,
		userZipCode, concatendatedProductHierarchyLvl, productTransactionDescription, productGroupId, productGroupName, lineItemQuantity,  lineItemCost, unitTax, invoiceDate, invoiceNumber, productCode, vendorId,
		isLocationSearchUsed, isPersonalSearchUsed, isVehicleSearchUsed, dateInserted
	)
	SELECT
		SOURCE.transactionId, SOURCE.userId, SOURCE.claimReferenceNumber, SOURCE.uniqueInstanceValue, SOURCE.transactionDate, SOURCE.companySoldToCode, SOURCE.officeSoldToCode, SOURCE.companyShippedToCode, SOURCE.officeShippedToCode, SOURCE.productTransactionCode, SOURCE.dateBilledRun, SOURCE.dateFilled,
		SOURCE.originalBillFlag, SOURCE.alternateBillFlag, SOURCE.isBilled, SOURCE.userName, SOURCE.userCustomerCode, SOURCE.userOfficeCode, SOURCE.userJobClassDescription, SOURCE.userAddressLine1, SOURCE.userCity, SOURCE.userState,
		SOURCE.userZipCode, SOURCE.concatendatedProductHierarchyLvl, SOURCE.productTransactionDescription, SOURCE.productGroupId, SOURCE.productGroupName, SOURCE.lineItemQuantity, SOURCE.lineItemCost, SOURCE.unitTax, SOURCE.invoiceDate, SOURCE.invoiceNumber, SOURCE.productCode, SOURCE.vendorId,
		@dateInserted
	FROM		
		#TempClaimReferenceExtract AS SOURCE
	EXCEPT
	SELECT
		ClaimReferenceExtract.transactionId, ClaimReferenceExtract.userId, ClaimReferenceExtract.claimReferenceNumber, ClaimReferenceExtract.uniqueInstanceValue, ClaimReferenceExtract.transactionDate, ClaimReferenceExtract.companySoldToCode, ClaimReferenceExtract.officeSoldToCode, ClaimReferenceExtract.companyShippedToCode, ClaimReferenceExtract.officeShippedToCode, ClaimReferenceExtract.productTransactionCode, ClaimReferenceExtract.dateBilledRun, ClaimReferenceExtract.dateFilled,
		ClaimReferenceExtract.originalBillFlag, ClaimReferenceExtract.alternateBillFlag, ClaimReferenceExtract.isBilled, ClaimReferenceExtract.userName, ClaimReferenceExtract.userCustomerCode, ClaimReferenceExtract.userOfficeCode, ClaimReferenceExtract.userJobClassDescription, ClaimReferenceExtract.userAddressLine1, ClaimReferenceExtract.userCity, ClaimReferenceExtract.userState,
		ClaimReferenceExtract.userZipCode, ClaimReferenceExtract.concatendatedProductHierarchyLvl, ClaimReferenceExtract.productTransactionDescription, ClaimReferenceExtract.productGroupId, ClaimReferenceExtract.productGroupName, ClaimReferenceExtract.lineItemQuantity, ClaimReferenceExtract.lineItemCost, ClaimReferenceExtract.unitTax, ClaimReferenceExtract.invoiceDate, ClaimReferenceExtract.invoiceNumber, ClaimReferenceExtract.productCode, ClaimReferenceExtract.vendorId,
		@dateInserted
	FROM
		DecisionNet.ClaimReferenceExtract
	OPTION (RECOMPILE);

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

--SELECT COUNT(*) FROM DecisionNet.ClaimReferenceExtract;
--TRUNCATE TABLE DecisionNet.ClaimReferenceExtract;

--EXEC DecisionNet.hsp_UpdateInsertClaimReferenceExtract
--	@dateFilterParam = '20140101';
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO

--SELECT COUNT(*) FROM DecisionNet.ClaimReferenceExtract;

----PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*

*/