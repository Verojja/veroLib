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
CREATE PROCEDURE DecisionNet.hsp_UpdateInsertClaimReferenceExtractAWSBeta
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));
	SELECT
		ClaimReference.transactionId,
		ClaimReference.userId,
		ClaimReference.claimReferenceNumber,
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
		ClaimReference.vendorId,
		CASE
			WHEN
				COALESCE
				(
					ClaimReference.nameSearched,
					ClaimReference.addressLine1Searched,
					ClaimReference.citySearched,
					ClaimReference.zipCodeSearched,
					ClaimReference.countyCodeSearched,
					ClaimReference.stateSearched
				) IS NOT NULL
			THEN
				CAST(1 AS BIT)
			ELSE
				CAST(0 AS BIT)
		END AS isLocationSearchUsed,
		CASE
			WHEN
				COALESCE
				(
					ClaimReference.dateOfBirthSearched,
					ClaimReference.minAgeSearched,
					ClaimReference.maxAgeSearched,
					ClaimReference.phoneNumber1Searched,
					ClaimReference.phoneNumber2Searched,
					ClaimReference.phoneNumber3Searched,
					ClaimReference.phoneNumber4Searched,
					ClaimReference.phoneNumber5Searched,
					ClaimReference.tokenizedSSNSearched,
					ClaimReference.radSearched,
					ClaimReference.phtcSearched
				) IS NOT NULL
			THEN
				CAST(1 AS BIT)
			ELSE
				CAST(0 AS BIT)
		END AS isPersonalSearchUsed,
		CASE
			WHEN
				COALESCE
				(
					ClaimReference.vinSearched,
					ClaimReference.driversLicenseStateSearched,
					ClaimReference.licencePlateSearched,
					ClaimReference.driversLicenseSearched
				) IS NOT NULL
			THEN
				CAST(1 AS BIT)
			ELSE
				CAST(0 AS BIT)
		END AS isVehicleSearchUsed
		INTO #TempClaimReferenceExtract
	FROM
		[ClaimSearch_Prod].DecisionNet.ClaimReference WITH (NOLOCK)
		INNER JOIN [ClaimSearch_Prod].DecisionNet.Product WITH (NOLOCK)
			ON Product.productTransactionCode = ClaimReference.productTransactionCode
		INNER JOIN [ClaimSearch_Prod].DecisionNet.ProductGroup WITH (NOLOCK)
			ON ProductGroup.productGroupId = Product.productGroupId
		OUTER APPLY
		(
			SELECT
				RIGHT(InnerProduct.recentlyObservedProductCode,4) AS productTransactionCode
			FROM
				[ClaimSearch_Prod].DecisionNet.Product AS InnerProduct WITH(NOLOCK)
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
				[ClaimSearch_Prod].DecisionNet.Expenditure AS INNERExpenditure WITH(NOLOCK)
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

	UPDATE DecisionNet.ClaimReferenceExtractAWSBeta WITH (TABLOCKX)
		SET
			ClaimReferenceExtractAWSBeta.transactionDate = SOURCE.transactionDate,
			ClaimReferenceExtractAWSBeta.companySoldToCode = SOURCE.companySoldToCode,
			ClaimReferenceExtractAWSBeta.officeSoldToCode = SOURCE.officeSoldToCode,
			ClaimReferenceExtractAWSBeta.companyShippedToCode = SOURCE.companyShippedToCode,
			ClaimReferenceExtractAWSBeta.officeShippedToCode = SOURCE.officeShippedToCode,
			ClaimReferenceExtractAWSBeta.productTransactionCode = SOURCE.productTransactionCode,
			ClaimReferenceExtractAWSBeta.dateBilledRun = SOURCE.dateBilledRun,
			ClaimReferenceExtractAWSBeta.dateFilled = SOURCE.dateFilled,
			ClaimReferenceExtractAWSBeta.originalBillFlag = SOURCE.originalBillFlag,
			ClaimReferenceExtractAWSBeta.alternateBillFlag = SOURCE.alternateBillFlag,
			ClaimReferenceExtractAWSBeta.isBilled = SOURCE.isBilled,
			ClaimReferenceExtractAWSBeta.userName = SOURCE.userName,
			ClaimReferenceExtractAWSBeta.userCustomerCode = SOURCE.userCustomerCode,
			ClaimReferenceExtractAWSBeta.userOfficeCode = SOURCE.userOfficeCode,
			ClaimReferenceExtractAWSBeta.userJobClassDescription = SOURCE.userJobClassDescription,
			ClaimReferenceExtractAWSBeta.userAddressLine1 = SOURCE.userAddressLine1,
			ClaimReferenceExtractAWSBeta.userCity = SOURCE.userCity,
			ClaimReferenceExtractAWSBeta.userState = SOURCE.userState,
			ClaimReferenceExtractAWSBeta.userZipCode = SOURCE.userZipCode,
			ClaimReferenceExtractAWSBeta.concatendatedProductHierarchyLvl = SOURCE.concatendatedProductHierarchyLvl,
			ClaimReferenceExtractAWSBeta.productTransactionDescription = SOURCE.productTransactionDescription,
			ClaimReferenceExtractAWSBeta.productGroupId = SOURCE.productGroupId,
			ClaimReferenceExtractAWSBeta.productGroupName = SOURCE.productGroupName,
			ClaimReferenceExtractAWSBeta.lineItemQuantity = SOURCE.lineItemQuantity,
			ClaimReferenceExtractAWSBeta.lineItemCost = SOURCE.lineItemCost,
			ClaimReferenceExtractAWSBeta.unitTax = SOURCE.unitTax,
			ClaimReferenceExtractAWSBeta.invoiceDate = SOURCE.invoiceDate,
			ClaimReferenceExtractAWSBeta.invoiceNumber = SOURCE.invoiceNumber,
			ClaimReferenceExtractAWSBeta.productCode = SOURCE.productCode,
			ClaimReferenceExtractAWSBeta.vendorId = SOURCE.vendorId,
			ClaimReferenceExtractAWSBeta.isLocationSearchUsed = SOURCE.isLocationSearchUsed,
			ClaimReferenceExtractAWSBeta.isPersonalSearchUsed = SOURCE.isPersonalSearchUsed,
			ClaimReferenceExtractAWSBeta.isVehicleSearchUsed = SOURCE.isVehicleSearchUsed,
			ClaimReferenceExtractAWSBeta.dateInserted = @dateInserted
	FROM
		#TempClaimReferenceExtract AS SOURCE
	WHERE
		SOURCE.transactionId = ClaimReferenceExtractAWSBeta.transactionId
		AND Source.userId = ClaimReferenceExtractAWSBeta.userId
		AND Source.claimReferenceNumber = ClaimReferenceExtractAWSBeta.claimReferenceNumber
		AND 
		(
			ISNULL(ClaimReferenceExtractAWSBeta.companySoldToCode,'') <> ISNULL(SOURCE.companySoldToCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.companyShippedToCode,'') <> ISNULL(SOURCE.companyShippedToCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.productTransactionCode,'') <> ISNULL(SOURCE.productTransactionCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.dateBilledRun,'00010101') <> ISNULL(SOURCE.dateBilledRun,'00010101')
			OR ISNULL(ClaimReferenceExtractAWSBeta.dateFilled,'00010101') <> ISNULL(SOURCE.dateFilled,'00010101')
			OR ISNULL(ClaimReferenceExtractAWSBeta.originalBillFlag,'') <> ISNULL(SOURCE.originalBillFlag,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.alternateBillFlag,'') <> ISNULL(SOURCE.alternateBillFlag,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.isBilled,'') <> ISNULL(SOURCE.isBilled,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userName,'') <> ISNULL(SOURCE.userName,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userCustomerCode,'') <> ISNULL(SOURCE.userCustomerCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userOfficeCode,'') <> ISNULL(SOURCE.userOfficeCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userJobClassDescription,'') <> ISNULL(SOURCE.userJobClassDescription,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userAddressLine1,'') <> ISNULL(SOURCE.userAddressLine1,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userCity,'') <> ISNULL(SOURCE.userCity,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userState,'') <> ISNULL(SOURCE.userState,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.userZipCode,'') <> ISNULL(SOURCE.userZipCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.concatendatedProductHierarchyLvl,'') <> ISNULL(SOURCE.concatendatedProductHierarchyLvl,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.productGroupId,0) <> ISNULL(SOURCE.productGroupId,0)
			OR ISNULL(ClaimReferenceExtractAWSBeta.productGroupName,'') <> ISNULL(SOURCE.productGroupName,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.lineItemQuantity,0) <> ISNULL(SOURCE.lineItemQuantity,0)
			OR ISNULL(ClaimReferenceExtractAWSBeta.lineItemCost,-.01) <> ISNULL(SOURCE.lineItemCost,-.01)
			OR ISNULL(ClaimReferenceExtractAWSBeta.unitTax,-.01) <> ISNULL(SOURCE.unitTax,-.01)
			OR ISNULL(ClaimReferenceExtractAWSBeta.invoiceDate,'00010101') <> ISNULL(SOURCE.invoiceDate,'00010101')
			OR ISNULL(ClaimReferenceExtractAWSBeta.invoiceNumber,'') <> ISNULL(SOURCE.invoiceNumber,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.productCode,'') <> ISNULL(SOURCE.productCode,'')
			OR ISNULL(ClaimReferenceExtractAWSBeta.vendorId,0) <> ISNULL(SOURCE.vendorId,0)
			OR ClaimReferenceExtractAWSBeta.isLocationSearchUsed <> SOURCE.isLocationSearchUsed
			OR ClaimReferenceExtractAWSBeta.isPersonalSearchUsed <> SOURCE.isPersonalSearchUsed
			OR ClaimReferenceExtractAWSBeta.isVehicleSearchUsed <> SOURCE.isVehicleSearchUsed
		)
	OPTION (RECOMPILE);
	
	INSERT INTO DecisionNet.ClaimReferenceExtractAWSBeta WITH (TABLOCKX)
	(
		transactionId, userId, claimReferenceNumber, transactionDate, companySoldToCode, officeSoldToCode, companyShippedToCode, officeShippedToCode, productTransactionCode, dateBilledRun, dateFilled,
		originalBillFlag, alternateBillFlag, isBilled, userName, userCustomerCode, userOfficeCode, userJobClassDescription, userAddressLine1, userCity, userState,
		userZipCode, concatendatedProductHierarchyLvl, productTransactionDescription, productGroupId, productGroupName, lineItemQuantity,  lineItemCost, unitTax, invoiceDate, invoiceNumber, productCode, vendorId,
		isLocationSearchUsed, isPersonalSearchUsed, isVehicleSearchUsed, dateInserted
	)
	SELECT
		SOURCE.transactionId, SOURCE.userId, SOURCE.claimReferenceNumber, SOURCE.transactionDate, SOURCE.companySoldToCode, SOURCE.officeSoldToCode, SOURCE.companyShippedToCode, SOURCE.officeShippedToCode, SOURCE.productTransactionCode, SOURCE.dateBilledRun, SOURCE.dateFilled,
		SOURCE.originalBillFlag, SOURCE.alternateBillFlag, SOURCE.isBilled, SOURCE.userName, SOURCE.userCustomerCode, SOURCE.userOfficeCode, SOURCE.userJobClassDescription, SOURCE.userAddressLine1, SOURCE.userCity, SOURCE.userState,
		SOURCE.userZipCode, SOURCE.concatendatedProductHierarchyLvl, SOURCE.productTransactionDescription, SOURCE.productGroupId, SOURCE.productGroupName, SOURCE.lineItemQuantity, SOURCE.lineItemCost, SOURCE.unitTax, SOURCE.invoiceDate, SOURCE.invoiceNumber, SOURCE.productCode, SOURCE.vendorId,
		SOURCE.isLocationSearchUsed, SOURCE.isPersonalSearchUsed, SOURCE.isVehicleSearchUsed, @dateInserted
	FROM		
		#TempClaimReferenceExtract AS SOURCE
	EXCEPT
	SELECT
		ClaimReferenceExtractAWSBeta.transactionId, ClaimReferenceExtractAWSBeta.userId, ClaimReferenceExtractAWSBeta.claimReferenceNumber, ClaimReferenceExtractAWSBeta.transactionDate, ClaimReferenceExtractAWSBeta.companySoldToCode, ClaimReferenceExtractAWSBeta.officeSoldToCode, ClaimReferenceExtractAWSBeta.companyShippedToCode, ClaimReferenceExtractAWSBeta.officeShippedToCode, ClaimReferenceExtractAWSBeta.productTransactionCode, ClaimReferenceExtractAWSBeta.dateBilledRun, ClaimReferenceExtractAWSBeta.dateFilled,
		ClaimReferenceExtractAWSBeta.originalBillFlag, ClaimReferenceExtractAWSBeta.alternateBillFlag, ClaimReferenceExtractAWSBeta.isBilled, ClaimReferenceExtractAWSBeta.userName, ClaimReferenceExtractAWSBeta.userCustomerCode, ClaimReferenceExtractAWSBeta.userOfficeCode, ClaimReferenceExtractAWSBeta.userJobClassDescription, ClaimReferenceExtractAWSBeta.userAddressLine1, ClaimReferenceExtractAWSBeta.userCity, ClaimReferenceExtractAWSBeta.userState,
		ClaimReferenceExtractAWSBeta.userZipCode, ClaimReferenceExtractAWSBeta.concatendatedProductHierarchyLvl, ClaimReferenceExtractAWSBeta.productTransactionDescription, ClaimReferenceExtractAWSBeta.productGroupId, ClaimReferenceExtractAWSBeta.productGroupName, ClaimReferenceExtractAWSBeta.lineItemQuantity, ClaimReferenceExtractAWSBeta.lineItemCost, ClaimReferenceExtractAWSBeta.unitTax, ClaimReferenceExtractAWSBeta.invoiceDate, ClaimReferenceExtractAWSBeta.invoiceNumber, ClaimReferenceExtractAWSBeta.productCode, ClaimReferenceExtractAWSBeta.vendorId,
		ClaimReferenceExtractAWSBeta.isLocationSearchUsed, ClaimReferenceExtractAWSBeta.isPersonalSearchUsed, ClaimReferenceExtractAWSBeta.isVehicleSearchUsed, @dateInserted
	FROM
		DecisionNet.ClaimReferenceExtractAWSBeta
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

EXEC DecisionNet.hsp_UpdateInsertClaimReferenceExtractAWSBeta
	@dateFilterParam = '20140101';
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
EXEC DecisionNet.hsp_UpdateInsertClaimReferenceExtractAWSBeta
	@dateFilterParam = '20140101';
SELECT COUNT(*) FROM DecisionNet.ClaimReferenceExtractAWSBeta;

----PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

(1 row(s) affected)
COMMIT TRANSACTION

*/