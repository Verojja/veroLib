BEGIN TRANSACTION
	SELECT
		Expenditure.companyShippedToCode,
		Expenditure.officeShippedToCode,
		Expenditure.invoiceExecutionDate,
		Expenditure.itemQuantity AS lineItemQuantity,
		Expenditure.lineItemUnitCost AS lineItemCost,
		CAST(ROUND(Expenditure.itemTax / Expenditure.itemQuantity,2) AS DECIMAL(17,2)) AS unitTax,
		Expenditure.invoiceDate,
		Expenditure.invoiceNumber,
		Expenditure.productCode,
		Expenditure.productTransactionCode,
		AlternateProductCode.productTransactionCode AS alternateProductTransactionCode,
		Expenditure.dateInserted
		INTO #ExpenditureData
	FROM
		(
			SELECT
				INNERExpenditure.invoiceNumber,
				/*INNERExpenditure.lineItemNumber,*/
				INNERExpenditure.invoiceDate,
				INNERExpenditure.invoiceExecutionDate,
				INNERExpenditure.companyShippedToCode,
				INNERExpenditure.officeShippedToCode,
				INNERExpenditure.productCode,
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
		OUTER APPLY
		(
			SELECT
				/*RIGHT(InnerProduct.recentlyObservedProductCode,4) AS productTransactionCode*/
				InnerProduct.productTransactionCode
			FROM
				DecisionNet.Product AS InnerProduct WITH(NOLOCK)
			WHERE
				InnerProduct.productTransactionCode <> RIGHT(InnerProduct.recentlyObservedProductCode,4) /*the recentlyObservedProductCode is from the Expenditure table*/
				AND Expenditure.productTransactionCode = RIGHT(InnerProduct.recentlyObservedProductCode,4)
		) AlternateProductCode;
	CREATE CLUSTERED INDEX PK_TempEx ON #ExpenditureData
		(companyShippedToCode, officeShippedToCode, invoiceExecutionDate, productTransactionCode, alternateProductTransactionCode);
		
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
		ClaimReference.dateBilledRun,
		ClaimReference.dateFilled,
		ClaimReference.dateSearched,
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
		Expenditure.lineItemQuantity,
		Expenditure.lineItemCost,
		Expenditure.unitTax,
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
		LEFT OUTER JOIN #ExpenditureData AS Expenditure
			ON Expenditure.companyShippedToCode = ClaimReference.companyShippedToCode
				AND Expenditure.officeShippedToCode = ClaimReference.officeShippedToCode
				AND Expenditure.invoiceExecutionDate = ClaimReference.dateBilledRun
				AND
				(
					Expenditure.productTransactionCode = ClaimReference.productTransactionCode
					OR
					(
						Expenditure.alternateProductTransactionCode = ClaimReference.productTransactionCode
						AND NOT EXISTS
						(
							SELECT NULL
							FROM
								#ExpenditureData AS INNERExpenditure WITH(NOLOCK)
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
	--WHERE
	--	/*
	--		SQL Server Execution Times: (for a CROSS APPLY of MAX date between the three tables)
	--		CPU time = 1164672 ms,  elapsed time = 1170162 ms.
	--		SQL Server Execution Times: (for current implimentation: IE the following CASE-check)
	--		CPU time = 1023062 ms,  elapsed time = 1118084 ms.
	--	*/
	--	CASE
	--		WHEN
	--			ISNULL(ClaimReference.dateInserted,'00010101') > ISNULL(Product.dateInserted,'00010101')
	--		THEN
	--			CASE
	--				WHEN
	--					ISNULL(ClaimReference.dateInserted,'00010101') > ISNULL(Expenditure.dateInserted,'00010101')
	--				THEN
	--					ClaimReference.dateInserted
	--				ELSE
	--					Expenditure.dateInserted
	--			END
	--		ELSE
	--			CASE
	--				WHEN
	--					ISNULL(Product.dateInserted,'00010101') > ISNULL(Expenditure.dateInserted,'00010101')
	--				THEN
	--					Product.dateInserted
	--				ELSE
	--					Expenditure.dateInserted
	--			END
	--	END >= @dateFilterParam
	--OPTION (RECOMPILE);
	SELECT
		COUNT(ClaimReferenceExtract.transactionId),
		dateInserted
	 --DecisionNet.ClaimReferenceExtract WITH (TABLOCKX)
		--SET
		--	ClaimReferenceExtract.transactionDate = SOURCE.transactionDate,
		--	ClaimReferenceExtract.companySoldToCode = SOURCE.companySoldToCode,
		--	ClaimReferenceExtract.officeSoldToCode = SOURCE.officeSoldToCode,
		--	ClaimReferenceExtract.companyShippedToCode = SOURCE.companyShippedToCode,
		--	ClaimReferenceExtract.officeShippedToCode = SOURCE.officeShippedToCode,
		--	ClaimReferenceExtract.productTransactionCode = SOURCE.productTransactionCode,
		--	ClaimReferenceExtract.dateBilledRun = SOURCE.dateBilledRun,
		--	ClaimReferenceExtract.dateFilled = SOURCE.dateFilled,
		--	ClaimReferenceExtract.dateSearched = SOURCE.dateSearched,
		--	ClaimReferenceExtract.originalBillFlag = SOURCE.originalBillFlag,
		--	ClaimReferenceExtract.alternateBillFlag = SOURCE.alternateBillFlag,
		--	ClaimReferenceExtract.isBilled = SOURCE.isBilled,
		--	ClaimReferenceExtract.userName = SOURCE.userName,
		--	ClaimReferenceExtract.userCustomerCode = SOURCE.userCustomerCode,
		--	ClaimReferenceExtract.userOfficeCode = SOURCE.userOfficeCode,
		--	ClaimReferenceExtract.userJobClassDescription = SOURCE.userJobClassDescription,
		--	ClaimReferenceExtract.userAddressLine1 = SOURCE.userAddressLine1,
		--	ClaimReferenceExtract.userCity = SOURCE.userCity,
		--	ClaimReferenceExtract.userState = SOURCE.userState,
		--	ClaimReferenceExtract.userZipCode = SOURCE.userZipCode,
		--	ClaimReferenceExtract.concatendatedProductHierarchyLvl = SOURCE.concatendatedProductHierarchyLvl,
		--	ClaimReferenceExtract.productTransactionDescription = SOURCE.productTransactionDescription,
		--	ClaimReferenceExtract.productGroupId = SOURCE.productGroupId,
		--	ClaimReferenceExtract.productGroupName = SOURCE.productGroupName,
		--	ClaimReferenceExtract.lineItemQuantity = SOURCE.lineItemQuantity,
		--	ClaimReferenceExtract.lineItemCost = SOURCE.lineItemCost,
		--	ClaimReferenceExtract.unitTax = SOURCE.unitTax,
		--	ClaimReferenceExtract.invoiceDate = SOURCE.invoiceDate,
		--	ClaimReferenceExtract.invoiceNumber = SOURCE.invoiceNumber,
		--	ClaimReferenceExtract.productCode = SOURCE.productCode,
		--	ClaimReferenceExtract.vendorId = SOURCE.vendorId,
		--	ClaimReferenceExtract.dateInserted = @dateInserted
	FROM
		#TempClaimReferenceExtract AS SOURCE
		INNER JOIN DecisionNet.ClaimReferenceExtract
			ON SOURCE.transactionId = ClaimReferenceExtract.transactionId
				AND Source.userId = ClaimReferenceExtract.userId
				AND Source.claimReferenceNumber = ClaimReferenceExtract.claimReferenceNumber
				AND Source.uniqueInstanceValue = ClaimReferenceExtract.uniqueInstanceValue
	WHERE
		--SOURCE.transactionId = ClaimReferenceExtract.transactionId
		--AND Source.userId = ClaimReferenceExtract.userId
		--AND Source.claimReferenceNumber = ClaimReferenceExtract.claimReferenceNumber
		--AND Source.uniqueInstanceValue = ClaimReferenceExtract.uniqueInstanceValue
		--AND 
		(
			--ISNULL(ClaimReferenceExtract.companySoldToCode,'') <> ISNULL(SOURCE.companySoldToCode,'')
			--OR ISNULL(ClaimReferenceExtract.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			--OR ISNULL(ClaimReferenceExtract.companyShippedToCode,'') <> ISNULL(SOURCE.companyShippedToCode,'')
			--OR ISNULL(ClaimReferenceExtract.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
			--OR ISNULL(ClaimReferenceExtract.productTransactionCode,'') <> ISNULL(SOURCE.productTransactionCode,'')
			--OR ISNULL(ClaimReferenceExtract.dateBilledRun,'00010101') <> ISNULL(SOURCE.dateBilledRun,'00010101')
			--OR ISNULL(ClaimReferenceExtract.dateFilled,'00010101') <> ISNULL(SOURCE.dateFilled,'00010101')
			--OR ISNULL(ClaimReferenceExtract.dateSearched,'00010101') <> ISNULL(SOURCE.dateSearched,'00010101')
			--OR ISNULL(ClaimReferenceExtract.originalBillFlag,'') <> ISNULL(SOURCE.originalBillFlag,'')
			--OR ISNULL(ClaimReferenceExtract.alternateBillFlag,'') <> ISNULL(SOURCE.alternateBillFlag,'')
			--OR ISNULL(ClaimReferenceExtract.isBilled,'') <> ISNULL(SOURCE.isBilled,'')
			--OR ISNULL(ClaimReferenceExtract.userName,'') <> ISNULL(SOURCE.userName,'')
			--OR ISNULL(ClaimReferenceExtract.userCustomerCode,'') <> ISNULL(SOURCE.userCustomerCode,'')
			--OR ISNULL(ClaimReferenceExtract.userOfficeCode,'') <> ISNULL(SOURCE.userOfficeCode,'')
			--OR ISNULL(ClaimReferenceExtract.userJobClassDescription,'') <> ISNULL(SOURCE.userJobClassDescription,'')
			--OR ISNULL(ClaimReferenceExtract.userAddressLine1,'') <> ISNULL(SOURCE.userAddressLine1,'')
			--OR ISNULL(ClaimReferenceExtract.userCity,'') <> ISNULL(SOURCE.userCity,'')
			--OR ISNULL(ClaimReferenceExtract.userState,'') <> ISNULL(SOURCE.userState,'')
			--OR ISNULL(ClaimReferenceExtract.userZipCode,'') <> ISNULL(SOURCE.userZipCode,'')
			--OR ISNULL(ClaimReferenceExtract.concatendatedProductHierarchyLvl,'') <> ISNULL(SOURCE.concatendatedProductHierarchyLvl,'')
			--OR ISNULL(ClaimReferenceExtract.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
			--OR ISNULL(ClaimReferenceExtract.productGroupId,0) <> ISNULL(SOURCE.productGroupId,0)
			--OR ISNULL(ClaimReferenceExtract.productGroupName,'') <> ISNULL(SOURCE.productGroupName,'')
			--OR ISNULL(ClaimReferenceExtract.lineItemQuantity,0) <> ISNULL(SOURCE.lineItemQuantity,0)
			--OR ISNULL(ClaimReferenceExtract.lineItemCost,-.01) <> ISNULL(SOURCE.lineItemCost,-.01)
			--OR ISNULL(ClaimReferenceExtract.unitTax,-.01) <> ISNULL(SOURCE.unitTax,-.01)
			--OR ISNULL(ClaimReferenceExtract.invoiceDate,'00010101') <> ISNULL(SOURCE.invoiceDate,'00010101')
			--OR 
			ISNULL(ClaimReferenceExtract.invoiceNumber,'') <> ISNULL(SOURCE.invoiceNumber,'')
			--OR ISNULL(ClaimReferenceExtract.productCode,'') <> ISNULL(SOURCE.productCode,'')
			--OR ISNULL(ClaimReferenceExtract.vendorId,0) <> ISNULL(SOURCE.vendorId,0)
		)
	GROUP BY dateInserted
	OPTION (RECOMPILE);
ROLLBACK TRANSACTION

/*

SELECT
      *
FROM
      DecisionNet.ClaimReferenceExtract
WHERE
transactionId IN  
(
      --'2018-10-02-10.46.49.556696',
      --'2018-10-01-12.09.39.389647',
      --'2018-10-01-16.01.17.389346',
      --'2018-10-09-17.54.18.803455',
      --'2018-10-10-14.51.32.020966',
      --'2018-10-09-11.29.10.497131',
      '2018-10-10-16.38.08.191927'
)


SELECT
      *
FROM
      DecisionNet.Expenditure
WHERE
      Expenditure.invoiceNumber = 'PR00171740'
      AND productTransactionCode = 'ACBS'
      AND officeShippedToCode = '00001'
      
      
SELECT
      *
FROM
      DecisionNet.ClaimReference
WHERE
transactionId IN  
(
      --'2018-10-02-10.46.49.556696',
      --'2018-10-01-12.09.39.389647',
      --'2018-10-01-16.01.17.389346',
      --'2018-10-09-17.54.18.803455',
      --'2018-10-10-14.51.32.020966',
      --'2018-10-09-11.29.10.497131',
      '2018-10-10-16.38.08.191927'
)


number of flagged I-TRNS 	dateInserted
101910				2019-01-17 !!
216951				2019-02-07 !!
54					2019-02-08
80					2019-02-09
10					2019-02-12
6					2019-02-13
90					2019-02-14
20					2019-02-15
92					2019-02-16
56					2019-02-19
28					2019-02-20
6					2019-02-21
58					2019-02-22
44					2019-02-23
4					2019-02-25
42					2019-02-26
2070				2019-02-28
118					2019-03-01
20					2019-03-02
6					2019-03-04
25					2019-03-05
126					2019-03-06
4306				2019-03-07
20					2019-03-08
24					2019-03-09
20					2019-03-11


*/