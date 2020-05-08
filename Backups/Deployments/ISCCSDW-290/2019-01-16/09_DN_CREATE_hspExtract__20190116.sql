USE [ClaimSearch_Prod]
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
ALTER TABLE DecisionNet.ClaimReferenceExtract
	ADD recentlyObservedLineItemCost DECIMAL(17,2) NULL;

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC DecisionNet.hsp_UpdateInsertClaimReferenceExtract
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

	DECLARE @Log_StepDesc varchar(1000)
		,@Log_RecordsAffected bigint
		,@Log_StartDateTime datetime
		,@ProductCode varchar(2)
		,@Log_StepStatus varchar(100)
		,@Log_EndDateTime datetime 
		,@Log_TimeTaken varchar(10)
		,@Log_ActualProcessedDate date
		,@sourcedate date

	SELECT
		@dateFilterParam =  COALESCE(@dateFilterParam,MAX(sourcedate))
	FROM [ClaimSearch_Prod].[DecisionNet].[CS_DecisionNet_Dashboard_Process_Log] WITH(nolock)
	WHERE StepNumber in (14, 15)
	
	SELECT  @sourcedate = CONVERT(DATE,CONVERT(VARCHAR(10),MAX(date_insert)),112)
	FROM ClaimSearch.CS.CLT00201  WITH(NOLOCK)
	
	Set @ProductCode = 'CS'

	--SET @dateFilterParam = '2014-01-01'

	IF NOT EXISTS ( select 1 from ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log WITH(NOLOCK)
	where StepNumber =  18
	and SourceDate = @sourcedate
	)
	BEGIN

		IF EXISTS (SELECT 1 
		from ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log with(nolock)
		where SourceDate =  @sourcedate
		and StepNumber = 15
		)
		BEGIN
			--SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));
	
----------------------------------------------------------------------------------------------------------	
			Set @Log_StepDesc = 'Create #TempClaimReferenceExtract'
			Set @Log_StartDateTime = GETDATE()		
	
	
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
				Product.recentlyObservedLineItemCost,
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
		
			SELECT  @Log_RecordsAffected = @@Rowcount
			Select @Log_ActualProcessedDate = CONVERT(date,getdate())
			Set @Log_EndDateTime = GETDATE()
			Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 
	
			Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
			Select @ProductCode, @sourcedate, @Log_ActualProcessedDate,16, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     
			----------------------------------------------------------------------------------------------------------------	
			Set @Log_StepDesc = 'Update DecisionNet.ClaimReferenceExtract'
			Set @Log_StartDateTime = GETDATE()		


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
					ClaimReferenceExtract.dateSearched = SOURCE.dateSearched,
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
					ClaimReferenceExtract.dateInserted = @dateInserted,
					ClaimReferenceExtract.recentlyObservedLineItemCost = SOURCE.recentlyObservedLineItemCost
					
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
					OR ISNULL(ClaimReferenceExtract.dateSearched,'00010101') <> ISNULL(SOURCE.dateSearched,'00010101')
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
					OR ISNULL(ClaimReferenceExtract.recentlyObservedLineItemCost,-.01) = ISNULL(SOURCE.recentlyObservedLineItemCost,-.01)
				);

			SELECT  @Log_RecordsAffected = @@Rowcount
			Select @Log_ActualProcessedDate = CONVERT(date,getdate())
			Set @Log_EndDateTime = GETDATE()
			Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
			Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
			Select @ProductCode, @sourcedate, @Log_ActualProcessedDate,17, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     
			----------------------------------------------------------------------------------------------------------------	
			Set @Log_StepDesc = 'Insert into DecisionNet.ClaimReferenceExtract'
			Set @Log_StartDateTime = GETDATE()		
	
			INSERT INTO DecisionNet.ClaimReferenceExtract WITH (TABLOCKX)
			(
				transactionId, userId, claimReferenceNumber, uniqueInstanceValue, transactionDate, companySoldToCode, officeSoldToCode, companyShippedToCode, officeShippedToCode, productTransactionCode, dateBilledRun, dateFilled, dateSearched,
				originalBillFlag, alternateBillFlag, isBilled, userName, userCustomerCode, userOfficeCode, userJobClassDescription, userAddressLine1, userCity, userState,
				userZipCode, concatendatedProductHierarchyLvl, productTransactionDescription, productGroupId, productGroupName, lineItemQuantity,  lineItemCost, unitTax, invoiceDate, invoiceNumber, productCode, vendorId,
				--isLocationSearchUsed, isPersonalSearchUsed, isVehicleSearchUsed,
				dateInserted, recentlyObservedLineItemCost
			)
			SELECT
				SOURCE.transactionId, SOURCE.userId, SOURCE.claimReferenceNumber, SOURCE.uniqueInstanceValue, SOURCE.transactionDate, SOURCE.companySoldToCode, SOURCE.officeSoldToCode, SOURCE.companyShippedToCode, SOURCE.officeShippedToCode, SOURCE.productTransactionCode, SOURCE.dateBilledRun, SOURCE.dateFilled, SOURCE.dateSearched,
				SOURCE.originalBillFlag, SOURCE.alternateBillFlag, SOURCE.isBilled, SOURCE.userName, SOURCE.userCustomerCode, SOURCE.userOfficeCode, SOURCE.userJobClassDescription, SOURCE.userAddressLine1, SOURCE.userCity, SOURCE.userState,
				SOURCE.userZipCode, SOURCE.concatendatedProductHierarchyLvl, SOURCE.productTransactionDescription, SOURCE.productGroupId, SOURCE.productGroupName, SOURCE.lineItemQuantity, SOURCE.lineItemCost, SOURCE.unitTax, SOURCE.invoiceDate, SOURCE.invoiceNumber, SOURCE.productCode, SOURCE.vendorId,
				--SOURCE.isLocationSearchUsed, SOURCE.isPersonalSearchUsed, SOURCE.isVehicleSearchUsed, 
				@dateInserted, SOURCE.recentlyObservedLineItemCost
			FROM		
				#TempClaimReferenceExtract AS SOURCE
			EXCEPT
			SELECT
				ClaimReferenceExtract.transactionId, ClaimReferenceExtract.userId, ClaimReferenceExtract.claimReferenceNumber, ClaimReferenceExtract.uniqueInstanceValue, ClaimReferenceExtract.transactionDate, ClaimReferenceExtract.companySoldToCode, ClaimReferenceExtract.officeSoldToCode, ClaimReferenceExtract.companyShippedToCode, ClaimReferenceExtract.officeShippedToCode, ClaimReferenceExtract.productTransactionCode, ClaimReferenceExtract.dateBilledRun, ClaimReferenceExtract.dateFilled, ClaimReferenceExtract.dateSearched,
				ClaimReferenceExtract.originalBillFlag, ClaimReferenceExtract.alternateBillFlag, ClaimReferenceExtract.isBilled, ClaimReferenceExtract.userName, ClaimReferenceExtract.userCustomerCode, ClaimReferenceExtract.userOfficeCode, ClaimReferenceExtract.userJobClassDescription, ClaimReferenceExtract.userAddressLine1, ClaimReferenceExtract.userCity, ClaimReferenceExtract.userState,
				ClaimReferenceExtract.userZipCode, ClaimReferenceExtract.concatendatedProductHierarchyLvl, ClaimReferenceExtract.productTransactionDescription, ClaimReferenceExtract.productGroupId, ClaimReferenceExtract.productGroupName, ClaimReferenceExtract.lineItemQuantity, ClaimReferenceExtract.lineItemCost, ClaimReferenceExtract.unitTax, ClaimReferenceExtract.invoiceDate, ClaimReferenceExtract.invoiceNumber, ClaimReferenceExtract.productCode, ClaimReferenceExtract.vendorId,
				--ClaimReferenceExtract.isLocationSearchUsed, ClaimReferenceExtract.isPersonalSearchUsed, ClaimReferenceExtract.isVehicleSearchUsed, 
				@dateInserted, ClaimReferenceExtract.recentlyObservedLineItemCost
			FROM
				DecisionNet.ClaimReferenceExtract /*DO NOT USE WITH NO LOCK HERE. IT IS A BAD THING. DON'T DO IT. TALK TO RDW.*/
		
			SELECT  @Log_RecordsAffected = @@Rowcount
			Select @Log_ActualProcessedDate = CONVERT(date,getdate())
			Set @Log_EndDateTime = GETDATE()
			Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 
	
			Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
			Select @ProductCode, @sourcedate, @Log_ActualProcessedDate,18, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

		END
----------------------------------------------------------------------------------------------------------------	
	END
END
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
Description: View for DashboardExtract. Transforms ProductGroupId code to column-flag,
			   which is what the Dashboard expects. Also aliases column names to be consistent with
			   the dev-version of the Dashboard (which the BI team had already started programing against).
			 Also includes date filter and joins against additional businessRequired production tables.
************************************************/
ALTER VIEW [DecisionNet].[V_ClaimReferenceExtract] AS
(
	SELECT
		ClaimReferenceExtract.transactionId AS I_TRNS,
		ClaimReferenceExtract.userId AS I_USR,
		ClaimReferenceExtract.claimReferenceNumber,
		ClaimReferenceExtract.transactionDate AS D_TRNS,
		ClaimReferenceExtract.companySoldToCode,
		CompanyHeirarchy.Customer_lvl0 + ' - ' + ClaimReferenceExtract.companySoldToCode AS companyName,
		ClaimReferenceExtract.officeSoldToCode,
		ClaimReferenceExtract.companyShippedToCode,
		ClaimReferenceExtract.officeShippedToCode,
		ClaimReferenceExtract.productTransactionCode,
		ClaimReferenceExtract.dateBilledRun,
		ClaimReferenceExtract.dateFilled,
		ClaimReferenceExtract.originalBillFlag AS F_BILLABLE,
		ClaimReferenceExtract.alternateBillFlag AS F_BILLMATCH,
		ClaimReferenceExtract.userName AS NAME,
		/*ClaimReferenceExtract.userCustomerCode AS I_CUST_USR,*/
		/*ClaimReferenceExtract.userOfficeCode AS I_REGOFF_USR,*/
		ClaimReferenceExtract.userJobClassDescription AS Jobclass_Desc,
		/*ClaimReferenceExtract.userAddressLine1,*/
		/*ClaimReferenceExtract.userCity,*/
		/*ClaimReferenceExtract.userState,*/
		/*ClaimReferenceExtract.userZipCode,*/
		/*ClaimReferenceExtract.concatendatedProductHierarchyLvl AS Product_Location,*/
		/*ClaimReferenceExtract.productTransactionDescription,*/
		/*The representing the following mutually-exclusive/binary values as INT to stay consistent with strucutres
			that were developed against. Would NOT recommend following this example in the future.
		*/
		ClaimReferenceExtract.productGroupName AS productCategory,
		1 AS volume,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 1 /*People*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_People,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 2 /*Vehicle_Location_Products*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Vehicle_Location_Products,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 3 /*Assets*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Assets,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 4 /*Business*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Business,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 5 /*Healthcare*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Healthcare,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 6 /*Directory_Assistance*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Directory_Assistance,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 7 /*Weather_Reports*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Weather_Reports,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 8 /*Criminal_Records*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Criminal_Records,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 9 /*Police_Reports*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Police_Reports,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 10 /*Driver_History*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Driver_History,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 11 /*Medical_Records*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_Medical_Records,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 12 /*ClaimDirector*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_ClaimDirector,
		CASE
			WHEN
				ClaimReferenceExtract.productGroupId IS NULL
			THEN
				NULL
			WHEN
				ClaimReferenceExtract.productGroupId = 13 /*OTHER*/
			THEN
				CAST(1 AS INT)
			ELSE
				0
		END AS F_OTHER,
		/**********************************************/
		COALESCE(ClaimReferenceExtract.lineItemCost, ClaimReferenceExtract.recentlyObservedLineItemCost) AS A_LN_ITM_EXTN_TR,
		ClaimReferenceExtract.unitTax AS A_LN_ITM_TAX_TR,
		ClaimReferenceExtract.invoiceDate,
		ClaimReferenceExtract.invoiceNumber,
		ClaimReferenceExtract.productCode,
		/*Columns Deprecated
			ClaimReferenceExtract.vendorTransactionDescription AS [T_VEND_TRNS],
			ClaimReferenceExtract.vendorAccountType AS [ACT_TYP],
		*/
		ClaimReferenceExtract.vendorId,
		/*ClaimReferenceExtract.islocationSearchUsed AS [LOCATION SEARCH ENTRY],*/
		/*ClaimReferenceExtract.isPersonalSearchUsed AS [PERSONAL SEARCH ENTRY],*/
		/*ClaimReferenceExtract.isVehicleSearchUsed AS [VEHICLE SEARCH ENTRY]*/
		dateInserted
	FROM
		DecisionNet.ClaimReferenceExtract  WITH(NOLOCK)
		LEFT OUTER JOIN dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH(NOLOCK)
			ON CompanyHeirarchy.lvl0 = ClaimReferenceExtract.companySoldToCode
	WHERE
		/*Refactor to DATEFROMPARTS in SQLSERVER 2012,
			use of BETWEEN preservs potential indexes on the DATE column*/
		ClaimReferenceExtract.transactionDate >= CAST(CAST((YEAR(GETDATE())-4) AS CHAR(4)) +'0101' AS DATE)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
