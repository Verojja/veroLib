--SET NOEXEC OFF;
/*
	This script will create the stored procedure for the Product Dimension Data Refresh.
		
	Execution of the sproc this this script creates relies on data from tables in ClaimSearch_Prod:
		[ClaimSearch_Prod].dbo.MPV00202
		[ClaimSearch_Prod].dbo.CLT00207
		[ClaimSearch_Prod].DecisionNet.ProductHierarchy (new table created by script 01)
		*[ClaimSearch_Prod].DecisionNet.ProductGroup (new table created by script 01)
		*[ClaimSearch_Prod].DecisionNet.TransactionType (new table created by script 01)
		
		*FK checked on Merge insert/update
		
*/

--BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 561) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
	
*/
/************************************************************************************************************************************************/	
/******************************************************Objects Required for indipendent testing**************************************************/	
	/*
		Run script 01_ to create the sub tables;
		 and script 02_ to populate the tables, for a realistic execution-test.
	*/
/*****************************************************************************************************************************/	
/*****************************************************************************************************************************/
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
/***********************************************
WorkItem: ISCCSDW-290
Date: 2018-08-02
Author: Robert David Warner
Description: Mechanism for data-refresh of the Product Table.
			Originally, the table was being droppped,re-created, reinserted into.
			Current behavior now is Upsert, through Merge syntax.
			dateFilterParam defaults to upsertAgainst last 7 calendar days, but otherwise
			can be set passed into for FULL INSERT/UPSERT.
				
			Performance: Execution was around 14 seconds, but appears to have jumped to 40. Need to closely monitor.
						Shifted to SELECT INTO vs. INSERT INTO for log-minimalization.
						INSERT LEFT JOIN since perfect set comparison not applicable.
************************************************/

--EXEC DecisionNet.hsp_UpdateInsertProduct
ALTER PROCEDURE DecisionNet.hsp_UpdateInsertProduct
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));
	
	--SET @dateFilterParam = '2014-01-01'
	
	
	DECLARE @Log_StepDesc varchar(1000)
		,@Log_RecordsAffected bigint
		,@Log_StartDateTime datetime
		,@ProductCode varchar(2)
		,@Log_StepStatus varchar(100)
		,@Log_EndDateTime datetime 
		,@Log_TimeTaken varchar(10)
		,@Log_ActualProcessedDate date
		
Set @ProductCode = 'CS'
	
----------------------------------------------------------------------------------------------------------	
Set @Log_StepDesc = 'Create #TempCLT207Data'
Set @Log_StartDateTime = GETDATE()	
	
	SELECT
		CAST(RankedDescriptions.productTransactionCode AS CHAR(4)) AS productTransactionCode, 
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'')AS VARCHAR(75)) AS productDescription,
		CAST(RankedDescriptions.productCode AS CHAR(9)) AS productCode,
		CAST(RankedDescriptions.oldProductCode AS CHAR(9)) AS oldProductCode,
		CAST(RankedDescriptions.lineItemUnitCost AS DECIMAL(17,2)) AS lineItemUnitCost,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.billableMatchCode)),'') AS CHAR(1)) AS billableMatchCode,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.billableNonMatchCode)),'') AS CHAR(1)) AS billableNonMatchCode,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.transactionTypeCode)),'') AS CHAR(1)) AS transactionTypeCode
		INTO #TempCLT207Data
	FROM
		(
			SELECT
				DescriptionCompareSet.productTransactionCode,
				DescriptionCompareSet.productDescription,
				DescriptionCompareSet.productCode,
				DescriptionCompareSet.oldProductCode,
				DescriptionCompareSet.lineItemUnitCost,
				DescriptionCompareSet.billableMatchCode,
				DescriptionCompareSet.billableNonMatchCode,
				DescriptionCompareSet.transactionTypeCode,
				DescriptionCompareSet.descriptionCompareSetVale,
				ROW_NUMBER() OVER(
					PARTITION BY
						DescriptionCompareSet.productTransactionCode
					ORDER BY
						CASE
							WHEN
								(DescriptionCompareSet.productDescription LIKE '%[0-9][0-9][0-9][0-9]%')
							THEN
								1
							ELSE
								0
						END,
						DescriptionCompareSet.Date_Insert DESC
				) AS productDescription_rowNumber
			FROM
			(
				SELECT
					CLT00207.C_ISO_TRNS AS productTransactionCode,
					CLT00207.T_ISO_TRNS AS productDescription,
					CLT00207.C_PS_PRD AS productCode,
					CLT00207.C_PS_PRD_OLD AS oldProductCode,
					CLT00207.A_ISO_TRNS_LIST AS lineItemUnitCost,
					CLT00207.F_BILL_MTCH AS billableMatchCode,
					CLT00207.F_BILL_NO_MTCH AS billableNonMatchCode,
					CLT00207.C_TRAN_TYP AS transactionTypeCode,
					CLT00207.Date_Insert,
					ROW_NUMBER() OVER(
						PARTITION BY
							CLT00207.C_ISO_TRNS, CLT00207.T_ISO_TRNS
						ORDER BY
							CASE
								WHEN
									(CLT00207.T_ISO_TRNS LIKE '%[0-9][0-9][0-9][0-9]%')
								THEN
									1
								ELSE
									0
							END,
							CLT00207.Date_Insert DESC
					) AS descriptionCompareSetVale
				FROM
					[ClaimSearch_Prod].dbo.CLT00207 WITH (NOLOCK)
				WHERE
					CAST(CAST(CLT00207.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
			) AS DescriptionCompareSet
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1
		
		
SELECT  @Log_RecordsAffected = @@Rowcount
Select @Log_ActualProcessedDate = CONVERT(date,getdate())
Set @Log_EndDateTime = GETDATE()
Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
Select @ProductCode, @dateFilterParam, @Log_ActualProcessedDate,1, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

-----------------------------------------------------------------------------	
Set @Log_StepDesc = 'Create #TempMPVData'
Set @Log_StartDateTime = GETDATE()	
	
	
	SELECT
		CAST(RankedDescriptions.productCode AS CHAR(9)) AS productCode,
		CAST(RankedDescriptions.productTransactionCode AS CHAR(4)) AS productTransactionCode,
		CAST(LEFT(RankedDescriptions.productCode,4) AS CHAR(4)) AS productTransactionTypeCode,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'') AS VARCHAR(75)) AS productDescription,
		RankedDescriptions.lineItemUnitCost
		INTO #TempMPVData
	FROM
		(
			SELECT
				DescriptionCompareSet.productTransactionCode,
				DescriptionCompareSet.productCode,
				DescriptionCompareSet.productDescription,
				DescriptionCompareSet.dateLineItemExractRun,
				DescriptionCompareSet.descriptionCompareSetVale,
				DescriptionCompareSet.lineItemUnitCost,
				ROW_NUMBER() OVER(
					PARTITION BY
						DescriptionCompareSet.productTransactionCode
					ORDER BY
						CASE
							WHEN
								(DescriptionCompareSet.productDescription LIKE '%[0-9][0-9][0-9][0-9]%')
							THEN
								1
							ELSE
								0
						END,
						DescriptionCompareSet.dateLineItemExractRun DESC
				) AS productDescription_rowNumber
			FROM
				(
					SELECT
						RIGHT(MPV00202.I_PRD,4) AS productTransactionCode,
						MPV00202.I_PRD AS productCode,
						MPV00202.T_PRD_DSC AS productDescription,
						MPV00202.A_LN_ITM_UNIT AS lineItemUnitCost,
						ROW_NUMBER() OVER(
							PARTITION BY
								RIGHT(MPV00202.I_PRD,4), MPV00202.T_PRD_DSC
							ORDER BY
								CASE
									WHEN
										(MPV00202.T_PRD_DSC LIKE '%[0-9][0-9][0-9][0-9]%')
									THEN
										1
									ELSE
										0
								END,
								CAST(
									SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,1,10)
									+ ' '
									+ REPLACE((SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
									+ (SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,20,8))
									AS DATETIME2(5)
								)
								DESC
						) AS descriptionCompareSetVale,
						CAST(
							SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,1,10)
							+ ' '
							+ REPLACE((SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
							+ (SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,20,8))
							AS DATETIME2(5)
						)AS dateLineItemExractRun
					FROM
						[ClaimSearch_Prod].dbo.MPV00202 WITH (NOLOCK)
					WHERE
						CAST(CAST(MPV00202.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam
				) AS DescriptionCompareSet
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1
		
		
SELECT  @Log_RecordsAffected = @@Rowcount
Select @Log_ActualProcessedDate = CONVERT(date,getdate())
Set @Log_EndDateTime = GETDATE()
Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
Select @ProductCode, @dateFilterParam, @Log_ActualProcessedDate,2, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

----------------------------------------------------------------------------------------------
Set @Log_StepDesc = 'Create #ProductMergeSource'
Set @Log_StartDateTime = GETDATE()	
	
	SELECT
		COALESCE(
			ProductHierarchy.productTransactionCode,
			#TempCLT207Data.productTransactionCode,
			#TempMPVData.productTransactionCode
		) AS productTransactionCode,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl1)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl1)),'')
		) AS productHierarchyLvl1,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl2)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl2)),'')
		) AS productHierarchyLvl2,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl3)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl3)),'')
		) AS productHierarchyLvl3,
		COALESCE(
				COALESCE(
					NULLIF(LTRIM(RTRIM(ProductHierarchy.productTransactionDescription)),''),
					NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productTransactionDescription)),'')
				),
				#TempCLT207Data.productDescription,
				#TempMPVData.productDescription
		) AS productTransactionDescription,
		#TempCLT207Data.billableMatchCode,
		#TempCLT207Data.billableNonMatchCode,
		COALESCE(#TempCLT207Data.productCode, #TempMPVData.productCode) AS recentlyObservedProductCode,
		#TempCLT207Data.oldProductCode AS recentlyObservedOldProductCode,
		/*The MPV tables are the source of truth on cost - DanR. 20180724
				To Consider: we could define additional cases for importing CLT207-lineItemUnitCost-Data,
				based on frequency of CLT207 update etc.*/
		COALESCE(#TempMPVData.lineItemUnitCost,#TempCLT207Data.lineItemUnitCost) AS recentlyObservedLineItemCost,
		/*For transactionTypeCode: Potentially Coalesce between CLT00207 and the productTransactionTypeCode of #TempMPVData*/
		#TempCLT207Data.transactionTypeCode,
		@dateInserted AS dateInserted
		INTO #ProductMergeSource
	FROM
		#TempMPVData
		FULL OUTER JOIN #TempCLT207Data
			ON #TempCLT207Data.productTransactionCode = #TempMPVData.productTransactionCode
		FULL OUTER JOIN DecisionNet.ProductHierarchy
			ON ProductHierarchy.productTransactionCode = #TempMPVData.productTransactionCode
				OR ProductHierarchy.productTransactionCode = #TempCLT207Data.productTransactionCode
		OUTER APPLY
		(
			SELECT
				RankedINNERProductHeirarchyRemap.productHierarchyLvl1,
				RankedINNERProductHeirarchyRemap.productHierarchyLvl2,
				RankedINNERProductHeirarchyRemap.productHierarchyLvl3,
				RankedINNERProductHeirarchyRemap.productTransactionDescription
			FROM
				(
					SELECT
						INNERProductHierarchyRemap.productHierarchyLvl1,
						INNERProductHierarchyRemap.productHierarchyLvl2,
						INNERProductHierarchyRemap.productHierarchyLvl3,
						INNERProductHierarchyRemap.productTransactionDescription,
						ROW_NUMBER() OVER(
							PARTITION BY INNERProductHierarchyRemap.productTransactionCodeRemap
								ORDER BY INNERProductHierarchyRemap.dateInserted DESC
						) AS remapByDate
					FROM
						DecisionNet.ProductHierarchy AS INNERProductHierarchyRemap
					WHERE
						INNERProductHierarchyRemap.productTransactionCodeRemap = COALESCE(#TempMPVData.productTransactionCode,#TempCLT207Data.productTransactionCode)
				) AS RankedINNERProductHeirarchyRemap
			WHERE
				RankedINNERProductHeirarchyRemap.remapByDate = 1
		) AS HierarchyRemapLookup
		
		
SELECT  @Log_RecordsAffected = @@Rowcount
Select @Log_ActualProcessedDate = CONVERT(date,getdate())
Set @Log_EndDateTime = GETDATE()
Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
Select @ProductCode, @dateFilterParam, @Log_ActualProcessedDate,3, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

--------------------------------------------------------------------------------------

Set @Log_StepDesc = 'Update DecisionNet.Product'
Set @Log_StartDateTime = GETDATE()	
		
	UPDATE DecisionNet.Product
	SET
		Product.productGroupId = SOURCE.productGroupId,
		Product.productTransactionDescription = SOURCE.productTransactionDescription,
		Product.productHierarchyLvl1 = SOURCE.productHierarchyLvl1,
		Product.productHierarchyLvl2 = SOURCE.productHierarchyLvl2,
		Product.productHierarchyLvl3 = SOURCE.productHierarchyLvl3,
		Product.billableMatchCode = SOURCE.billableMatchCode,
		Product.billableNonMatchCode = SOURCE.billableNonMatchCode,
		Product.recentlyObservedProductCode = SOURCE.recentlyObservedProductCode,
		Product.recentlyObservedOldProductCode = SOURCE.recentlyObservedOldProductCode,
		Product.recentlyObservedLineItemCost = SOURCE.recentlyObservedLineItemCost,
		Product.transactionTypeCode = SOURCE.transactionTypeCode,
		Product.dateInserted = SOURCE.dateInserted
	FROM
		(
			SELECT
				#ProductMergeSource.productTransactionCode,
				CASE
					/*
						Makes you wish the ternary operator existed... added in SQL-SRVR 2012:
						IIF(condition, truthy, falsy)
					*/
					/*
						The following Match conditions are explicitly set based on existing business rules 20180806.
							Should the case be that ProductGroupId values are NOT being derived correctly, these
							comparisons would need to be updated.
					*/
					WHEN
						SUBSTRING(#ProductMergeSource.productHierarchyLvl2,1,6) = 'People'
					THEN
						1 /*People*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Vehicle Data'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Vehicle Sightings'
					THEN
						2 /*Vehicle_Location_Products*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Assets'
					THEN
						3 /*Assets*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Business'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Businesses'
					THEN
						4 /*Business*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Healthcare'
					THEN
						5 /*Healthcare*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Directory Assistance'
					THEN
						6 /*Directory_Assistance*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
					THEN
						7 /*Weather_Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
					THEN
						8 /*Criminal_Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
					THEN
						9 /*Police_Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
					THEN
						10 /*Driver_History*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
					THEN
						11 /*Medical_Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
					THEN
						12 /*ClaimDirector*/
					ELSE
						13 /*OTHER*/
				END AS productGroupId,
				#ProductMergeSource.productTransactionDescription,
				#ProductMergeSource.productHierarchyLvl1,
				#ProductMergeSource.productHierarchyLvl2,
				#ProductMergeSource.productHierarchyLvl3,
				#ProductMergeSource.billableMatchCode,
				#ProductMergeSource.billableNonMatchCode,
				#ProductMergeSource.recentlyObservedProductCode,
				#ProductMergeSource.recentlyObservedOldProductCode,
				#ProductMergeSource.recentlyObservedLineItemCost,
				#ProductMergeSource.transactionTypeCode,
				#ProductMergeSource.dateInserted
			FROM
				#ProductMergeSource
		) AS SOURCE
	WHERE
		Product.productTransactionCode = SOURCE.productTransactionCode
		AND
		(
			Product.productGroupId <> SOURCE.productGroupId
			/*Values for Hierarchy_Lvl, description, and matchCode are guaranteed <> ''; so sentinal value can be used*/
			OR ISNULL(Product.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
			OR ISNULL(Product.productHierarchyLvl1,'') <> ISNULL(SOURCE.productHierarchyLvl1,'')
			OR ISNULL(Product.productHierarchyLvl2,'') <> ISNULL(SOURCE.productHierarchyLvl2,'')
			OR ISNULL(Product.productHierarchyLvl3,'') <> ISNULL(SOURCE.productHierarchyLvl3,'')
			OR ISNULL(Product.billableMatchCode,'') <> ISNULL(SOURCE.billableMatchCode,'')
			OR ISNULL(Product.billableNonMatchCode,'') <> ISNULL(SOURCE.billableNonMatchCode,'')
			OR ISNULL(Product.transactionTypeCode,'') <> ISNULL(SOURCE.transactionTypeCode,'')
			/* recentlyObservedProductCode or recentlyObservedOldProductCode not necessarily consistent and may result in
				unwanted frequency of updates if considered.
				OR ISNULL(TARGET.recentlyObservedProductCode,'') <> ISNULL(SOURCE.recentlyObservedProductCode,'')
				OR ISNULL(TARGET.recentlyObservedOldProductCode,'') <> ISNULL(SOURCE.recentlyObservedOldProductCode,'')
			*/
			/*No obvious sentinal value available*/
			OR(
				Product.recentlyObservedLineItemCost IS NULL AND SOURCE.recentlyObservedLineItemCost IS NOT NULL
				OR Product.recentlyObservedLineItemCost IS NOT NULL AND SOURCE.recentlyObservedLineItemCost IS NULL
				OR Product.recentlyObservedLineItemCost <> SOURCE.recentlyObservedLineItemCost
			)
			/*Dont update if the only difference is the dateInserted
				OR TARGET.dateInserted <> SOURCE.dateInserted
			*/
		)


SELECT  @Log_RecordsAffected = @@Rowcount
Select @Log_ActualProcessedDate = CONVERT(date,getdate())
Set @Log_EndDateTime = GETDATE()
Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
Select @ProductCode, @dateFilterParam, @Log_ActualProcessedDate,4, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

------------------------------------------------------------------------------------------------------------------------------

Set @Log_StepDesc = 'insert into DecisionNet.Product'
Set @Log_StartDateTime = GETDATE()	

	
	/*Explicit LEFT JOIN style for INSERT non-existent records, since set comparison not applicable*/
	INSERT INTO DecisionNet.Product
	(
		productTransactionCode,
		productGroupId,
		productTransactionDescription,
		productHierarchyLvl1,
		productHierarchyLvl2,
		productHierarchyLvl3,
		billableMatchCode,
		billableNonMatchCode,
		recentlyObservedProductCode,
		recentlyObservedOldProductCode,
		recentlyObservedLineItemCost,
		transactionTypeCode,
		dateInserted
	)
	SELECT
		SOURCE.productTransactionCode,
		SOURCE.productGroupId,
		SOURCE.productTransactionDescription,
		SOURCE.productHierarchyLvl1,
		SOURCE.productHierarchyLvl2,
		SOURCE.productHierarchyLvl3,
		SOURCE.billableMatchCode,
		SOURCE.billableNonMatchCode,
		SOURCE.recentlyObservedProductCode,
		SOURCE.recentlyObservedOldProductCode,
		SOURCE.recentlyObservedLineItemCost,
		SOURCE.transactionTypeCode,
		SOURCE.dateInserted
	FROM
		(
			SELECT
				#ProductMergeSource.productTransactionCode,
				CASE
					/*
						Makes you wish the ternary operator existed... added in SQL-SRVR 2012:
						IIF(condition, truthy, falsy)
					*/
					/*
						The following Match conditions are explicitly set based on existing business rules 20180806.
							Should the case be that ProductGroupId values are NOT being derived correctly, these
							comparisons would need to be updated.
					*/
					WHEN
						SUBSTRING(#ProductMergeSource.productHierarchyLvl2,1,6) = 'People'
					THEN
						1 /*People*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Vehicle Data'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Vehicle Sightings'
					THEN
						2 /*Vehicle_Location_Products*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Assets'
					THEN
						3 /*Assets*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Business'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Businesses'
					THEN
						4 /*Business*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Healthcare'
					THEN
						5 /*Healthcare*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Directory Assistance'
					THEN
						6 /*Directory_Assistance*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
					THEN
						7 /*Weather_Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
					THEN
						8 /*Criminal_Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
					THEN
						9 /*Police_Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
					THEN
						10 /*Driver_History*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
					THEN
						11 /*Medical_Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
					THEN
						12 /*ClaimDirector*/
					ELSE
						13 /*OTHER*/
				END AS productGroupId,
				#ProductMergeSource.productTransactionDescription,
				#ProductMergeSource.productHierarchyLvl1,
				#ProductMergeSource.productHierarchyLvl2,
				#ProductMergeSource.productHierarchyLvl3,
				#ProductMergeSource.billableMatchCode,
				#ProductMergeSource.billableNonMatchCode,
				#ProductMergeSource.recentlyObservedProductCode,
				#ProductMergeSource.recentlyObservedOldProductCode,
				#ProductMergeSource.recentlyObservedLineItemCost,
				#ProductMergeSource.transactionTypeCode,
				#ProductMergeSource.dateInserted
			FROM
				#ProductMergeSource
		) AS SOURCE
		LEFT OUTER JOIN DecisionNet.Product
			ON Product.productTransactionCode = SOURCE.productTransactionCode
	WHERE
		Product.productTransactionCode IS NULL
		

SELECT  @Log_RecordsAffected = @@Rowcount
Select @Log_ActualProcessedDate = CONVERT(date,getdate())
Set @Log_EndDateTime = GETDATE()
Set @Log_TimeTaken = convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+convert(varchar(5),DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, @Log_StartDateTime, @Log_EndDateTime)%60)) 

	
Insert into ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log  (ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus, RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc])
Select @ProductCode, @dateFilterParam, @Log_ActualProcessedDate,5, @Log_StepDesc, 'Success', @Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL     

END



--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--/*
--	How are permissions for the Execution of this sproc being controlled?
--*/

--TRUNCATE TABLE DecisionNet.Product;
--SELECT COUNT(*) FROM DecisionNet.Product;

----SET STATISTICS IO ON;
----SET STATISTICS TIME ON;


--EXEC DecisionNet.hsp_UpdateInsertProduct
--	@dateFilterParam = '20080101';
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO

--SET STATISTICS IO OFF;
--SET STATISTICS TIME OFF;

--SELECT COUNT(*) FROM DecisionNet.Product;

--PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*
	
(13 row(s) affected)

(6 row(s) affected)

(6241 row(s) affected)
COMMIT TRANSACTION

SQL Server parse and compile time: 
   CPU time = 0 ms, elapsed time = 3 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.
Table 'CLT00207'. Scan count 65, logical reads 80943, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 90891 ms,  elapsed time = 35762 ms.

(6236 row(s) affected)
Table 'MPV00202'. Scan count 65, logical reads 10984, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 14358 ms,  elapsed time = 3950 ms.

(2000 row(s) affected)
Table '#TempMPVData_________________________________________________________________________________________________00000000314B'. Scan count 65, logical reads 16, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#TempCLT207Data______________________________________________________________________________________________00000000314A'. Scan count 65, logical reads 52, physical reads 0, read-ahead reads 5, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Worktable'. Scan count 3, logical reads 15478, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'ProductHierarchy'. Scan count 14577, logical reads 36479, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 6093 ms,  elapsed time = 587 ms.

(7308 row(s) affected)
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Product'. Scan count 1, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 1 ms.

(0 row(s) affected)
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'ProductGroup'. Scan count 1, logical reads 2, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'TransactionType'. Scan count 1, logical reads 2, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table 'Product'. Scan count 1, logical reads 14776, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
Table '#ProductMergeSource_________________________________________________________________________________________________00000000314C'. Scan count 1, logical reads 113, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 140 ms,  elapsed time = 137 ms.

(7308 row(s) affected)

 SQL Server Execution Times:
   CPU time = 111967 ms,  elapsed time = 40921 ms.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.

(1 row(s) affected)
ROLLBACK TRANSACTION


*/