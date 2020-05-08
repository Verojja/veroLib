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
ALTER PROCEDURE [DecisionNet].[hsp_UpdateInsertProduct]
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	SET @dateFilterParam = COALESCE(@dateFilterParam, DATEADD(DAY,-1,GETDATE()));

	SELECT
		CAST(RankedDescriptions.productTransactionCode AS CHAR(4)) AS productTransactionCode, 
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'')AS VARCHAR(75)) AS productDescription,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productCode)),'') AS CHAR(9)) AS productCode,
		CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.oldProductCode)),'') AS CHAR(9)) AS oldProductCode,
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
				MostRecentlyObservedCLTData.productCode,
				MostRecentlyObservedCLTData.oldProductCode,
				MostRecentlyObservedCLTData.lineItemUnitCost,
				MostRecentlyObservedCLTData.billableMatchCode,
				MostRecentlyObservedCLTData.billableNonMatchCode,
				MostRecentlyObservedCLTData.transactionTypeCode,
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
						MostRecentlyObservedCLTData.Date_Insert DESC
				) AS productDescription_rowNumber
			FROM
			(
				SELECT
					INNERCLT00207.C_ISO_TRNS AS productTransactionCode,
					INNERCLT00207.T_ISO_TRNS AS productDescription,
					ROW_NUMBER() OVER(
						PARTITION BY
							INNERCLT00207.C_ISO_TRNS, INNERCLT00207.T_ISO_TRNS
						ORDER BY
							CASE
								WHEN
									(INNERCLT00207.T_ISO_TRNS LIKE '%[0-9][0-9][0-9][0-9]%')
								THEN
									1
								ELSE
									0
							END,
							INNERCLT00207.Date_Insert DESC
					) AS DescriptionCompareSetVale
				FROM
					[ClaimSearch_Prod].dbo.CLT00207 AS INNERCLT00207 WITH (NOLOCK)
				/*WHERE Do to an inconsistency between CLT207's currentstateprocess and the currentstateprocess of MPV,
						Information is lost if this filter is observed. 20181004
					CAST(CAST(INNERCLT00207.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam*/
			) AS DescriptionCompareSet
			CROSS APPLY
			(
				SELECT TOP (1)
					RecentINNERCLT00207.C_PS_PRD AS productCode,
					RecentINNERCLT00207.C_PS_PRD_OLD AS oldProductCode,
					RecentINNERCLT00207.A_ISO_TRNS_LIST AS lineItemUnitCost,
					RecentINNERCLT00207.C_TRAN_TYP AS transactionTypeCode,
					RecentINNERCLT00207.Date_Insert,
					RecentINNERCLT00207.F_BILL_MTCH AS billableMatchCode,
					RecentINNERCLT00207.F_BILL_NO_MTCH AS billableNonMatchCode
				FROM
					[ClaimSearch_Prod].dbo.CLT00207 AS RecentINNERCLT00207 WITH (NOLOCK)
				WHERE
					RecentINNERCLT00207.C_ISO_TRNS = DescriptionCompareSet.productTransactionCode
				ORDER BY
					RecentINNERCLT00207.Date_Insert DESC
			) MostRecentlyObservedCLTData
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1
	
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
				MostRecentlyObservedMPVData.productCode,
				DescriptionCompareSet.productDescription,
				MostRecentlyObservedMPVData.dateLineItemExractRun,
				DescriptionCompareSet.descriptionCompareSetVale,
				MostRecentlyObservedMPVData.lineItemUnitCost,
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
						MostRecentlyObservedMPVData.dateLineItemExractRun DESC
				) AS productDescription_rowNumber
			FROM
				(
					SELECT
						RIGHT(MPV00202.I_PRD,4) AS productTransactionCode,
						MPV00202.T_PRD_DSC AS productDescription,
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
						) AS descriptionCompareSetVale
					FROM
						[ClaimSearch_Prod].dbo.MPV00202 WITH (NOLOCK)
					/*WHERE Do to an inconsistency between CLT207's currentstateprocess and the currentstateprocess of MPV,
						Information is lost if this filter is observed. 20181004
					CAST(CAST(INNERCLT00207.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam*/
				) AS DescriptionCompareSet
				CROSS APPLY
				(
					SELECT TOP (1)
						RecentINNERMPV00202.I_PRD AS productCode,
						RecentINNERMPV00202.A_LN_ITM_UNIT AS lineItemUnitCost,
						CAST(
							SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,1,10)
							+ ' '
							+ REPLACE((SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
							+ (SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,20,8))
							AS DATETIME2(5)
						)AS dateLineItemExractRun
					FROM
						[ClaimSearch_Prod].dbo.MPV00202 AS RecentINNERMPV00202 WITH (NOLOCK)
					WHERE
						RIGHT(RecentINNERMPV00202.I_PRD,4) = DescriptionCompareSet.productTransactionCode
					ORDER BY
						RecentINNERMPV00202.Date_Insert DESC
				) MostRecentlyObservedMPVData
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1;
	
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
						2 /*Vehicle*/
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
						6 /*Directory Assistance*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
					THEN
						7 /*Weather Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
					THEN
						8 /*Criminal\Civil*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
					THEN
						9 /*Police Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
					THEN
						10 /*Driver History*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
					THEN
						11 /*Medical Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
					THEN
						12 /*ClaimDirector*/
					
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Court Fees'
					THEN
						14 /*Court Fees*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Agency Fees'
					THEN
						15 /*Agency Fees*/
						WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'OFAC'
					THEN
						16 /*OFAC*/
						WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Append-DS'
					THEN
						17 /*Append-DS*/

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
			OR ISNULL(Product.recentlyObservedProductCode,'') <> ISNULL(SOURCE.recentlyObservedProductCode,'')
			/* recentlyObservedProductCode or recentlyObservedOldProductCode not necessarily consistent and may result in
				unwanted frequency of updates if considered.
				OR ISNULL(Product.recentlyObservedOldProductCode,'') <> ISNULL(SOURCE.recentlyObservedOldProductCode,'')
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
						2 /*Vehicle*/
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
						6 /*Directory Assistance*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
						OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
					THEN
						7 /*Weather Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
					THEN
						8 /*Criminal\Civil*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
					THEN
						9 /*Police Reports*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
					THEN
						10 /*Driver History*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
					THEN
						11 /*Medical Records*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
					THEN
						12 /*ClaimDirector*/
					
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Court Fees'
					THEN
						14 /*Court Fees*/
					WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Agency Fees'
					THEN
						15 /*Agency Fees*/
						WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'OFAC'
					THEN
						16 /*OFAC*/
						WHEN
						#ProductMergeSource.productHierarchyLvl2 = 'Append-DS'
					THEN
						17 /*Append-DS*/

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
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

SELECT COUNT(*) FROM DecisionNet.Product;
TRUNCATE TABLE DecisionNet.Product;

EXEC DecisionNet.hsp_UpdateInsertProduct
	@dateFilterParam = '20140101';

SELECT productTransactionCode,
productGroupId,
productTransactionDescription,
productHierarchyLvl1,
productHierarchyLvl2,
productHierarchyLvl3,
billableMatchCode,
billableNonMatchCode
transactionTypeCode FROM DecisionNet.Product
EXCEPT
SELECT productTransactionCode,
productGroupId,
productTransactionDescription,
productHierarchyLvl1,
productHierarchyLvl2,
productHierarchyLvl3,
billableMatchCode,
billableNonMatchCode
transactionTypeCode FROM [ClaimSearch_Prod].DecisionNet.Product;

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;