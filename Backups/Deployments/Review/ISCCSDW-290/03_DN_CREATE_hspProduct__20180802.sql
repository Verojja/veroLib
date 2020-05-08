SET NOEXEC OFF;
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

BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 735) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
*/
/************************************************************************************************************************************************/	
/******************************************************Objects Required for indipendent testing**************************************************/	
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE SCHEMA DecisionNet
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE TABLE DecisionNet.ProductGroup
	--(
	--	productGroupId TINYINT IDENTITY(1,1) NOT NULL,
	--	productGroupName VARCHAR(100) NOT NULL,
	--	productGroupDescription VARCHAR(250) NULL,
	--	CONSTRAINT PK_ProductType_productGroupId
	--		PRIMARY KEY CLUSTERED (productGroupId)
	--);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE TABLE DecisionNet.TransactionType
	--(
	--	transactionTypeCode CHAR(1) NOT NULL,
	--	transactionTypeDescription VARCHAR(75) NULL,
	--	dateInserted DATE NOT NULL,
	--	CONSTRAINT PK_TransactionType_transactionTypeCode
	--		PRIMARY KEY CLUSTERED (transactionTypeCode)
	--);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE TABLE DecisionNet.ProductHierarchy
	--(
	--	productTransactionCode CHAR(4) NOT NULL,
	--	productTransactionDescription VARCHAR(75) NULL,
	--	productTransactionCodeRemap CHAR(4) NULL,
	--	productHierarchy_Lvl1 VARCHAR(75) NOT NULL,
	--	productHierarchy_Lvl2 VARCHAR(75) NOT NULL,
	--	productHierarchy_Lvl3 VARCHAR(75) NOT NULL,
	--	dateInserted DATE NOT NULL,
	--	CONSTRAINT PK_ProductHierarchy_productTransactionCode
	--		PRIMARY KEY CLUSTERED (productTransactionCode)
		
	--);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE NONCLUSTERED INDEX FNIX_ProductHierarchy_productTransactionCodeRemap
	--	ON DecisionNet.ProductHierarchy (productTransactionCodeRemap)
	--		INCLUDE (productTransactionDescription, productHierarchy_Lvl1, productHierarchy_Lvl2, productHierarchy_Lvl3, dateInserted)
	--		WHERE productTransactionCodeRemap IS NOT NULL;
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--CREATE TABLE DecisionNet.Product
	--(
	--	productTransactionCode CHAR(4) NOT NULL,
	--	productGroupId TINYINT NOT NULL,
	--	productTransactionDescription VARCHAR(75) NULL,
	--	productHierarchy_Lvl1 VARCHAR(75) NULL,
	--	productHierarchy_Lvl2 VARCHAR(75) NULL,
	--	productHierarchy_Lvl3 VARCHAR(75) NULL,
	--	billableMatchCode CHAR(1) NULL,
	--	nonBillableMatchCode CHAR(1) NULL,
	--	recentlyObservedProductCode CHAR(9) NULL,
	--	recentlyObservedLineItemCost DECIMAL(17,2) NULL,
	--	transactionTypeCode CHAR(1) NULL
	--		CONSTRAINT FK_Product_TransactionType_transactionTypeCode
	--			FOREIGN KEY REFERENCES DecisionNet.TransactionType (transactionTypeCode),
	--	dateInserted DATE NOT NULL,
	--	CONSTRAINT PK_Product_productTransactionCode
	--		PRIMARY KEY CLUSTERED (productTransactionCode),
	--	CONSTRAINT FK_Product_ProductGroup_productGroupId
	--		FOREIGN KEY (productGroupId) REFERENCES DecisionNet.ProductGroup (productGroupId),
	--	CONSTRAINT CK_Product_billableMatchCode_ASSERT_KnownValueORNULL
	--		CHECK (billableMatchCode IS NULL OR billableMatchCode IN ('Y','N','I')),
	--	CONSTRAINT CK_Product_nonBillableMatchCode_ASSERT_KnownValueORNULL
	--		CHECK (nonBillableMatchCode IS NULL OR billableMatchCode IN ('Y','N','I'))
	--);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--/*Populate_ProductGroup*/
	--INSERT INTO DecisionNet.ProductGroup
	--	(productGroupName, productGroupDescription)
	--VALUES
	--	/*1*/('People', NULL),
	--	/*2*/('Vehicle_Location_Products', NULL),
	--	/*3*/('Assets', NULL),
	--	/*4*/('Business', NULL),
	--	/*5*/('Healthcare', NULL),
	--	/*6*/('Directory_Assistance', NULL),
	--	/*7*/('Weather_Reports', NULL),
	--	/*8*/('Criminal_Records', NULL),
	--	/*9*/('Police_Reports', NULL),
	--	/*10*/('Driver_History', NULL),
	--	/*11*/('Medical_Records', NULL),
	--	/*12*/('ClaimDirector', NULL),
	--	/*13*/('OTHER', NULL);
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--/*Populate_TransactionType*/
	--INSERT INTO DecisionNet.TransactionType
	--	(transactionTypeCode, transactionTypeDescription, dateInserted)
	--SELECT
	--	DN_Lookup_TransactionType.C_TRAN_TYP,
	--	DN_Lookup_TransactionType.T_TRAN_TYP,
	--	CAST(CAST(DN_Lookup_TransactionType.Date_Insert AS VARCHAR(8)) AS DATE) 
	--FROM
	--	[ClaimSearch_Dev].dbo.DN_Lookup_TransactionType
	--WHERE
	--	DN_Lookup_TransactionType.C_TRAN_TYP IS NOT NULL;
	--GO
	--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	--BEGIN
	--	ROLLBACK TRANSACTION;
	--	SET NOEXEC ON;
	--END
	--GO
	--/*Populate_ProductHierarchy*/
	--DECLARE @dateInserted DATE = GETDATE();
	--INSERT INTO DecisionNet.ProductHierarchy
	--(
	--	productTransactionCode,
	--	productTransactionCodeRemap,
	--	productTransactionDescription,
	--	productHierarchy_Lvl1,
	--	productHierarchy_Lvl2,
	--	productHierarchy_Lvl3,
	--	dateInserted
	--)
	--SELECT
	--	CAST(DIM_DN_Product_load9.C_ISO_TRNS AS VARCHAR(4)),
	--	CAST(DIM_DN_Product_load9.C_ISO_TRNS_Remap AS VARCHAR(4)),
	--	CAST(DIM_DN_Product_load9.T_ISO_TRNS AS VARCHAR(75)),
	--	CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
	--	CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
	--	CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
	--	@dateInserted
	--FROM
	--	[ClaimSearch_Dev].dbo.DIM_DN_Product_load9
	--WHERE
	--	DIM_DN_Product_load9.C_ISO_TRNS NOT IN
	--	(
	--		SELECT
	--			Zach_DIM_DN_Product.C_ISO_TRNS
	--		FROM
	--			[ClaimSearch_Dev].dbo.Zach_DIM_DN_Product
	--		UNION
	--		SELECT
	--			DIM_DN_Product_load9.C_ISO_TRNS
	--		FROM
	--			[ClaimSearch_Dev].dbo.DIM_DN_Product_load9
	--		GROUP BY
	--			DIM_DN_Product_load9.C_ISO_TRNS
	--		HAVING
	--			COUNT(*) > 1
	--	)
	--UNION
	--SELECT
	--	Zach_DIM_DN_Product.C_ISO_TRNS,
	--	Zach_DIM_DN_Product.C_ISO_TRNS_Remap,
	--	CAST(Zach_DIM_DN_Product.T_ISO_TRNS AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
	--	@dateInserted
	--FROM
	--	[ClaimSearch_Dev].dbo.Zach_DIM_DN_Product
	--UNION
	--SELECT
	--	Zach_DIM_DN_ProductNULL.C_ISO_TRNS,
	--	Zach_DIM_DN_ProductNULL.C_ISO_TRNS_Remap,
	--	CAST(Zach_DIM_DN_ProductNULL.T_ISO_TRNS AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
	--	CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
	--	@dateInserted
	--FROM
	--	[ClaimSearch_Dev].dbo.Zach_DIM_DN_ProductNULL
	--UNION
	--SELECT
	--	ManualProductHierarchyDeDupset.productTranscationCode,
	--	ManualProductHierarchyDeDupset.productTranscationCodeRemap,
	--	ManualProductHierarchyDeDupset.productDescription,
	--	ManualProductHierarchyDeDupset.productHeirarchyLvl1,
	--	ManualProductHierarchyDeDupset.productHeirarchyLvl2,
	--	ManualProductHierarchyDeDupset.productHeirarchyLvl3,
	--	@dateInserted
	--FROM
	--	(
	--		VALUES
	--			('ABAM', NULL, 'Incarceration Search', 'Public Records', 'People', 'Background Check Options'), 
	--			('ABAO', NULL, 'Multi-State Arrest & Booking Records', 'Public Records', 'People', 'Background Check Options'), 
	--			('ABBW', NULL, 'Wants & Warrants Report ', 'Public Records', 'People', 'Background Check Options'), 
	--			('ACDA', NULL, 'Basic Lookup', 'Public Records', 'Directory Assistance', 'Search Options'), 
	--			('ACWS', NULL, 'Place of Employment Search', 'Public Records', 'People', 'Search Options')
	--	) AS ManualProductHierarchyDeDupset (productTranscationCode, productTranscationCodeRemap, productDescription, productHeirarchyLvl1, productHeirarchyLvl2, productHeirarchyLvl3);
/*****************************************************************************************************************************/	
/*****************************************************************************************************************************/	
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
			
			Performance: Execution appears to be around 14 seconds.
************************************************/
CREATE PROCEDURE DecisionNet.hsp_UpdateInsertProduct
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	CREATE TABLE #IndexedTempMPVData
	(
		productCode CHAR(9) NOT NULL,
		productTransactionCode CHAR(4) NOT NULL
		CONSTRAINT PK_IndexedTempMPVData_productTransactionCode
			PRIMARY KEY CLUSTERED,
		productTransactionTypeCode CHAR(4) NOT NULL,
		productDescription varchar(75) NULL,
		lineItemUnitCost decimal(17,2) NULL
	);
	CREATE TABLE #IndexedTempCLT207Data
	(
		productTransactionCode CHAR(4) NOT NULL
		CONSTRAINT PK_IndexedTempCLT207Data_productTransactionCode
			PRIMARY KEY CLUSTERED,
		productDescription varchar(75) NULL,
		productCode CHAR(9) NOT NULL,
		lineItemUnitCost decimal(17,2) NULL,
		billableMatchCode CHAR(1) NULL,
		nonBillableMatchCode CHAR(1) NULL,
		transactionTypeCode CHAR(1) NULL
	);
	INSERT INTO #IndexedTempCLT207Data
	(
		productTransactionCode,
		productDescription,
		productCode,
		lineItemUnitCost,
		billableMatchCode,
		nonBillableMatchCode,
		transactionTypeCode
	)
	SELECT
		RankedDescriptions.productTransactionCode,
		NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'') AS productDescription,
		RankedDescriptions.productCode,
		RankedDescriptions.lineItemUnitCost,
		NULLIF(LTRIM(RTRIM(RankedDescriptions.billableMatchCode)),'') AS billableMatchCode,
		NULLIF(LTRIM(RTRIM(RankedDescriptions.nonBillableMatchCode)),'') AS nonBillableMatchCode,
		NULLIF(LTRIM(RTRIM(RankedDescriptions.transactionTypeCode)),'') AS transactionTypeCode
	FROM
		(
			SELECT
				DescriptionCompareSet.productTransactionCode,
				DescriptionCompareSet.productDescription,
				DescriptionCompareSet.productCode,
				DescriptionCompareSet.lineItemUnitCost,
				DescriptionCompareSet.billableMatchCode,
				DescriptionCompareSet.nonBillableMatchCode,
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
					CLT00207.A_ISO_TRNS_LIST AS lineItemUnitCost,
					CLT00207.F_BILL_MTCH AS billableMatchCode,
					CLT00207.F_BILL_NO_MTCH AS nonBillableMatchCode,
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
					[ClaimSearch_Prod].dbo.CLT00207
			) AS DescriptionCompareSet
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1;
		
	INSERT INTO #IndexedTempMPVData
	(
		productCode,
		productTransactionCode,
		productTransactionTypeCode,
		productDescription,
		lineItemUnitCost
	)
	SELECT
		RankedDescriptions.productCode,
		RankedDescriptions.productTransactionCode, 
		LEFT(RankedDescriptions.productCode,4) AS productTransactionTypeCode,
		NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'') AS productDescription,
		RankedDescriptions.lineItemUnitCost
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
						[ClaimSearch_Prod].dbo.MPV00202
				) AS  DescriptionCompareSet
			WHERE
				DescriptionCompareSet.descriptionCompareSetVale = 1
		) AS RankedDescriptions
	WHERE
		RankedDescriptions.productDescription_rowNumber = 1;
	
	CREATE TABLE #ProductMergeSource
	(
		productTransactionCode CHAR(4) NOT NULL,
		productTransactionDescription VARCHAR(75) NULL,
		productHierarchy_Lvl1 VARCHAR(75) NULL,
		productHierarchy_Lvl2 VARCHAR(75) NULL,
		productHierarchy_Lvl3 VARCHAR(75) NULL,
		billableMatchCode CHAR(1) NULL,
		nonBillableMatchCode CHAR(1) NULL,
		recentlyObservedProductCode CHAR(9) NULL,
		recentlyObservedLineItemCost DECIMAL(17,2) NULL,
		transactionTypeCode CHAR(1) NULL,
		dateInserted DATE NOT NULL,
		CONSTRAINT PK_Product_productTransactionCode
			PRIMARY KEY CLUSTERED (productTransactionCode)
	);
	INSERT INTO #ProductMergeSource
	(
		productTransactionCode,
		/*GroupId is calculated in the SourceOfTheMerge (following section)
			based on the flattened Heirarchy data.*/
		productHierarchy_Lvl1,
		productHierarchy_Lvl2,
		productHierarchy_Lvl3,
		productTransactionDescription,
		billableMatchCode,
		nonBillableMatchCode,
		recentlyObservedProductCode,
		recentlyObservedLineItemCost,
		transactionTypeCode,
		dateInserted
	)
	SELECT
		COALESCE(
			ProductHierarchy.productTransactionCode,
			#IndexedTempCLT207Data.productTransactionCode,
			#IndexedTempMPVData.productTransactionCode
		) AS productTransactionCode,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchy_Lvl1)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchy_Lvl1)),'')
		) AS productHierarchy_Lvl1,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchy_Lvl2)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchy_Lvl2)),'')
		) AS productHierarchy_Lvl2,
		COALESCE(
			NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchy_Lvl3)),''),
			NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchy_Lvl3)),'')
		) AS productHierarchy_Lvl3,
		COALESCE(
				COALESCE(
					NULLIF(LTRIM(RTRIM(ProductHierarchy.productTransactionDescription)),''),
					NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productTransactionDescription)),'')
				),
				#IndexedTempCLT207Data.productDescription,
				#IndexedTempMPVData.productDescription
		) AS productTransactionDescription,
		#IndexedTempCLT207Data.billableMatchCode,
		#IndexedTempCLT207Data.nonBillableMatchCode,
		COALESCE(#IndexedTempCLT207Data.productCode, #IndexedTempMPVData.productCode) AS recentlyObservedProductCode,
		/*The MPV tables are the source of truth on cost - DanR. 20180724
				To Consider: we could define additional cases for importing CLT207-lineItemUnitCost-Data,
				based on frequency of CLT207 update etc.*/
		COALESCE(#IndexedTempMPVData.lineItemUnitCost,#IndexedTempCLT207Data.lineItemUnitCost) AS recentlyObservedLineItemCost,
		/*For transactionTypeCode: Potentially Coalesce between CLT00207 and the productTransactionTypeCode of #IndexedTempMPVData*/
		#IndexedTempCLT207Data.transactionTypeCode,
		@dateInserted
	FROM
		#IndexedTempMPVData
		FULL OUTER JOIN #IndexedTempCLT207Data
			ON #IndexedTempCLT207Data.productTransactionCode = #IndexedTempMPVData.productTransactionCode
		FULL OUTER JOIN DecisionNet.ProductHierarchy
			ON ProductHierarchy.productTransactionCode = #IndexedTempMPVData.productTransactionCode
				OR ProductHierarchy.productTransactionCode = #IndexedTempCLT207Data.productTransactionCode
		OUTER APPLY
		(
			SELECT
				RankedINNERProductHeirarchyRemap.productHierarchy_Lvl1,
				RankedINNERProductHeirarchyRemap.productHierarchy_Lvl2,
				RankedINNERProductHeirarchyRemap.productHierarchy_Lvl3,
				RankedINNERProductHeirarchyRemap.productTransactionDescription
			FROM
				(
					SELECT
						INNERProductHierarchyRemap.productHierarchy_Lvl1,
						INNERProductHierarchyRemap.productHierarchy_Lvl2,
						INNERProductHierarchyRemap.productHierarchy_Lvl3,
						INNERProductHierarchyRemap.productTransactionDescription,
						ROW_NUMBER() OVER(
							PARTITION BY INNERProductHierarchyRemap.productTransactionCodeRemap
								ORDER BY INNERProductHierarchyRemap.dateInserted DESC
						) AS remapByDate
					FROM
						DecisionNet.ProductHierarchy AS INNERProductHierarchyRemap
					WHERE
						INNERProductHierarchyRemap.productTransactionCodeRemap = COALESCE(#IndexedTempMPVData.productTransactionCode,#IndexedTempCLT207Data.productTransactionCode)
				) AS RankedINNERProductHeirarchyRemap
			WHERE
				RankedINNERProductHeirarchyRemap.remapByDate = 1
		) AS HierarchyRemapLookup;
		
	MERGE INTO DecisionNet.Product AS TARGET
	USING
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
					SUBSTRING(#ProductMergeSource.productHierarchy_Lvl2,1,6) = 'People'
				THEN
					1 /*People*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Vehicle Data'
					OR #ProductMergeSource.productHierarchy_Lvl2 = 'Vehicle Sightings'
				THEN
					2 /*Vehicle_Location_Products*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Assets'
				THEN
					3 /*Assets*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Business'
					OR #ProductMergeSource.productHierarchy_Lvl2 = 'Businesses'
				THEN
					4 /*Business*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Healthcare'
				THEN
					5 /*Healthcare*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Directory Assistance'
				THEN
					6 /*Directory_Assistance*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Weather Report'
					OR #ProductMergeSource.productHierarchy_Lvl2 = 'Weather Reports'
				THEN
					7 /*Weather_Reports*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Criminal Records'
				THEN
					8 /*Criminal_Records*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Police Reports'
				THEN
					9 /*Police_Reports*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Driver History'
				THEN
					10 /*Driver_History*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'Medical Records'
				THEN
					11 /*Medical_Records*/
				WHEN
					#ProductMergeSource.productHierarchy_Lvl2 = 'ClaimDirector'
				THEN
					12 /*ClaimDirector*/
				ELSE
					13 /*OTHER*/
			END AS productGroupId,
			#ProductMergeSource.productTransactionDescription,
			#ProductMergeSource.productHierarchy_Lvl1,
			#ProductMergeSource.productHierarchy_Lvl2,
			#ProductMergeSource.productHierarchy_Lvl3,
			#ProductMergeSource.billableMatchCode,
			#ProductMergeSource.nonBillableMatchCode,
			#ProductMergeSource.recentlyObservedProductCode,
			#ProductMergeSource.recentlyObservedLineItemCost,
			#ProductMergeSource.transactionTypeCode,
			#ProductMergeSource.dateInserted
		FROM
			#ProductMergeSource
	) AS SOURCE
		ON TARGET.productTransactionCode = SOURCE.productTransactionCode
	WHEN MATCHED
		AND
		(
			TARGET.productGroupId <> SOURCE.productGroupId
			/*Values for Hierarchy_Lvl, description, and matchCode are guaranteed <> ''; so sentinal value can be used*/
			OR ISNULL(TARGET.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
			OR ISNULL(TARGET.productHierarchy_Lvl1,'') <> ISNULL(SOURCE.productHierarchy_Lvl1,'')
			OR ISNULL(TARGET.productHierarchy_Lvl2,'') <> ISNULL(SOURCE.productHierarchy_Lvl2,'')
			OR ISNULL(TARGET.productHierarchy_Lvl3,'') <> ISNULL(SOURCE.productHierarchy_Lvl3,'')
			OR ISNULL(TARGET.billableMatchCode,'') <> ISNULL(SOURCE.billableMatchCode,'')
			OR ISNULL(TARGET.nonBillableMatchCode,'') <> ISNULL(SOURCE.nonBillableMatchCode,'')
			OR ISNULL(TARGET.transactionTypeCode,'') <> ISNULL(SOURCE.transactionTypeCode,'')
			/* recentlyObservedProductCode not necessarily consistent and may result in
				unwanted frequency of updates if considered.
				OR ISNULL(TARGET.recentlyObservedProductCode,'') <> ISNULL(SOURCE.recentlyObservedProductCode,'')
			*/
			/*No obvious sentinal value available*/
			OR(
				TARGET.recentlyObservedLineItemCost IS NULL AND SOURCE.recentlyObservedLineItemCost IS NOT NULL
				OR TARGET.recentlyObservedLineItemCost IS NOT NULL AND SOURCE.recentlyObservedLineItemCost IS NULL
				OR TARGET.recentlyObservedLineItemCost <> SOURCE.recentlyObservedLineItemCost
			)
			/*Dont update if the only difference is the dateInserted
				OR TARGET.dateInserted <> SOURCE.dateInserted
			*/
		)
	THEN UPDATE
	SET
		TARGET.productGroupId = SOURCE.productGroupId,
		TARGET.productTransactionDescription = SOURCE.productTransactionDescription,
		TARGET.productHierarchy_Lvl1 = SOURCE.productHierarchy_Lvl1,
		TARGET.productHierarchy_Lvl2 = SOURCE.productHierarchy_Lvl2,
		TARGET.productHierarchy_Lvl3 = SOURCE.productHierarchy_Lvl3,
		TARGET.billableMatchCode = SOURCE.billableMatchCode,
		TARGET.nonBillableMatchCode = SOURCE.nonBillableMatchCode,
		TARGET.recentlyObservedProductCode = SOURCE.recentlyObservedProductCode,
		TARGET.recentlyObservedLineItemCost = SOURCE.recentlyObservedLineItemCost,
		TARGET.transactionTypeCode = SOURCE.transactionTypeCode,
		TARGET.dateInserted = SOURCE.dateInserted
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
	(
		productTransactionCode,
		productGroupId,
		productTransactionDescription,
		productHierarchy_Lvl1,
		productHierarchy_Lvl2,
		productHierarchy_Lvl3,
		billableMatchCode,
		nonBillableMatchCode,
		recentlyObservedProductCode,
		recentlyObservedLineItemCost,
		transactionTypeCode,
		dateInserted
	)
	VALUES
	(
		SOURCE.productTransactionCode,
		SOURCE.productGroupId,
		SOURCE.productTransactionDescription,
		SOURCE.productHierarchy_Lvl1,
		SOURCE.productHierarchy_Lvl2,
		SOURCE.productHierarchy_Lvl3,
		SOURCE.billableMatchCode,
		SOURCE.nonBillableMatchCode,
		SOURCE.recentlyObservedProductCode,
		SOURCE.recentlyObservedLineItemCost,
		SOURCE.transactionTypeCode,
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
--/*
--	How are permissions for the Execution of this sproc being controlled?
--*/
--EXEC DecisionNet.hsp_UpdateInsertProduct;
--SELECT COUNT(*) AS onlyOnHeirarchyCount FROM DecisionNet.Product WHERE Product.recentlyObservedProductCode IS NULL;
--SELECT COUNT(*) AS totalCount FROM DecisionNet.Product;
--SELECT COUNT(*) AS notOnHeirarchyCount FROM DecisionNet.Product WHERE Product.productHierarchy_Lvl1 IS NULL;
--SELECT * FROM DecisionNet.Product;

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/