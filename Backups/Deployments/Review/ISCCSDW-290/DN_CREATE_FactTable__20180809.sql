SET NOEXEC OFF;
/*
	This script will instantiate the support and dimension table(s); it is NOT designed
		to be made into a job/sproc or automated in any way.
		It will only be executed a single time to CREATE several tables / INDEXES.
	
	Execution of this script relies on zero data on tables. IE: there is NO required data refresh
	for existing production data, as is the case with the hps_ scripts.
	
	Note: At the time of script-submission, GRANT / DENY permission(s) statements were NOT included.
*/


BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 193) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
*/

/************************************************************************************************************************************************/
/******************************************************Objects Required for indipendent testing**************************************************/
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	CREATE SCHEMA DecisionNet
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	/*See Populate_ProductGroup on Initialize script.*/
	CREATE TABLE DecisionNet.ProductGroup
	(
		productGroupId TINYINT IDENTITY(1,1) NOT NULL
			CONSTRAINT PK_ProductType_productTypeId PRIMARY KEY CLUSTERED,
		productGroupName VARCHAR(100) NOT NULL,
		productGroupDescription VARCHAR(250) NULL
	);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	/*See Populate_TransactionType on Initialize script.*/
	CREATE TABLE DecisionNet.TransactionType
	(
		transactionTypeCode CHAR(1) NOT NULL
			CONSTRAINT PK_TransactionType_transactionTypeCode PRIMARY KEY CLUSTERED,
		transactionTypeDescription VARCHAR(75) NULL,
		dateInserted DATE NOT NULL
	);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	/*See Populate_ProductGroup on Initialize script.*/
	CREATE TABLE DecisionNet.ProductHierarchy
	(
		productTransactionCode CHAR(4) NOT NULL
			CONSTRAINT PK_ProductHierarchy_productTransactionCode PRIMARY KEY CLUSTERED,
		productTransactionDescription VARCHAR(75) NULL,
		productTransactionCodeRemap CHAR(4) NULL,
		productHierarchy_Lvl1 VARCHAR(75) NOT NULL,
		productHierarchy_Lvl2 VARCHAR(75) NOT NULL,
		productHierarchy_Lvl3 VARCHAR(75) NOT NULL,
		dateInserted DATE NOT NULL
	);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	CREATE /*UNIQUE = FUC_; NonUnq = FNIX_ (F is due to filter)*/ NONCLUSTERED INDEX FNIX_ProductHierarchy_productTransactionCodeRemap
		ON DecisionNet.ProductHierarchy (productTransactionCodeRemap)
			INCLUDE (productTransactionDescription, productHierarchy_Lvl1, productHierarchy_Lvl2, productHierarchy_Lvl3, dateInserted)
			WHERE productTransactionCodeRemap IS NOT NULL;
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	CREATE TABLE DecisionNet.Product
	(
		productTransactionCode CHAR(4) NOT NULL
			CONSTRAINT PK_Product_productTransactionCode
				PRIMARY KEY CLUSTERED (productTransactionCode),
		productGroupId TINYINT NOT NULL
			CONSTRAINT PK_Product_productTransactionCode_productGroupId
				FOREIGN KEY REFERENCES DecisionNet.ProductGroup (productGroupId),
		productTransactionDescription VARCHAR(75) NULL,
		productHierarchy_Lvl1 VARCHAR(75) NULL,
		productHierarchy_Lvl2 VARCHAR(75) NULL,
		productHierarchy_Lvl3 VARCHAR(75) NULL,
		billableMatchCode CHAR(1) NULL,
		nonBillableMatchCode CHAR(1) NULL,
		recentlyObservedProductCode CHAR(9) NULL,
		recentlyObservedLineItemCost DECIMAL(17,2) NULL,
		transactionTypeCode CHAR(1) NULL
			CONSTRAINT FK_Product_TransactionType_transactionTypeCode
				FOREIGN KEY REFERENCES DecisionNet.TransactionType (transactionTypeCode),
		dateInserted DATE NOT NULL,
		CONSTRAINT CC_Product_billableMatchCode_ASSERT_KnownValueORNULL
			CHECK (billableMatchCode IS NULL OR billableMatchCode IN ('Y','N','I')),
		CONSTRAINT CC_Product_nonBillableMatchCode_ASSERT_KnownValueORNULL
			CHECK (nonBillableMatchCode IS NULL OR billableMatchCode IN ('Y','N','I'))
	);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	CREATE TABLE DecisionNet.Expenditure /*Revenue*/
	(
		invoiceNumber VARCHAR(22) NOT NULL,
		lineItemNumber INT NOT NULL,
		CONSTRAINT PK_Expenditure_invoiceNumber_lineItemNumber
			PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber),
		invoiceDate DATE NOT NULL,
		companySoldToCode CHAR(4) NOT NULL,
		officeSoldToCode CHAR(5) NULL,
		companyShippedToCode CHAR(4) NOT NULL,
		officeShippedToCode CHAR(5) NULL,
		productCode CHAR(9) NOT NULL,
		productTransactionTypeCode AS LEFT(productCode,4),
		productTransactionCode AS RIGHT(productCode,4),
		lineItemQuantity INT NOT NULL,
		lineItemUnitCost DECIMAL(17,2) NOT NULL,
		lineItemTax DECIMAL(17,2) NOT NULL,
		dateInserted DATE NOT NULL
	);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	CREATE NONCLUSTERED INDEX NIX_Expenditure_productTransactionCode
		ON DecisionNet.Expenditure (productTransactionCode)
			INCLUDE (companySoldToCode, companyShippedToCode, lineItemUnitCost);
	CREATE NONCLUSTERED INDEX NIX_Expenditure_invoiceDate
		ON DecisionNet.Expenditure (invoiceDate)
			INCLUDE (productCode, productTransactionTypeCode, productTransactionCode);
	CREATE NONCLUSTERED INDEX NIX_Expenditure_companySoldToCode
		ON DecisionNet.Expenditure (companySoldToCode)
			INCLUDE (officeSoldToCode);
	CREATE NONCLUSTERED INDEX NIX_Expenditure_companyShippedToCode
		ON DecisionNet.Expenditure (companyShippedToCode)
			INCLUDE (officeShippedToCode);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	/***************************************************/
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	INSERT INTO DecisionNet.ProductGroup
		(productGroupName, productGroupDescription)
	VALUES
		/*1*/('People', NULL),
		/*2*/('Vehicle_Location_Products', NULL),
		/*3*/('Assets', NULL),
		/*4*/('Business', NULL),
		/*5*/('Healthcare', NULL),
		/*6*/('Directory_Assistance', NULL),
		/*7*/('Weather_Reports', NULL),
		/*8*/('Criminal_Records', NULL),
		/*9*/('Police_Reports', NULL),
		/*10*/('Driver_History', NULL),
		/*11*/('Medical_Records', NULL),
		/*12*/('ClaimDirector', NULL),
		/*13*/('OTHER', NULL);
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	INSERT INTO DecisionNet.TransactionType
		(transactionTypeCode, transactionTypeDescription, dateInserted)
	SELECT
		DN_Lookup_TransactionType.C_TRAN_TYP,
		DN_Lookup_TransactionType.T_TRAN_TYP,
		CAST(CAST(DN_Lookup_TransactionType.Date_Insert AS VARCHAR(8)) AS DATE) 
	FROM
		[ClaimSearch_Dev].dbo.DN_Lookup_TransactionType
	WHERE
		DN_Lookup_TransactionType.C_TRAN_TYP IS NOT NULL;
	GO
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
	DECLARE @dateInserted DATE = GETDATE();
	INSERT INTO DecisionNet.ProductHierarchy
	(
		productTransactionCode,
		productTransactionCodeRemap,
		productTransactionDescription,
		productHierarchy_Lvl1,
		productHierarchy_Lvl2,
		productHierarchy_Lvl3,
		dateInserted
	)
	SELECT
		CAST(DIM_DN_Product_load9.C_ISO_TRNS AS VARCHAR(4)),
		CAST(DIM_DN_Product_load9.C_ISO_TRNS_Remap AS VARCHAR(4)),
		CAST(DIM_DN_Product_load9.T_ISO_TRNS AS VARCHAR(75)),
		CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
		CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
		CAST(DIM_DN_Product_load9.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
		@dateInserted
	FROM
		[ClaimSearch_Dev].dbo.DIM_DN_Product_load9
	WHERE
		DIM_DN_Product_load9.C_ISO_TRNS NOT IN
		(
			SELECT
				Zach_DIM_DN_Product.C_ISO_TRNS
			FROM
				[ClaimSearch_Dev].dbo.Zach_DIM_DN_Product
			UNION
			SELECT
				DIM_DN_Product_load9.C_ISO_TRNS
			FROM
				[ClaimSearch_Dev].dbo.DIM_DN_Product_load9
			GROUP BY
				DIM_DN_Product_load9.C_ISO_TRNS
			HAVING
				COUNT(*) > 1
		)
	UNION
	SELECT
		Zach_DIM_DN_Product.C_ISO_TRNS,
		Zach_DIM_DN_Product.C_ISO_TRNS_Remap,
		CAST(Zach_DIM_DN_Product.T_ISO_TRNS AS VARCHAR(75)),
		CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
		CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
		CAST(Zach_DIM_DN_Product.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
		@dateInserted
	FROM
		[ClaimSearch_Dev].dbo.Zach_DIM_DN_Product
	UNION
	SELECT
		Zach_DIM_DN_ProductNULL.C_ISO_TRNS,
		Zach_DIM_DN_ProductNULL.C_ISO_TRNS_Remap,
		CAST(Zach_DIM_DN_ProductNULL.T_ISO_TRNS AS VARCHAR(75)),
		CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL1 AS VARCHAR(75)),
		CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL2 AS VARCHAR(75)),
		CAST(Zach_DIM_DN_ProductNULL.C_PROD_HIERARCHY_LVL3 AS VARCHAR(75)),
		@dateInserted
	FROM
		[ClaimSearch_Dev].dbo.Zach_DIM_DN_ProductNULL
	UNION
	SELECT
		ManualProductHierarchyDeDupset.productTranscationCode,
		ManualProductHierarchyDeDupset.productTranscationCodeRemap,
		ManualProductHierarchyDeDupset.productDescription,
		ManualProductHierarchyDeDupset.productHeirarchyLvl1,
		ManualProductHierarchyDeDupset.productHeirarchyLvl2,
		ManualProductHierarchyDeDupset.productHeirarchyLvl3,
		@dateInserted
	FROM
		(
			VALUES
				('ABAM', NULL, 'Incarceration Search', 'Public Records', 'People', 'Background Check Options'), 
				('ABAO', NULL, 'Multi-State Arrest & Booking Records', 'Public Records', 'People', 'Background Check Options'), 
				('ABBW', NULL, 'Wants & Warrants Report ', 'Public Records', 'People', 'Background Check Options'), 
				('ACDA', NULL, 'Basic Lookup', 'Public Records', 'Directory Assistance', 'Search Options'), 
				('ACWS', NULL, 'Place of Employment Search', 'Public Records', 'People', 'Search Options')
		) AS ManualProductHierarchyDeDupset (productTranscationCode, productTranscationCodeRemap, productDescription, productHeirarchyLvl1, productHeirarchyLvl2, productHeirarchyLvl3);
	GO
	/***************************************************/
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO
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
		productTransactionCode CHAR(4) NOT NULL
			CONSTRAINT PK_Product_productTransactionCode
				PRIMARY KEY CLUSTERED (productTransactionCode),
		productTransactionDescription VARCHAR(75) NULL,
		productHierarchy_Lvl1 VARCHAR(75) NULL,
		productHierarchy_Lvl2 VARCHAR(75) NULL,
		productHierarchy_Lvl3 VARCHAR(75) NULL,
		billableMatchCode CHAR(1) NULL,
		nonBillableMatchCode CHAR(1) NULL,
		recentlyObservedProductCode CHAR(9) NULL,
		recentlyObservedLineItemCost DECIMAL(17,2) NULL,
		transactionTypeCode CHAR(1) NULL,
		dateInserted DATE NOT NULL
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
	/***************************************************/
	IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
	BEGIN
		ROLLBACK TRANSACTION;
		SET NOEXEC ON;
	END
	GO	
	DECLARE @dateInserted DATE = GETDATE();
	CREATE TABLE #IndexedTempMPVDataExpenditure
	(
		invoiceNumber VARCHAR(22) NOT NULL,
		lineItemNumber INT NOT NULL,
		CONSTRAINT PK_IndexedTempMPVData_invoiceNumber_lineItemNumber
			PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber),
		invoiceDate DATE NOT NULL,

		companySoldToCode VARCHAR(5) NOT NULL,
		officeSoldToCode VARCHAR(5) NOT NULL,

		companyShippedToCode VARCHAR(5) NOT NULL,
		officeShippedToCode VARCHAR(5) NOT NULL,

		productCode CHAR(9) NOT NULL,
		lineItemQuantity INT NULL,
		lineItemUnitCost decimal(17,2) NULL,
		lineItemTax decimal(17,2) NULL,
		dateInserted DATE NOT NULL
	);
	INSERT INTO #IndexedTempMPVDataExpenditure
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
	SELECT DISTINCT /*There are Currently 14,493 EXACT DUPLICATE datarows in MPV00202*/
		MPV00202.N_INV_NO AS invoiceNumber,
		MPV00202.N_INV_LN_ITM AS lineItemNumber,
		MPV00200.invoiceDate AS invoiceDate,
		SUBSTRING(MPV00202.I_CST_SOLD,1,4) AS companySoldToCode,
		SUBSTRING(MPV00202.I_CST_SOLD,5,5) AS officeSoldToCode,
			--	office_sold.[MCITY] as I_REGOFF_SOLD_CITY,
			--	office_sold.[MST] AS I_REGOFF_SOLD_STATE,
			--	office_sold.[MZIP] AS I_REGOFF_SOLD_ZIP,
		SUBSTRING(MPV00202.I_CST_SHP,1,4) AS companyShippedToCode,
		SUBSTRING(MPV00202.I_CST_SHP,5,5) AS officeShippedToCode,
			--	office_shipped.[MCITY] as I_REGOFF_SHIPPED_CITY,
			--	office_shipped.[MST] AS I_REGOFF_SHIPPED_STATE,
			--	office_shipped.[MZIP] AS I_REGOFF_SHIPPED_ZIP,
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
			#IndexedTempMPVDataExpenditure.invoiceNumber,
			#IndexedTempMPVDataExpenditure.lineItemNumber,
			#IndexedTempMPVDataExpenditure.invoiceDate,
			#IndexedTempMPVDataExpenditure.companySoldToCode,
			#IndexedTempMPVDataExpenditure.officeSoldToCode,
			#IndexedTempMPVDataExpenditure.companyShippedToCode,
			#IndexedTempMPVDataExpenditure.officeShippedToCode,
			#IndexedTempMPVDataExpenditure.productCode,
			#IndexedTempMPVDataExpenditure.lineItemQuantity,
			#IndexedTempMPVDataExpenditure.lineItemUnitCost,
			#IndexedTempMPVDataExpenditure.lineItemTax,
			#IndexedTempMPVDataExpenditure.dateInserted
		FROM
			#IndexedTempMPVDataExpenditure
	) AS SOURCE
		ON TARGET.invoiceNumber = SOURCE.invoiceNumber
		AND TARGET.lineItemNumber = SOURCE.lineItemNumber
	WHEN MATCHED
		AND
		(
			TARGET.invoiceDate <> SOURCE.invoiceDate
			OR TARGET.companySoldToCode <> SOURCE.companySoldToCode
			OR ISNULL(TARGET.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
			OR TARGET.companyShippedToCode <> SOURCE.companyShippedToCode
			OR ISNULL(TARGET.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
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
	/***************************************************/
/************************************************************************************************************************************************/
/************************************************************************************************************************************************/
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
	Quite a bit about this table doesn't sit well with me, but right now is not the time to refactor it.
	At first glance it looks like this table is unique to transactionIDs,
	However, a transactionId is simply the DateTime that the system records when a user performs a search.
	
	There's also userId (the specific user who performed the search?); however, the userId is sometimes an empty string.
	
	ISSSN is an inconsistent composit key (concatenation of the transactionId-DateTime and the userId-CharacterString.
		I say inconsistent because it's value is sometimes blank '', somtimes just the transactionId-DateTime without
		a userId, and sometimes the wrong second-values on the transactionId-DateTime with the correct userId.
	
	T_User_RFRNC (or the claimReferenceNumber) identifies all of the searches/trnsactionIds that occur for a given event by a single user.
		Since it is possible to see 5-25 sequential transactionId-DateTimes, differing by only a single pico-second (smaller than nano)
		
	I was also unable to acertain the purpose of every column; as such, it is highly likely that this table includes more data than it should.
*/
CREATE TABLE DecisionNet.[Transaction] /*ClaimReference ? _ ProductFact ?*/
(
	transactionId CHAR(26) NOT NULL, /*I_TRNS*/
	userId VARCHAR(5) NOT NULL, /*I_USR*/
	claimReferenceNumber VARCHAR(30) NOT NULL, /*T_USR_RFRNC*/
	/*Question:*/
	claimReferenceNubmerSanitized AS CAST([ClaimSearch_Dev].[dbo].fn_StripCharacters(claimReferenceNumber, '^a-z0-9') AS VARCHAR(30)),
		/*Q:if claimReferenceNumber needs to be sanitized why not just constrain it.
		and if it doesn't need to be sanitized (IE: non-alphanumeric characters are legit,
		then are we not potentially introducing duplicate values by sanitizing?
	*/
	transactionDate AS CAST(
		SUBSTRING(transactionId,1,10)
		+ ' '
		+ REPLACE((SUBSTRING(transactionId,12,8)),'.',':')
		+ (SUBSTRING(transactionId,20,8))
		AS DATETIME2(6)
	), /*D_TRNS*/
	/*Dev Note:*/
	/*needs NULLIF''*/iSSSNCode VARCHAR(31) NULL,
		/*While I_SSSN is (in MOST cases) a simple concatenation of (a non exact match of) the transactionId and the userId,
			several instances where userId is not populated for "batches" IE: non-human-users.
			IE: information is lost if we store this value as a computed column.
	*/
	/*Dev Note:*/
	usrRfrncVldCode CHAR(1) NULL, /*F_USR_RFRNC_VLD*/
		/*5000 odd instances where value = 'F', rest null/emptyString.
		not in dashboard-extract, and description from excel-doc: "Internal Use only (not populated)"
		possible to remove/deprecate
	*/
	/*needs NULLIF''*/userIPAddress VARCHAR(15) NULL, /*T_IP_ADR_CUST*/
	/*needs NULLIF''*/companySoldToCode CHAR(4) NULL, /*I_CUST*/
	/*needs NULLIF''*/officeSoldToCode CHAR(5) NULL, /*I_REGOFF*/
	/*needs NULLIF''*/companyShippedToCode CHAR(4) NULL, /*I_CUST_SHP_TO*/
	/*needs NULLIF''*/officeShippedToCode CHAR(5) NULL, /*I_REGOFF_SHP_TO*/
	/*Dev Note:*/
	vendorId SMALLINT NULL, /*CLT201.I_VEND*/
		/*This column should be foreignKey-constrained against dbo.DIM_DN_OrderStatus.C_ORDR_STUS,
		 but that table does not have a PK. Deferring to DIM_DN_OrderStatus for DataType.
	*/
	/*needs NULLIF''*/C_RSLT_VEND VARCHAR(10) NULL, /*internal code, unknown use or meaning*/
	/*Dev Note:*/
	/*needs NULLIF''*/orderStatus TINYINT NOT NULL, /*CLT220.C_ORDR_STUS*/
		/*This column should be foreignKey-constrained against dbo.DIM_DN_OrderStatus.C_ORDR_STUS,
		 but that table does not have a PK. Also, the data type on the OrderStatusDim is VARCHAR(1).
	*/
	orderStatusAmended TINYINT NULL, /*contrived value, related to orderStatus */
	/*needs NULLIF''*/reportType VARCHAR(4) NULL, /*C_RPT_TYP, seems to be alphanumeric single char, can't find related table. also appears to be non-nullable, however with a left-outer join must be nulllable*/
	/*needs NULLIF''*/productTransactionCode CHAR(4) NULL, /*C_ISO_TRNS, Appears to be non-nullable, however with a left-outer join must be nulllable*/
	/*needs NULLIF''*/VendorTransactionID VARCHAR(20) NULL, /*I_VEN_TRNS,	true variable-lengh alpha-numeric. several empty-string values.*/
	isMatched BIT NULL, /*CLT201.F_MTCH, field tracks whether or not this transaction was a match or not. does not look like it is nullable, however need NULL based on no match possiblities*/
	/*needs NULLIF''*/isBilled CHAR(1) NULL, /*CLT201.F_BILL, field tracks whether or not this transaction was billed or not. does look like it has empty strings as well as 'I' codes.*/

	/*needs NULLIF''*/mtroStatusCode CHAR(1) NULL, /*CLT220.C_MTRO_STUS, alpha-numeric single character code that is an empty string in just under half the rows.*/
	/*needs NULLIF''*/vendorTransactionCode VARCHAR(4) NULL, /*CLT201.C_VEND_TRNS, Looks like the value is either a duplicate of the productTransactionCode, an empty string value, some number of '?'{3,4}, or a singleAlphaChar / two-digit-numeric value. little corrolation to productTypeCode/productVendorCode*/
	/*Dev Note:*/
	/*needs NULLIF''*/isoPKGCode VARCHAR(4) NULL, /*CLT201.C_ISO_PKG*/
		/*100% empty string or NULL value 20180815.
		not in dashboard-extract, and description from excel-doc: "Internal Use only"
		possible to remove/deprecate
	*/
	/*needs NULLIF''*/nmTypeCode CHAR(1) NULL, /*CLT200.C_NM_TYP, Value is either 'I' or (slightly over 50%) empty string.*/
	
	/*needs NULLIF''*/rsltIsoCode VARCHAR(10) NULL, /*CLT200.C_RSLT_ISO	Value is either '000' '100' or empty string. about 10million null/emptystring.*/			
	/*needs NULLIF''*/busDbCode CHAR(1) NULL, /*CLT200.F_BUS_DB 100% empty string value, not in dash*/
	/*needs NULLIF''*/cityExclRsltCode CHAR(1) NULL, /*CLT200.F_CITY_EXCL_RSLT, 100% empty string value, not in dash*/
	/*needs NULLIF''*/otherPrdSSSNCode VARCHAR(40) NULL, /*CLT200.I_OTH_PRD_SSSN, empty string 139/140%; when populated, almost identical to iSSSNCode - only differs by minute-second datetime2.*/
	/*needs NULLIF''*/browserDetail VARCHAR(258) NULL, /*CLT200.T_CUST_BROWSER_DTL, !!!NOTE requires additional NULLIF wraper for SOH character ASCII(1) *could it be json to string conversion?, data for what browser used for transaction*/
	/*needs NULLIF''*/email VARCHAR(255) NULL, /*CLT200.I_EMAIL, no integrity (no surprise), almost 100% emptystring, almost want to scrub out ^[a-zA-Z]@[a-zA-Z].com*/
	/*needs NULLIF''*/additionalCharge DECIMAL(6,2) NULL, /*CLT220.A_ADDL_CHRG, Aparnetly "not used in dash" Looks like a Non Nullable field (needs to be NULL due to LEFT OUTER JOIN. Slightly less than 50% values = 0.*/
	/*needs NULLIF''*/otherProductCode VARCHAR(10) NULL, /*CLT200.I_OTH_PRD, "IS in extract". note from excel: "This tell us if other products underlie the transaction in the 200 table". Seems to corelate to some type-string. almost 98% emptyString*/
	/*needs NULLIF''*/iSSSNPrntCode VARCHAR(40) NULL, /*CLT200.I_SSSN_PRNT, empty string 139/140%; when populated, almost identical to iSSSNCode - only differs by minute-second datetime2.*/
	/*
		D_BILL_TS		Date Billed
		D_BILL_RUN_TS		Date Billed was Run
		D_DELETE_TS		Date Deleted
		D_SRCH_TS		Date Searched
		D_FILL_TS		Date Fillied
		D_BILL		Date Billed
		D_BILL_RUN		Date Billed was Run
		D_DELETE		Date Deleted
		D_SRCH		Date Searched
		D_FILL		Date Fillied
		convert using:
		CAST(
			SUBSTRING(CLT00220.D_BILL,1,10)
			+ ' '
			+ REPLACE((SUBSTRING(CLT00220.D_BILL,12,8)),'.',':')
			+ (SUBSTRING(CLT00220.D_BILL,20,8))
			AS DATETIME2(6)
		)
	*/
	dateBilled DATETIME2(6) NULL, /*CLT00220.D_BILL_TS*/
	dateBilledRun DATETIME2(5) NULL, /*CLT00201.D_BILL_RUN_TS*/
	dateDeleted DATETIME2(6) NULL, /*CLT00220.D_DELETE_TS*/
	dateSearched DATETIME2(6) NULL, /*CLT00201.D_SRCH_TS*/
	dateFilled DATETIME2(6) NULL, /*CLT00220.D_FILL_TS*/
	
	/*needs NULLIF''*/nameSearched VARCHAR(70) NULL, /*CLT00200.M_FUL_NM_SRCH*/
	/*needs NULLIF''*/dateOfBirthSearched VARCHAR(8) NULL, /*CLT00200.D_BRTH_SRCH*/
	/*needs NULLIF''*/minAgeSearched VARCHAR(3) NULL, /*CLT00200.N_AGE_LOW_SRCH*/
	/*needs NULLIF''*/maxAgeSearched VARCHAR(3) NULL, /*CLT00200.N_AGE_HI_SRCH*/
	/*needs NULLIF''*/addressLine1Searched VARCHAR(50) NULL, /*CLT00200.T_ADR_LN1_SRCH*/
	/*needs NULLIF''*/licencePlateStateSearched VARCHAR(2) NULL, /*CLT00200.C_LIC_PLT_ST_SRCH*/
	/*needs NULLIF''*/citySearched VARCHAR(25) NULL, /*CLT00200.M_CITY_SRCH*/
	/*needs NULLIF''*/zipCodeSearched VARCHAR(5) NULL, /*CLT00200.C_ZIP_SRCH*/
	/*needs NULLIF''*/licencePlateSearched VARCHAR(20) NULL, /*CLT00200.N_LIC_PLT_SRCH*/
	/*needs NULLIF''*/driversLicenseSearched VARCHAR(52) NULL, /*CLT00200.N_DRV_LIC_SRCH*/
	/*needs NULLIF''*/countyCodeSearched VARCHAR(30) NULL, /*CLT00200.M_CNTY_SRCH*/
	/*needs NULLIF''*/phoneNumber1Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH*/
	/*needs NULLIF''*/phoneNumber2Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_2*/
	/*needs NULLIF''*/phoneNumber3Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_3*/
	/*needs NULLIF''*/phoneNumber4Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_4*/
	/*needs NULLIF''*/phoneNumber5Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_5*/
	/*needs NULLIF''*/radSearched VARCHAR(3) NULL, /*CLT00200.N_RAD_SRCH*/
	/*needs NULLIF''*/phtcSearched VARCHAR(1) NULL, /*CLT00200.F_PHTC_SRCH*/
	/*needs NULLIF''*/vinSearched VARCHAR(20) NULL, /*CLT00200.N_VIN_SRCH*/
	/*needs NULLIF''*/driversLicenseStateSearched VARCHAR(2) NULL, /*CLT00200.C_DRV_LIC_ST_SRCH*/
	/*needs NULLIF''*/stateSearched VARCHAR(2) NULL, /*CLT00200.C_ST_SRCH*/
	/*needs NULLIF''*/tokenizedSSNSearched VARCHAR(30) NULL, /*CLT00200.N_SSN_SRCH*/
	dateInserted DATE NOT NULL,
	CONSTRAINT PK_Transaction_transactionId_userId_claimReferenceNumber
		PRIMARY KEY CLUSTERED (transactionId, userId, claimReferenceNumber)
);

PRINT'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/