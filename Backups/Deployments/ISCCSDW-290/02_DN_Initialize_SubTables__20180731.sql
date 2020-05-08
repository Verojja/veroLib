SET NOEXEC OFF;
/*
	This script will initialize the support tables exactly once; it is NOT designed
		to be made into a job/sproc or automated in any way.
		It will only be executed a single time to initialize the support tables for
		some of the DecisionNet Dimensions.
	
	Execution of this script relies on data from tables in ClaimSearch_Dev ONLY.
		IE: there is NO required data refresh for existing production data,
		as is the case with the hps_ scripts.
*/

BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 187) for the production deploy.
Message log output should be similar to the following:

	(13 row(s) affected)

	(6 row(s) affected)

	(6241 row(s) affected)
	
	COMMIT TRANSACTION
*/
/************************************************************************************************************************************************/
/******************************************************Objects Required for indipendent testing**************************************************/
	/*
		Run script 01_ to create the sub tables
	*/
/************************************************************************************************************************************************/	
/************************************************************************************************************************************************/	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*Populate_ProductGroup*/
IF(EXISTS(SELECT NULL FROM sys.all_objects WHERE all_objects.object_id = OBJECT_ID('DecisionNet.ProductGroup')))
BEGIN
	IF(NOT EXISTS(SELECT NULL FROM DecisionNet.ProductGroup))
	BEGIN
		INSERT INTO DecisionNet.ProductGroup
			(productGroupId, productGroupName, productGroupDescription)
		VALUES
			(1, 'People', NULL),
			(2, 'Vehicle_Location_Products', NULL),
			(3, 'Assets', NULL),
			(4, 'Business', NULL),
			(5, 'Healthcare', NULL),
			(6, 'Directory_Assistance', NULL),
			(7, 'Weather_Reports', NULL),
			(8, 'Criminal_Records', NULL),
			(9, 'Police_Reports', NULL),
			(10, 'Driver_History', NULL),
			(11, 'Medical_Records', NULL),
			(12, 'ClaimDirector', NULL),
			(13, 'OTHER', NULL);
	END
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*Populate_TransactionType*/
IF(EXISTS(SELECT NULL FROM sys.all_objects WHERE all_objects.object_id = OBJECT_ID('DecisionNet.TransactionType')))
BEGIN
	IF(NOT EXISTS(SELECT NULL FROM DecisionNet.TransactionType))
	BEGIN
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
	END
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*Populate_ProductHierarchy*/
IF(EXISTS(SELECT NULL FROM sys.all_objects WHERE all_objects.object_id = OBJECT_ID('DecisionNet.ProductHierarchy')))
BEGIN
	IF(NOT EXISTS(SELECT NULL FROM DecisionNet.ProductHierarchy))
	BEGIN
		DECLARE @dateInserted DATE = GETDATE();
		INSERT INTO DecisionNet.ProductHierarchy
		(
			productTransactionCode,
			productTransactionCodeRemap,
			productTransactionDescription,
			productHierarchyLvl1,
			productHierarchyLvl2,
			productHierarchyLvl3,
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
	END
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/