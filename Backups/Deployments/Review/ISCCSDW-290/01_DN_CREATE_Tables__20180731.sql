Use ClaimSearch_Dev;
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
/*Remeber to switch to explicit COMMIT TRANSACTION (line 194) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE SCHEMA DecisionNet AUTHORIZATION dbo;
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
	productGroupId TINYINT IDENTITY(1,1) NOT NULL,
	productGroupName VARCHAR(100) NOT NULL,
	productGroupDescription VARCHAR(250) NULL,
	CONSTRAINT PK_ProductType_productGroupId
		PRIMARY KEY CLUSTERED (productGroupId)
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
	transactionTypeCode CHAR(1) NOT NULL,
	transactionTypeDescription VARCHAR(75) NULL,
	dateInserted DATE NOT NULL,
	CONSTRAINT PK_TransactionType_transactionTypeCode
		PRIMARY KEY CLUSTERED (transactionTypeCode)
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
	productTransactionCode CHAR(4) NOT NULL,
	productTransactionDescription VARCHAR(75) NULL,
	productTransactionCodeRemap CHAR(4) NULL,
	productHierarchy_Lvl1 VARCHAR(75) NOT NULL,
	productHierarchy_Lvl2 VARCHAR(75) NOT NULL,
	productHierarchy_Lvl3 VARCHAR(75) NOT NULL,
	dateInserted DATE NOT NULL,
	CONSTRAINT PK_ProductHierarchy_productTransactionCode
		PRIMARY KEY CLUSTERED (productTransactionCode)
	
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX FNIX_ProductHierarchy_productTransactionCodeRemap
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
	productTransactionCode CHAR(4) NOT NULL, /*C_ISO_TRNS*/
	productGroupId TINYINT NOT NULL,
	productTransactionDescription VARCHAR(75) NULL, /*T_ISO_TRNS*/
	/*Explicitly choosing to include the HeirarchyLevel values
		for both performance reasons (controlled denormalization)
		as well as diferentiating whether or not the productGroupId is default \ derived.
	*/
	productHierarchy_Lvl1 VARCHAR(75) NULL,
	productHierarchy_Lvl2 VARCHAR(75) NULL,
	productHierarchy_Lvl3 VARCHAR(75) NULL,
	billableMatchCode CHAR(1) NULL, /*F_BILL_MTCH*/
	nonBillableMatchCode CHAR(1) NULL, /*F_BILL_NO_MTCH*/
	/*I really beleive that these values are NOT functionally dependent on the ProductTransactionCode;
		I believe that they were previously included as a method of performance increase (controlled denormalization),
		HOWEVER it is possible to map the product_code incorrectly (since the vendor/productType component of the productCode
		is really dependent on the invoiceNumber-invoiceLineItemNumber (IE: Expenditure table)
	*/
	recentlyObservedProductCode CHAR(9) NULL, /*C_PS_PRD*/
	recentlyObservedLineItemCost DECIMAL(17,2) NULL, /*consolidation of existing amount values*/
	/*oldProductCode VARCHAR(18) NULL, /*C_PS_PRD_OLD*/*/
	transactionTypeCode CHAR(1) NULL
		CONSTRAINT FK_Product_TransactionType_transactionTypeCode
			FOREIGN KEY REFERENCES DecisionNet.TransactionType (transactionTypeCode), /*C_TRAN_TYP*/
	/*transactionTypeDescription VARCHAR(75) NULL, /*T_TRAN_TYP*/*/
	dateInserted DATE NOT NULL,
	/*If the number of variations of the billableMatchCode increase then it is likely that a type-table would be prefered,
		rather than a CheckConstraint
	*/
	CONSTRAINT PK_Product_productTransactionCode
		PRIMARY KEY CLUSTERED (productTransactionCode),
	CONSTRAINT FK_Product_ProductGroup_productGroupId
		FOREIGN KEY (productGroupId) REFERENCES DecisionNet.ProductGroup (productGroupId),
	CONSTRAINT CK_Product_billableMatchCode_ASSERT_KnownValueORNULL
		CHECK (billableMatchCode IS NULL OR billableMatchCode IN ('Y','N','I')),
	CONSTRAINT CK_Product_nonBillableMatchCode_ASSERT_KnownValueORNULL
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
	invoiceNumber VARCHAR(22) NOT NULL, /*N_INV_NO*/
	lineItemNumber INT NOT NULL, /*N_INV_LN_ITM*/
	invoiceDate DATE NOT NULL, /*D_INV_DT*/
	companySoldToCode CHAR(4) NOT NULL, /*I_CUST_SOLD*/
	officeSoldToCode CHAR(5) NULL, /*I_REGOFF_SOLD*/
	companyShippedToCode CHAR(4) NOT NULL, /*I_CUST_SHIPPED*/
	officeShippedToCode CHAR(5) NULL, /*I_REGOFF_SHIPPED*/
	productCode CHAR(9) NOT NULL, /*C_PS_PRD*/
	productTransactionTypeCode AS LEFT(productCode,4), /*C_ISO_TRNS_TYP*/
	productTransactionCode AS RIGHT(productCode,4), /*C_ISO_TRNS*/
	lineItemQuantity INT NOT NULL, /*A_LN_ITM_QTY*/
	lineItemUnitCost DECIMAL(17,2) NOT NULL, /*A_LN_ITM_UNIT*/
	lineItemTax DECIMAL(17,2) NOT NULL, /*A_LN_ITM_TAX*/
	dateInserted DATE NOT NULL,
	CONSTRAINT PK_Expenditure_invoiceNumber_lineItemNumber
		PRIMARY KEY CLUSTERED (invoiceNumber, lineItemNumber)
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
/*
	How are permissions for accessing these tables controlled?
*/
PRINT'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/