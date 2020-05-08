SET NOEXEC OFF;

BEGIN TRANSACTION
--DROP VIEW DecisionNet.V_ClaimReferenceExtract;
--DROP TABLE DecisionNet.ClaimReferenceExtract;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/************************************************************************************************************************************************/	
/*
	DEV NOTES:
	This would eventually hopefully be a VIEW so that underlying data was always up to date with dimention tables and storage-space was not wasted.
	Additionally, this would (through the object_definition) show comments etc -- although at that point the code would need to be cleaned up / made more
	  consisetent with our teamconvention coding standards.
	
*/
CREATE TABLE DecisionNet.ClaimReferenceExtract
(
	transactionId CHAR(26) NOT NULL, /*I_TRNS direct pull transactionId from fact table*/
	userId VARCHAR(5) NOT NULL, /*I_USR direct pull userId from fact table*/
	claimReferenceNumber VARCHAR(30) NOT NULL, /*T_USR_RFRNC direct pull claimReferenceNumber from fact table*/
	uniqueInstanceValue SMALLINT NOT NULL,
	transactionDate DATE NOT NULL, /*D_TRNS CAST transactionDate from fact table*/
	companySoldToCode CHAR(4) NULL, /*I_CUST direct pull companySoldToCode from Expenditure dim*/
	officeSoldToCode CHAR(5) NULL, /*I_REGOFF direct pull officeSoldToCode from Expenditure dim*/
	companyShippedToCode CHAR(4) NULL, /*I_CUST_SHP_TO direct pull companyShippedToCode from Expenditure dim*/
	officeShippedToCode CHAR(5) NULL, /*I_REGOFF_SHP_TO direct pull officeShippedToCode from Expenditure dim*/
	/*DevNote:*/
	/*deprecating*//*OrderStatus_Desc VARCHAR(50) NULL, *//*orderStatus / [ClaimSearch_Dev_Cubes].[dbo].[DN_Lookup_OrderStatus] using C_ORDR_STUS_AMENDED:*/
		/* could also be using: T_ORDR_STUS from dbo.DIM_DN_OrderStatus, joined against C_ORDR_STUS using orderStatus
		Romana states this is NOT currently being used in the dash; could deprecate. asking domain expert (dan).
	*/
	productTransactionCode CHAR(4) NULL, /*C_ISO_TRNS direct pull productTransactionCode from product dim*/
	dateBilledRun DATETIME2(5) NULL, /*D_BILL_RUN dateBilledRun fact table*/
	dateFilled DATETIME2(6) NULL, /*D_FILL dateFilled from fact table*/
	dateSearched DATETIME2(6) NULL, /*D_SRCH_TS dateSearched from fact table*/
	/*DevNote:*/
	originalBillFlag CHAR(1) NOT NULL, /*F_BILLABLE contrived*/
		/*CASE 
			WHEN revenue.[F_BILL_MTCH] IN ('I','Y') and [DN_FactTransactionMain].[F_BILL] = 'Y' 
			THEN 'Y' ELSE 'N' 
		END AS F_BILLABLE
	*/
	alternateBillFlag CHAR(1) NOT NULL, /*newcolumn added F_BILLMATCH::contrived off of match behavior and binary matchstatus*/
	isBilled CHAR(1) NULL, /*newcolumn; including a subcomponent of a critical derived column for performance of verification purposes*/
	/*deprecating*//*Match_Reason_Combined NVARCHAR(MAX) NULL, *//*contrived pivottable value*/
	/*deprecating*//*TOTAL_SEARCHES INT NULL, *//*contrived pivot table value*/
	/************************//*USER FIELDS/************************/
		  LEFT JOIN [ClaimSearch_Prod].[dbo].[PYRTAB]User_DN_Profile  
		  ON User_DN_Profile.I_USR = DN_FactTransactionMain.I_USR
			userName AS NAME,
			userCustomerCode AS I_CUST_USR,
			userOfficeCode AS I_REGOFF_USR,
			userJobClassDescription AS Jobclass_D
			userAddressLine1 AS ADDR1,
			userCity AS CITY,
			userState AS STATE,
			userZipCode AS ZIPCODE,
	****************************************************************/
	userName VARCHAR(30) NULL,
	userCustomerCode VARCHAR(4) NULL,
	userOfficeCode VARCHAR(5) NULL,
	userJobClassDescription VARCHAR(50) NULL,
	userAddressLine1 VARCHAR(25) NULL,
	userCity VARCHAR(25) NULL,
	userState VARCHAR(2) NULL,
	userZipCode VARCHAR(9) NULL,
	/***************************************************************/
	/*deprecating*//*stateRegion VARCHAR(15) NULL, /* STATE_REGION[ClaimSearch_Prod].[dbo].[Lookup_States].Region_Name, left join ON Lookup_States.State_Abb = [User_DN_Profile].[STATE]*/*/
	concatendatedProductHierarchyLvl VARCHAR(227) NULL, /*Product_Location contrived value: '/' delineated list of prodHeirarchyLvl1-3*/
	productTransactionDescription VARCHAR(75) NULL, /*T_ISO_TRNS productDescription from product dim*/
	productGroupId TINYINT NULL,
	/*case statements off of the productGroupId
		--F_People BIT NULL, 
		--F_Vehicle_Location_Products BIT NULL, 
		--F_Assets BIT NULL, 
		--F_Business BIT NULL, 
		--F_Healthcare BIT NULL, 
		--F_Directory_Assistance BIT NULL, 
		--F_Weather_Reports BIT NULL, 
		--F_Criminal_Records BIT NULL, 
		--F_Police_Reports BIT NULL, 
		--F_Driver_History BIT NULL, 
		--F_Medical_Records BIT NULL, 
		--F_ClaimDirector BIT NULL, 
		--F_OTHER BIT NULL,
	*/
	/**********************************************/
	productGroupName VARCHAR(100) NULL, /*newcolumn added productGroupName by bi team request, reduces work in spotfire*/
	/*deprecating*//*transactionTypeDescription VARCHAR(30) NULL, *//*T_TRAN_TYP was deprecated on the ProductDim; not used.*/
	lineItemQuantity INT NULL, /*newcolumn added quantity to reduce workload in spotfire*/
	lineItemCost DECIMAL(17,2) NULL, /*A_LN_ITM_EXTN_TR WAS* contrived; grabs "total" from mpv202 and divides by "quantity", then casts and rounds.
		ROUND(
			cast(
				main.[A_LN_ITM_EXTN] / [A_LN_ITM_QTY] as numeric(9,3)
			)
			, 2
		)
		* NOTE: but my version is just a direct pull of lineItemUnitCost from Exp. Dim.
	 */
	unitTax DECIMAL(17,2) NULL, /*A_LN_ITM_TAX_TR contrived; grabs "tax" from mpv202 and divides by "quantity", then casts and rounds.
		ROUND(
			cast(
				main.[A_LN_ITM_TAX]/[A_LN_ITM_QTY]  as numeric(9,3)
			)
			, 2
		)
	 */
	invoiceDate DATE NULL, /*D_INV_DT_Revenue : pull directly from Expenditure dim*/
	invoiceNumber VARCHAR(22) NULL, /*N_INV_NO pull directly from Expenditure dim*/
	/*deprecating*//*F_INVOICE_EXIST VARCHAR(1) NULL, *//*completely derived column*/
	productCode CHAR(9) NULL, /*I_PRD direct pull from Expenditure dim*/
	/*Vend info deprecated and replaced with just vendId*/
		/*deprecating*//*vendorTransactionDescription VARCHAR(50) NULL, /*[T_VEND_TRNS] cast from [ClaimSearch_Prod].[dbo].[Dim_DN_Vendor] join on Dim_DN_Vendor.I_VEND = DN_FactTransactionMain.I_VEND */*/
		/*deprecating*//*vendorAccountType VARCHAR(15) NULL, /*[ACT_TYP] cast from [ClaimSearch_Prod].[dbo].[Dim_DN_Vendor] join on Dim_DN_Vendor.I_VEND = DN_FactTransactionMain.I_VEND */*/
	vendorId SMALLINT NULL, /*I_VEND directpull from productFact*/
	/*deprecating*//*DN_Lookup_OrderStatus.OrderDescription *//*Not used and from Dev artifact.*/
	/*Deprecating 20181105*/--isLocationSearchUsed BIT NOT NULL, /*[LOCATION SEARCH ENTRY] contrived/derived from fact*/
	/*Deprecating 20181105*/--isPersonalSearchUsed BIT NOT NULL, /*[PERSONAL SEARCH ENTRY]	contrived/derived from fact*/
	/*Deprecating 20181105*/--isVehicleSearchUsed BIT NOT NULL, /*[VEHICLE SEARCH ENTRY] contrived/derived from fact*/
	dateInserted DATE NOT NULL /*added by JDE team. probably should have been included from the begining.*/
	--CONSTRAINT PK_ClaimReferenceExtract_claimReferenceId
	--	PRIMARY KEY CLUSTERED (claimReferenceId)
);
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
CREATE VIEW DecisionNet.V_ClaimReferenceExtract AS
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
		ClaimReferenceExtract.lineItemCost AS A_LN_ITM_EXTN_TR,
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

--EXEC sp_help 'DecisionNet.ClaimReferenceExtract'
--EXEC sp_help 'DecisionNet.V_ClaimReferenceExtract'

--PRINT'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*
COMMIT TRANSACTION

*/