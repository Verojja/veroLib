USE ClaimSearch_Prod
GO
DROP TABLE DecisionNet.ClaimReference;
DROP TABLE DecisionNet.ClaimReferenceExtract;
GO
CREATE TABLE DecisionNet.ClaimReference
(
	transactionId CHAR(26) NOT NULL, /*I_TRNS*/
	userId VARCHAR(5) NOT NULL, /*I_USR*/
	claimReferenceNumber VARCHAR(30) NOT NULL, /*T_USR_RFRNC*/
	uniqueInstanceValue SMALLINT NOT NULL,
	claimReferenceNubmerSanitized AS CAST(dbo.fn_StripCharacters(claimReferenceNumber, '^a-z0-9') AS VARCHAR(30)),
	transactionDate AS CAST(
		SUBSTRING(transactionId,1,10)
		+ ' '
		+ REPLACE((SUBSTRING(transactionId,12,8)),'.',':')
		+ (SUBSTRING(transactionId,20,8))
		AS DATETIME2(6)
	), /*D_TRNS*/
	/*Dev Note:*/
	/*needs NULLIF''*/iSSSNCode VARCHAR(40) NULL,
		/*While I_SSSN is (in MOST cases) a simple concatenation of (a non exact match of) the transactionId and the userId,
			several instances where userId is not populated for "batches" IE: non-human-users.
			IE: information is lost if we store this value as a computed column.
	*/
	/*Dev Note:*/
	/*Deprecating 20181105*/--usrRfrncVldCode CHAR(1) NULL, /*F_USR_RFRNC_VLD*/
		/*5000 odd instances where value = 'F', rest null/emptyString.
		not in dashboard-extract, and description from excel-doc: "Internal Use only (not populated)"
		possible to remove/deprecate
	*/
	/*Deprecating 20181105*/--userIPAddress VARCHAR(50) NULL, /*T_IP_ADR_CUST, !!!NOTE Column requires VARCHAR(50) since it contains up to 3 concatenated ipAddresses*/
	companySoldToCode CHAR(4) NULL, /*I_CUST*/
	officeSoldToCode CHAR(5) NULL, /*I_REGOFF*/
	companyShippedToCode CHAR(4) NULL, /*I_CUST_SHP_TO*/
	officeShippedToCode CHAR(5) NULL, /*I_REGOFF_SHP_TO*/
	/*Dev Note:*/
	vendorId SMALLINT NULL, /*CLT201.I_VEND*/
		/*This column should be foreignKey-constrained against dbo.DIM_DN_OrderStatus.C_ORDR_STUS,
		 but that table does not have a PK. Deferring to DIM_DN_OrderStatus for DataType.
	*/
	rsltVendCode VARCHAR(10) NULL, /*CLT00200.C_RSLT_VEND, internal code, unknown use or meaning*/
	/*Dev Note:*/
	orderStatus TINYINT NULL, /*CLT220.C_ORDR_STUS*/
		/*This column should be foreignKey-constrained against dbo.DIM_DN_OrderStatus.C_ORDR_STUS,
		 but that table does not have a PK. Also, the data type on the OrderStatusDim is VARCHAR(1).
		 data doesn't look to be nullable, but needs null incase non-match to table
	*/
	/*Dev Note:*//*
		orderStatusAmended TINYINT NULL, /*contrived value, related to orderStatus */
		*//*Column deprecated post conversation with Dan - better to represent this mapping in a seperate table.
	*/
	reportType VARCHAR(4) NULL, /*C_RPT_TYP, seems to be alphanumeric single char, can't find related table. also appears to be non-nullable, however with a left-outer join must be nulllable*/
	productTransactionCode CHAR(4) NULL, /*C_ISO_TRNS, Appears to be non-nullable, however with a left-outer join must be nulllable*/
	vendorTransactionID VARCHAR(20) NULL, /*I_VEN_TRNS,	true variable-lengh alpha-numeric. several empty-string values.*/
	isMatched BIT NULL, /*CLT201.F_MTCH, field tracks whether or not this transaction was a match or not. does not look like it is nullable, however need NULL based on no match possiblities*/
	isBilled CHAR(1) NULL, /*CLT201.F_BILL, field tracks whether or not this transaction was billed or not. does look like it has empty strings as well as 'I' codes.*/

	mtroStatusCode CHAR(1) NULL, /*CLT220.C_MTRO_STUS, alpha-numeric single character code that is an empty string in just under half the rows.*/
	vendorTransactionCode VARCHAR(4) NULL, /*CLT201.C_VEND_TRNS, Looks like the value is either a duplicate of the productTransactionCode, an empty string value, some number of '?'{3,4}, or a singleAlphaChar / two-digit-numeric value. little corrolation to productTypeCode/productVendorCode*/
	/*Dev Note:*/
	/*Deprecating 20181105*/--isoPKGCode VARCHAR(4) NULL, /*CLT201.C_ISO_PKG*/
		/*100% empty string or NULL value 20180815.
		not in dashboard-extract, and description from excel-doc: "Internal Use only"
		possible to remove/deprecate
	*/
	/*Deprecating 20181105*/--nmTypeCode CHAR(1) NULL, /*CLT200.C_NM_TYP, Value is either 'I' or (slightly over 50%) empty string.*/
	
	/*Deprecating 20181105*/--rsltIsoCode VARCHAR(10) NULL, /*CLT200.C_RSLT_ISO	Value is either '000' '100' or empty string. about 10million null/emptystring.*/			
	/*Deprecating 20181105*/--busDbCode CHAR(1) NULL, /*CLT200.F_BUS_DB 100% empty string value, not in dash*/
	/*Deprecating 20181105*/--cityExclRsltCode CHAR(1) NULL, /*CLT200.F_CITY_EXCL_RSLT, 100% empty string value, not in dash*/
	/*Deprecating 20181105*/--otherPrdSSSNCode VARCHAR(40) NULL, /*CLT200.I_OTH_PRD_SSSN, empty string 139/140%; when populated, almost identical to iSSSNCode - only differs by minute-second datetime2.*/
	/*Deprecating 20181105*/--browserDetail VARCHAR(258) NULL, /*CLT200.T_CUST_BROWSER_DTL, !!!NOTE requires additional NULLIF wraper for SOH character ASCII(1) *could it be json to string conversion?, data for what browser used for transaction*/
	/*Deprecating 20181105*/--email VARCHAR(255) NULL, /*CLT200.I_EMAIL, no integrity (no surprise), almost 100% emptystring, almost want to scrub out ^[a-zA-Z]@[a-zA-Z].com*/
	additionalCharge DECIMAL(6,2) NULL, /*CLT220.A_ADDL_CHRG, Aparnetly "not used in dash" Looks like a Non Nullable field (needs to be NULL due to LEFT OUTER JOIN. Slightly less than 50% values = 0.*/
	otherProductCode VARCHAR(10) NULL, /*CLT200.I_OTH_PRD, "IS in extract". note from excel: "This tell us if other products underlie the transaction in the 200 table". Seems to corelate to some type-string. almost 98% emptyString*/
	/*Deprecating 20181105*/--iSSSNPrntCode VARCHAR(40) NULL, /*CLT200.I_SSSN_PRNT, empty string 139/140%; when populated, almost identical to iSSSNCode - only differs by minute-second datetime2.*/
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
	
	/*Deprecating 20181105*/--nameSearched VARCHAR(70) NULL, /*CLT00200.M_FUL_NM_SRCH*/
	/*Deprecating 20181105*/--dateOfBirthSearched VARCHAR(8) NULL, /*CLT00200.D_BRTH_SRCH*/
	/*Deprecating 20181105*/--minAgeSearched VARCHAR(3) NULL, /*CLT00200.N_AGE_LOW_SRCH*/
	/*Deprecating 20181105*/--maxAgeSearched VARCHAR(3) NULL, /*CLT00200.N_AGE_HI_SRCH*/
	/*Deprecating 20181105*/--addressLine1Searched VARCHAR(50) NULL, /*CLT00200.T_ADR_LN1_SRCH*/
	/*Deprecating 20181105*/--licencePlateStateSearched VARCHAR(2) NULL, /*CLT00200.C_LIC_PLT_ST_SRCH*/
	/*Deprecating 20181105*/--citySearched VARCHAR(25) NULL, /*CLT00200.M_CITY_SRCH*/
	/*Deprecating 20181105*/--zipCodeSearched VARCHAR(5) NULL, /*CLT00200.C_ZIP_SRCH*/
	/*Deprecating 20181105*/--licencePlateSearched VARCHAR(20) NULL, /*CLT00200.N_LIC_PLT_SRCH*/
	/*Deprecating 20181105*/--driversLicenseSearched VARCHAR(52) NULL, /*CLT00200.N_DRV_LIC_SRCH*/
	/*Deprecating 20181105*/--countyCodeSearched VARCHAR(30) NULL, /*CLT00200.M_CNTY_SRCH*/
	/*Deprecating 20181105*/--phoneNumber1Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH*/
	/*Deprecating 20181105*/--phoneNumber2Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_2*/
	/*Deprecating 20181105*/--phoneNumber3Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_3*/
	/*Deprecating 20181105*/--phoneNumber4Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_4*/
	/*Deprecating 20181105*/--phoneNumber5Searched VARCHAR(10) NULL, /*CLT00200.N_TEL_SRCH_5*/
	/*Deprecating 20181105*/--radSearched VARCHAR(3) NULL, /*CLT00200.N_RAD_SRCH*/
	/*Deprecating 20181105*/--phtcSearched VARCHAR(1) NULL, /*CLT00200.F_PHTC_SRCH*/
	/*Deprecating 20181105*/--vinSearched VARCHAR(20) NULL, /*CLT00200.N_VIN_SRCH*/
	/*Deprecating 20181105*/--driversLicenseStateSearched VARCHAR(2) NULL, /*CLT00200.C_DRV_LIC_ST_SRCH*/
	/*Deprecating 20181105*/--stateSearched VARCHAR(2) NULL, /*CLT00200.C_ST_SRCH*/
	/*Deprecating 20181105*/--tokenizedSSNSearched VARCHAR(30) NULL, /*CLT00200.N_SSN_SRCH*/
	dateInserted DATE NOT NULL
);
GO
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