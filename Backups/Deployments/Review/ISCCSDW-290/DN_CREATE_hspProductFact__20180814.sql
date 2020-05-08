SET NOEXEC OFF;
/*
	This script will create the stored procedure for the FACT Table Data Refresh.
		
	Execution of the sproc this this script creates relies on data from tables in ClaimSearch_Prod:
		[ClaimSearch_Prod].dbo.CLT00200
		[ClaimSearch_Prod].dbo.CLT00201
		[ClaimSearch_Prod].dbo.CLT00220
*/

BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 278) for the production deploy.
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
/************************************************************************************************************************************************/	
/************************************************************************************************************************************************/	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCSDW-290
Date: 2018-07-23
Author: Robert David Warner
Description: Mechanism for data-refresh of the Fact Table.
			Originally, the table was being droppped,re-created, reinserted into.
			Current behavior now is Upsert, through Merge syntax.
			
			Performance: No notes for now
************************************************/
CREATE PROCEDURE DecisionNet.hsp_UpdateInsertTransaction
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE();
	CREATE TABLE #IndexedTempCLTData
	(
		transactionId CHAR(26) NOT NULL,
		userId VARCHAR(5) NOT NULL,
		claimReferenceNumber VARCHAR(30) NOT NULL,
		iSSSNCode VARCHAR(31) NULL,
		usrRfrncVldCode CHAR(1) NULL,
		userIPAddress VARCHAR(15) NULL,
		companySoldToCode CHAR(4) NULL,
		officeSoldToCode CHAR(5) NULL,
		companyShippedToCode CHAR(4) NULL,
		officeShippedToCode CHAR(5) NULL,
		vendorId SMALLINT NULL,
		C_RSLT_VEND VARCHAR(10) NULL,
		orderStatus TINYINT NOT NULL,
		orderStatusAmended TINYINT NULL,
		reportType VARCHAR(4) NULL,
		productTransactionCode CHAR(4) NULL,
		VendorTransactionID VARCHAR(20) NULL,
		isMatched BIT NULL,
		isBilled CHAR(1) NULL,
		mtroStatusCode CHAR(1) NULL,
		vendorTransactionCode VARCHAR(4) NULL,
		isoPKGCode VARCHAR(4) NULL,
		nmTypeCode CHAR(1) NULL,
		rsltIsoCode VARCHAR(10) NULL,
		busDbCode CHAR(1) NULL,
		cityExclRsltCode CHAR(1) NULL,
		otherPrdSSSNCode VARCHAR(40) NULL,
		browserDetail VARCHAR(258) NULL,
		email VARCHAR(255) NULL,
		additionalCharge DECIMAL(6,2) NULL,
		otherProductCode VARCHAR(10) NULL,
		iSSSNPrntCode VARCHAR(40) NULL,
		dateBilled DATETIME2(6) NULL,
		dateBilledRun DATETIME2(5) NULL,
		dateDeleted DATETIME2(6) NULL,
		dateSearched DATETIME2(6) NULL,
		dateFilled DATETIME2(6) NULL,
		nameSearched VARCHAR(70) NULL,
		dateOfBirthSearched VARCHAR(8) NULL,
		minAgeSearched VARCHAR(3) NULL,
		maxAgeSearched VARCHAR(3) NULL,
		addressLine1Searched VARCHAR(50) NULL,
		licencePlateStateSearched VARCHAR(2) NULL,
		citySearched VARCHAR(25) NULL,
		zipCodeSearched VARCHAR(5) NULL,
		licencePlateSearched VARCHAR(20) NULL,
		driversLicenseSearched VARCHAR(52) NULL,
		countyCodeSearched VARCHAR(30) NULL,
		phoneNumber1Searched VARCHAR(10) NULL,
		phoneNumber2Searched VARCHAR(10) NULL,
		phoneNumber3Searched VARCHAR(10) NULL,
		phoneNumber4Searched VARCHAR(10) NULL,
		phoneNumber5Searched VARCHAR(10) NULL,
		radSearched VARCHAR(3) NULL,
		phtcSearched VARCHAR(1) NULL,
		vinSearched VARCHAR(20) NULL,
		driversLicenseStateSearched VARCHAR(2) NULL,
		stateSearched VARCHAR(2) NULL,
		tokenizedSSNSearched VARCHAR(30) NULL,
		dateInserted DATE NOT NULL,
		CONSTRAINT PK_Transaction_transactionId_userId_claimReferenceNumber
			PRIMARY KEY CLUSTERED (transactionId, userId, claimReferenceNumber)
	);
	
	INSERT INTO #IndexedTempCLTData
	(
		transactionId, userId, claimReferenceNumber, iSSSNCode, usrRfrncVldCode, userIPAddress, companySoldToCode, officeSoldToCode, companyShippedToCode, officeShippedToCode, vendorId,
		C_RSLT_VEND, orderStatus, orderStatusAmended, reportType, productTransactionCode, VendorTransactionID, isMatched, isBilled, mtroStatusCode, vendorTransactionCode,
		isoPKGCode, nmTypeCode, rsltIsoCode, busDbCode, cityExclRsltCode, otherPrdSSSNCode, browserDetail, email, additionalCharge, otherProductCode,
		iSSSNPrntCode, dateBilled, dateBilledRun, dateDeleted, dateSearched, dateFilled, nameSearched, dateOfBirthSearched, minAgeSearched, maxAgeSearched,
		addressLine1Searched, licencePlateStateSearched, citySearched, zipCodeSearched, licencePlateSearched, driversLicenseSearched, countyCodeSearched, phoneNumber1Searched, phoneNumber2Searched, phoneNumber3Searched,
		phoneNumber4Searched, phoneNumber5Searched, radSearched, phtcSearched, vinSearched, driversLicenseStateSearched, stateSearched, tokenizedSSNSearched, dateInserted
	)
	
		
		
	MERGE INTO DecisionNet.ProductFact AS TARGET
	USING
	(
		SELECT
			*
		FROM
			#IndexedTempCLTData
END AS C_ORDR_STUS_AMENDED  
		FROM
			#IndexedTempCLTData
	) AS SOURCE
		ON 
	WHEN MATCHED
		AND
		(
			TARGET.invoiceDate <> SOURCE.invoiceDate
			OR TARGET.companySoldToCode <> SOURCE.companySoldToCode
			OR TARGET.officeSoldToCode <> SOURCE.officeSoldToCode
			OR TARGET.companyShippedToCode <> SOURCE.companyShippedToCode
			OR TARGET.officeShippedToCode <> SOURCE.officeShippedToCode
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
/*
	How are permissions for the Execution of this sproc being controlled?
*/
--EXEC DecisionNet.hsp_UpdateInsertExpenditure;
--SELECT * FROM DecisionNet.Expenditure;

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;


/*

*/