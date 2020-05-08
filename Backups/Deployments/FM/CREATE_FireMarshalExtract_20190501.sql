SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	DROP TABLE dbo.FireMarshalExtract
	
--*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-24
Author: Robert David Warner
Description: Current Snapshot of Fire Claim(s) to be sent to FireMarshal(s).
				Updated daily /*20190425*/,
				Records in this object should be mutually exclusive with
				the FireMarshalClaimSendHistory

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalExtract
(   
	elementalClaimId BIGINT NOT NULL,
	claimId BIGINT NOT NULL,
	/*uniqueInstanceValue SMALLINT NOT NULL,*/
	
	isoFileNumber VARCHAR(11) NULL, /*isoClaimId*/
	
	reportingStatus VARCHAR(25) NULL, 
	fMstatus VARCHAR(255) NULL,
	fMDate DATE NULL,
	claimIsOpen BIT NOT NULL,
	dateSubmittedToIso DATE NULL,
	
	originalClaimNumber VARCHAR(30) NULL,
	originalPolicyNumber VARCHAR(30) NOT NULL,
	
	insuranceProviderOfficeCode CHAR(5) NOT NULL,
	insuranceProviderCompanyCode CHAR(4) NOT NULL,
	adjusterCompanyCode CHAR(4) NULL,
	adjusterCompanyName VARCHAR(75) NULL,
	
	companyName VARCHAR(75) NOT NULL,
	affiliate1Code CHAR(4) NOT NULL,
	affiliate1Name VARCHAR(75) NOT NULL,
	affiliate2Code CHAR(4) NOT NULL,
	affiliate2Name VARCHAR(75) NOT NULL,
	groupCode CHAR(4) NOT NULL,
	groupName VARCHAR(75) NOT NULL,
	
	lossAddressLine1 VARCHAR(50) NULL,
	lossAddressLine2 VARCHAR(50) NULL,
	lossCityName VARCHAR(25) NULL,
	
	lossStateCode CHAR(2) NULL,
	lossStateName VARCHAR(50) NULL,
	lossGeoCounty VARCHAR(25) NULL,
	lossZipCode	VARCHAR(9) NULL,
	lossLatitude VARCHAR(15) NULL,
	lossLongitude VARCHAR(15) NULL,
	
	lossDescription VARCHAR(50) NULL,
	lossDescriptionExtended VARCHAR(200) NULL,
	
	dateOfLoss DATE NULL,
	lossTypeCode CHAR(4) NULL,
	lossTypeDescription VARCHAR(42) NULL,
	policyTypeCode CHAR(4) NULL,
	policyTypeDescription VARCHAR(100) NULL,
	coverageTypeCode CHAR(4) NULL,
	coverageTypeDescription VARCHAR(42) NULL,
	
	settlementAmount MONEY NULL,
	estimatedLossAmount MONEY NULL,
	policyAmount MONEY NULL,
	buildingPaidAmount MONEY NULL,
	contentReserveAmount MONEY NULL,
	contentPaidAmount MONEY NULL,
	isIncendiaryFire BIT NOT NULL,
	
	isClaimUnderSIUInvestigation BIT NULL,
	siuCompanyName VARCHAR(70) NULL,
	siuRepresentativeFullName VARCHAR(250) NULL,
	siuWorkPhoneNumber CHAR(10) NULL,
	siuCellPhoneNumber CHAR(10) NULL,
	
	isActive BIT NOT NULL,
	isCurrent BIT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	involvedPartyFullName VARCHAR(250),
	adjusterId BIGINT NULL,
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	dateInserted DATETIME2(0) NOT NULL
	CONSTRAINT PK_FireMarshalExtract_elementalClaimId
		PRIMARY KEY CLUSTERED (elementalClaimId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--CREATE NONCLUSTERED INDEX NIX_ t_ c
--	ON dbo.V_ActiveElementalClaim ()
--	INCLUDE ();
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--/***********************************************
--WorkItem: ISCCINTEL-2316
--Date: 2019-01-24
--Author: Robert David Warner
--Description: Logging table for InvolvedPartyAddressMap
				
--			Performance: No current notes.
--************************************************/
--CREATE TABLE dbo.FireMarshalExtractActivityLog
--(
--	fireMarshalExtractActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
--	productCode VARCHAR(50) NULL,
--	sourceDateTime DATETIME2(0) NOT NULL,
--	executionDateTime DATETIME2(0) NOT NULL,
--	stepId TINYINT NOT NULL,
--	stepDescription VARCHAR(1000) NULL,
--	stepStartDateTime DATETIME2(0) NULL, 
--	stepEndDateTime DATETIME2(0) NULL,
--	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
--	recordsAffected BIGINT NULL,
--	isSuccessful BIT NOT NULL,
--	stepExecutionNotes VARCHAR(1000) NULL,
--	CONSTRAINT PK_FireMarshalExtract_fireMarshalExtractActivityLogId
--		PRIMARY KEY CLUSTERED (fireMarshalExtractActivityLogId)
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_FireMarshalExtractActivityLog_isSuccessful_stepId_executionDateTime
--	ON dbo.FireMarshalExtractActivityLog (isSuccessful, stepId, executionDateTime);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--EXEC sp_help 'dbo.FireMarshalExtract'
--EXEC sp_help 'dbo.FireMarshalExtractActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/