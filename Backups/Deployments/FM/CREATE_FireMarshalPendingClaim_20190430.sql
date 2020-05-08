SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*

DROP VIEW dbo.V_ActiveCurrentPendingFMClaim;
DROP TABLE dbo.FireMarshalPendingClaim;

--/*

--DROP TABLE dbo.FMPendingClaimActivityLog;

--*/
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
Date: 2019-04-30
Author: Robert David Warner
Description: System of captureing both current Snapshot of Fire Claim(s)
			 to be sent to FireMarshal(s), as well as their changes over time.

				Updated daily /*20190425*/,
				Records in this object should be mutually exclusive with
				the FireMarshalClaimSendHistory

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalPendingClaim
(   
	elementalClaimId BIGINT NOT NULL,
	uniqueInstanceValue TINYINT NOT NULL,
	
	claimId BIGINT NOT NULL,
	isoFileNumber VARCHAR(11) NULL, /*isoClaimId*/
	
	reportingStatus VARCHAR(25) NULL, 
	fireMarshallStatus VARCHAR(255) NULL,
	fireMarshallDate DATE NULL,
	claimIsOpen BIT NOT NULL,
	dateSubmittedToIso DATE NULL,
	
	originalClaimNumber VARCHAR(30) NULL,
	originalPolicyNumber VARCHAR(30) NOT NULL,
	
	insuranceProviderOfficeCode CHAR(5) NOT NULL,
	insuranceProviderCompanyCode CHAR(4) NOT NULL,
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
	
	lossZipCode	VARCHAR(9) NULL,
	lossGeoCounty VARCHAR(25) NULL,
	
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
	
	estimatedLossAmount MONEY NULL,
	settlementAmount MONEY NULL,
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
	
	involvedPartyId BIGINT NOT NULL,
	adjusterId BIGINT NULL,
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	
	/*dateClaimClosed DATE NULL,*/
	/*coverageStatus VARCHAR(3) NULL,*/
	
	reserveAmount MONEY NULL,
	totalInsuredAmount MONEY NULL,
	replacementAmount MONEY NULL,
	actualCashAmount MONEY NULL,
	buildingPolicyAmount MONEY NULL,
	buildingTotalInsuredAmount MONEY NULL,
	buildingReplacementAmount MONEY NULL,
	buildingActualCashAmount MONEY NULL,
	buildingEstimatedLossAmount MONEY NULL,
	contentPolicyAmount MONEY NULL,
	contentTotalInsuredAmount MONEY NULL,
	contentReplacementAmount MONEY NULL,
	contentActualCashAmount MONEY NULL,
	contentEstimatedLossAmount MONEY NULL,
	stockPolicyAmount MONEY NULL,
	stockTotalInsuredAmount MONEY NULL,
	stockReplacementAmount MONEY NULL,
	stockActualCashAmount MONEY NULL,
	stockEstimatedLossAmount MONEY NULL,
	lossOfUsePolicyAmount MONEY NULL,
	lossOfUseTotalInsuredAmount MONEY NULL,
	lossOfUseReplacementAmount MONEY NULL,
	lossOfUseActualCashAmount MONEY NULL,
	lossOfUseEstimatedLossAmount MONEY NULL,
	otherPolicyAmount MONEY NULL,
	otherTotalInsuredAmount MONEY NULL,
	otherReplacementAmount MONEY NULL,
	otherActualCashAmount MONEY NULL,
	otherEstimatedLossAmount MONEY NULL,
	buildingReserveAmount MONEY NULL,
	
	stockReserveAmount MONEY NULL,
	stockPaidAmount MONEY NULL,
	lossOfUseReserve MONEY NULL,
	lossOfUsePaid MONEY NULL,
	otherReserveAmount MONEY NULL,
	otherPaidAmount MONEY NULL,

	isActive BIT NOT NULL,
	isCurrent BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL
	CONSTRAINT PK_FireMarshalPendingClaim_elementalClaimId_uniqueInstanceValue
		PRIMARY KEY CLUSTERED (elementalClaimId, uniqueInstanceValue)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_FireMarshalPendingClaim_isCurrent_isActive
	ON dbo.FireMarshalPendingClaim (isCurrent, isActive)
	INCLUDE (claimId, isoFileNumber, reportingStatus, fireMarshallStatus, fireMarshallDate, claimIsOpen, dateSubmittedToIso, originalClaimNumber, originalPolicyNumber, insuranceProviderOfficeCode, insuranceProviderCompanyCode, companyName, affiliate1Code, affiliate1Name, affiliate2Code, affiliate2Name, groupCode, groupName, lossAddressLine1, lossAddressLine2, lossCityName, lossStateCode, lossStateName, lossZipCode, lossGeoCounty, lossLatitude, lossLongitude, lossDescription, lossDescriptionExtended, dateOfLoss,
			lossTypeCode, lossTypeDescription, policyTypeCode, policyTypeDescription, coverageTypeCode, coverageTypeDescription, settlementAmount, estimatedLossAmount, policyAmount, isIncendiaryFire, isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName, siuWorkPhoneNumber, siuCellPhoneNumber, involvedPartyId, adjusterId, involvedPartySequenceId, reserveAmount, totalInsuredAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount,
			contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount, otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount, otherPaidAmount, dateInserted);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-04-30
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
************************************************/
CREATE VIEW dbo.V_ActiveCurrentPendingFMClaim
WITH SCHEMABINDING
AS
(
	SELECT
		FireMarshalPendingClaim.elementalClaimId,
		FireMarshalPendingClaim.uniqueInstanceValue,
		FireMarshalPendingClaim.claimId,
		FireMarshalPendingClaim.isoFileNumber,
		FireMarshalPendingClaim.reportingStatus,
		FireMarshalPendingClaim.fireMarshallStatus,
		FireMarshalPendingClaim.fireMarshallDate,
		FireMarshalPendingClaim.claimIsOpen,
		FireMarshalPendingClaim.dateSubmittedToIso,
		FireMarshalPendingClaim.originalClaimNumber,
		FireMarshalPendingClaim.originalPolicyNumber,
		FireMarshalPendingClaim.insuranceProviderOfficeCode,
		FireMarshalPendingClaim.insuranceProviderCompanyCode,
		FireMarshalPendingClaim.companyName,
		FireMarshalPendingClaim.affiliate1Code,
		FireMarshalPendingClaim.affiliate1Name,
		FireMarshalPendingClaim.affiliate2Code,
		FireMarshalPendingClaim.affiliate2Name,
		FireMarshalPendingClaim.groupCode,
		FireMarshalPendingClaim.groupName,
		FireMarshalPendingClaim.lossAddressLine1,
		FireMarshalPendingClaim.lossAddressLine2,
		FireMarshalPendingClaim.lossCityName,
		FireMarshalPendingClaim.lossStateCode,
		FireMarshalPendingClaim.lossStateName,
		FireMarshalPendingClaim.lossZipCode,
		FireMarshalPendingClaim.lossGeoCounty,
		FireMarshalPendingClaim.lossLatitude,
		FireMarshalPendingClaim.lossLongitude,
		FireMarshalPendingClaim.lossDescription,
		FireMarshalPendingClaim.lossDescriptionExtended,
		FireMarshalPendingClaim.dateOfLoss,
		FireMarshalPendingClaim.lossTypeCode,
		FireMarshalPendingClaim.lossTypeDescription,
		FireMarshalPendingClaim.policyTypeCode,
		FireMarshalPendingClaim.policyTypeDescription,
		FireMarshalPendingClaim.coverageTypeCode,
		FireMarshalPendingClaim.coverageTypeDescription,
		FireMarshalPendingClaim.estimatedLossAmount,
		FireMarshalPendingClaim.settlementAmount,
		FireMarshalPendingClaim.policyAmount,
		FireMarshalPendingClaim.buildingPaidAmount,
		FireMarshalPendingClaim.contentReserveAmount,
		FireMarshalPendingClaim.contentPaidAmount,
		FireMarshalPendingClaim.isIncendiaryFire,
		FireMarshalPendingClaim.isClaimUnderSIUInvestigation,
		FireMarshalPendingClaim.siuCompanyName,
		FireMarshalPendingClaim.siuRepresentativeFullName,
		FireMarshalPendingClaim.siuWorkPhoneNumber,
		FireMarshalPendingClaim.siuCellPhoneNumber,
		FireMarshalPendingClaim.involvedPartyId,
		FireMarshalPendingClaim.adjusterId,
		FireMarshalPendingClaim.involvedPartySequenceId,
		FireMarshalPendingClaim.reserveAmount,
		FireMarshalPendingClaim.totalInsuredAmount,
		FireMarshalPendingClaim.replacementAmount,
		FireMarshalPendingClaim.actualCashAmount,
		FireMarshalPendingClaim.buildingPolicyAmount,
		FireMarshalPendingClaim.buildingTotalInsuredAmount,
		FireMarshalPendingClaim.buildingReplacementAmount,
		FireMarshalPendingClaim.buildingActualCashAmount,
		FireMarshalPendingClaim.buildingEstimatedLossAmount,
		FireMarshalPendingClaim.contentPolicyAmount,
		FireMarshalPendingClaim.contentTotalInsuredAmount,
		FireMarshalPendingClaim.contentReplacementAmount,
		FireMarshalPendingClaim.contentActualCashAmount,
		FireMarshalPendingClaim.contentEstimatedLossAmount,
		FireMarshalPendingClaim.stockPolicyAmount,
		FireMarshalPendingClaim.stockTotalInsuredAmount,
		FireMarshalPendingClaim.stockReplacementAmount,
		FireMarshalPendingClaim.stockActualCashAmount,
		FireMarshalPendingClaim.stockEstimatedLossAmount,
		FireMarshalPendingClaim.lossOfUsePolicyAmount,
		FireMarshalPendingClaim.lossOfUseTotalInsuredAmount,
		FireMarshalPendingClaim.lossOfUseReplacementAmount,
		FireMarshalPendingClaim.lossOfUseActualCashAmount,
		FireMarshalPendingClaim.lossOfUseEstimatedLossAmount,
		FireMarshalPendingClaim.otherPolicyAmount,
		FireMarshalPendingClaim.otherTotalInsuredAmount,
		FireMarshalPendingClaim.otherReplacementAmount,
		FireMarshalPendingClaim.otherActualCashAmount,
		FireMarshalPendingClaim.otherEstimatedLossAmount,
		FireMarshalPendingClaim.buildingReserveAmount,
		FireMarshalPendingClaim.stockReserveAmount,
		FireMarshalPendingClaim.stockPaidAmount,
		FireMarshalPendingClaim.lossOfUseReserve,
		FireMarshalPendingClaim.lossOfUsePaid,
		FireMarshalPendingClaim.otherReserveAmount,
		FireMarshalPendingClaim.otherPaidAmount,
		FireMarshalPendingClaim.dateInserted
	FROM
		dbo.FireMarshalPendingClaim
	WHERE
		FireMarshalPendingClaim.isActive = 1
		AND FireMarshalPendingClaim.isCurrent = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN	
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveCurrentPendingFMClaim_elementalClaimId_uniqueInstanceValue
	ON dbo.V_ActiveCurrentPendingFMClaim (elementalClaimId, uniqueInstanceValue)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--/***********************************************
--WorkItem: ISCCINTEL-2316
--Date: 2019-04-30
--Author: Robert David Warner
--Description: Logging table for InvolvedPartyAddressMap
				
--			Performance: No current notes.
--************************************************/
--CREATE TABLE dbo.FMPendingClaimActivityLog
--(
--	fMPendingClaimActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
--	productCode VARCHAR(50) NULL,
--	sourceDateTime DATETIME2(0) NOT NULL,
--	executionDateTime DATETIME2(0) NOT NULL,
--	stepId TINYINT NOT NULL,
--	stepDescription VARCHAR(1000) NULL,
--	stepStartDateTime DATETIME2 (0) NULL, 
--	stepEndDateTime DATETIME2(0) NULL,
--	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
--	recordsAffected BIGINT NULL,
--	isSuccessful BIT NOT NULL,
--	stepExecutionNotes VARCHAR(1000) NULL,
--	CONSTRAINT PK_FMPendingClaimActivityLog_fMPendingClaimActivityLogId
--		PRIMARY KEY CLUSTERED (fMPendingClaimActivityLogId)
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_FMPendingClaimActivityLog_isSuccessful_stepId_executionDateTime
--	ON dbo.FMPendingClaimActivityLog (isSuccessful, stepId, executionDateTime);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--EXEC sp_help 'dbo.FireMarshalPendingClaim'
--EXEC sp_help 'dbo.V_ActiveCurrentPendingFMClaim'
--EXEC sp_help 'dbo.FMPendingClaimActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;
		
/*

*/
