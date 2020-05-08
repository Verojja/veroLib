SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	DROP VIEW dbo.V_ActiveFireMarshalClaimSendHistory
	--DROP TABLE dbo.FMClaimSendHistoryActivityLog
	DROP TABLE dbo.FireMarshalClaimSendHistory
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
Description: History of Fire Claim(s) that were "sent" to FireMarshal(s).
				HSP for rowGeneration executed daily, HOWEVER DML is restricted
					to schedule date on FireMarshal Controller.
					
				Records in this object should be mutually exclusive with
				the FireMarshalPendingClaim

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalClaimSendHistory
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
	dateInserted DATETIME2(0) NOT NULL
	CONSTRAINT PK_FireMarshalClaimSendHistory_elementalClaimId_uniqueInstanceValue
		PRIMARY KEY CLUSTERED (elementalClaimId, uniqueInstanceValue)	
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
************************************************/
CREATE VIEW dbo.V_ActiveFireMarshalClaimSendHistory
WITH SCHEMABINDING
AS
(
	SELECT
		FireMarshalClaimSendHistory.elementalClaimId,
		FireMarshalClaimSendHistory.uniqueInstanceValue,
		FireMarshalClaimSendHistory.claimId,
		FireMarshalClaimSendHistory.isoFileNumber,
		FireMarshalClaimSendHistory.reportingStatus,
		FireMarshalClaimSendHistory.fireMarshallStatus,
		FireMarshalClaimSendHistory.fireMarshallDate,
		FireMarshalClaimSendHistory.claimIsOpen,
		FireMarshalClaimSendHistory.dateSubmittedToIso,
		FireMarshalClaimSendHistory.originalClaimNumber,
		FireMarshalClaimSendHistory.originalPolicyNumber,
		FireMarshalClaimSendHistory.insuranceProviderOfficeCode,
		FireMarshalClaimSendHistory.insuranceProviderCompanyCode,
		FireMarshalClaimSendHistory.companyName,
		FireMarshalClaimSendHistory.affiliate1Code,
		FireMarshalClaimSendHistory.affiliate1Name,
		FireMarshalClaimSendHistory.affiliate2Code,
		FireMarshalClaimSendHistory.affiliate2Name,
		FireMarshalClaimSendHistory.groupCode,
		FireMarshalClaimSendHistory.groupName,
		FireMarshalClaimSendHistory.lossAddressLine1,
		FireMarshalClaimSendHistory.lossAddressLine2,
		FireMarshalClaimSendHistory.lossCityName,
		FireMarshalClaimSendHistory.lossStateCode,
		FireMarshalClaimSendHistory.lossStateName,
		FireMarshalClaimSendHistory.lossZipCode,
		FireMarshalClaimSendHistory.lossGeoCounty,
		FireMarshalClaimSendHistory.lossLatitude,
		FireMarshalClaimSendHistory.lossLongitude,
		FireMarshalClaimSendHistory.lossDescription,
		FireMarshalClaimSendHistory.lossDescriptionExtended,
		FireMarshalClaimSendHistory.dateOfLoss,
		FireMarshalClaimSendHistory.lossTypeCode,
		FireMarshalClaimSendHistory.lossTypeDescription,
		FireMarshalClaimSendHistory.policyTypeCode,
		FireMarshalClaimSendHistory.policyTypeDescription,
		FireMarshalClaimSendHistory.coverageTypeCode,
		FireMarshalClaimSendHistory.coverageTypeDescription,
		FireMarshalClaimSendHistory.estimatedLossAmount,
		FireMarshalClaimSendHistory.settlementAmount,
		FireMarshalClaimSendHistory.policyAmount,
		FireMarshalClaimSendHistory.buildingPaidAmount,
		FireMarshalClaimSendHistory.contentReserveAmount,
		FireMarshalClaimSendHistory.contentPaidAmount,
		FireMarshalClaimSendHistory.isIncendiaryFire,
		FireMarshalClaimSendHistory.isClaimUnderSIUInvestigation,
		FireMarshalClaimSendHistory.siuCompanyName,
		FireMarshalClaimSendHistory.siuRepresentativeFullName,
		FireMarshalClaimSendHistory.siuWorkPhoneNumber,
		FireMarshalClaimSendHistory.siuCellPhoneNumber,
		FireMarshalClaimSendHistory.involvedPartyId,
		FireMarshalClaimSendHistory.adjusterId,
		FireMarshalClaimSendHistory.involvedPartySequenceId,
		FireMarshalClaimSendHistory.reserveAmount,
		FireMarshalClaimSendHistory.totalInsuredAmount,
		FireMarshalClaimSendHistory.replacementAmount,
		FireMarshalClaimSendHistory.actualCashAmount,
		FireMarshalClaimSendHistory.buildingPolicyAmount,
		FireMarshalClaimSendHistory.buildingTotalInsuredAmount,
		FireMarshalClaimSendHistory.buildingReplacementAmount,
		FireMarshalClaimSendHistory.buildingActualCashAmount,
		FireMarshalClaimSendHistory.buildingEstimatedLossAmount,
		FireMarshalClaimSendHistory.contentPolicyAmount,
		FireMarshalClaimSendHistory.contentTotalInsuredAmount,
		FireMarshalClaimSendHistory.contentReplacementAmount,
		FireMarshalClaimSendHistory.contentActualCashAmount,
		FireMarshalClaimSendHistory.contentEstimatedLossAmount,
		FireMarshalClaimSendHistory.stockPolicyAmount,
		FireMarshalClaimSendHistory.stockTotalInsuredAmount,
		FireMarshalClaimSendHistory.stockReplacementAmount,
		FireMarshalClaimSendHistory.stockActualCashAmount,
		FireMarshalClaimSendHistory.stockEstimatedLossAmount,
		FireMarshalClaimSendHistory.lossOfUsePolicyAmount,
		FireMarshalClaimSendHistory.lossOfUseTotalInsuredAmount,
		FireMarshalClaimSendHistory.lossOfUseReplacementAmount,
		FireMarshalClaimSendHistory.lossOfUseActualCashAmount,
		FireMarshalClaimSendHistory.lossOfUseEstimatedLossAmount,
		FireMarshalClaimSendHistory.otherPolicyAmount,
		FireMarshalClaimSendHistory.otherTotalInsuredAmount,
		FireMarshalClaimSendHistory.otherReplacementAmount,
		FireMarshalClaimSendHistory.otherActualCashAmount,
		FireMarshalClaimSendHistory.otherEstimatedLossAmount,
		FireMarshalClaimSendHistory.buildingReserveAmount,
		FireMarshalClaimSendHistory.stockReserveAmount,
		FireMarshalClaimSendHistory.stockPaidAmount,
		FireMarshalClaimSendHistory.lossOfUseReserve,
		FireMarshalClaimSendHistory.lossOfUsePaid,
		FireMarshalClaimSendHistory.otherReserveAmount,
		FireMarshalClaimSendHistory.otherPaidAmount,
		FireMarshalClaimSendHistory.isActive,
		FireMarshalClaimSendHistory.dateInserted
	FROM
		dbo.FireMarshalClaimSendHistory
	WHERE
		FireMarshalClaimSendHistory.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFireMarshalClaimSendHistory_elementalClaimId
	ON dbo.V_ActiveFireMarshalClaimSendHistory (elementalClaimId)
	WITH (FILLFACTOR = 90);
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
--CREATE TABLE dbo.FMClaimSendHistoryActivityLog
--(
--	fMClaimSendHistoryActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
--	CONSTRAINT PK_FMClaimSendHistoryActivityLog_fMClaimSendHistoryActivityLogId
--		PRIMARY KEY CLUSTERED (fMClaimSendHistoryActivityLogId)
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_FMClaimSendHistoryActivityLog_isSuccessful_stepId_executionDateTime
--	ON dbo.ElementalClaimActivityLog (isSuccessful, stepId, executionDateTime);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--EXEC sp_help 'dbo.FireMarshalClaimSendHistory'
--EXEC sp_help 'dbo.V_ActiveFireMarshalClaimSendHistory'
--EXEC sp_help 'dbo.FMClaimSendHistoryActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;