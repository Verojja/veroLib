SET NOEXEC OFF;

BEGIN TRANSACTION

DROP VIEW dbo.V_ActiveFireMarshalClaimSendHistory
DROP VIEW dbo.V_ActiveCurrentPendingFMClaim
DROP TABLE dbo.FireMarshalExtract
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
	claimId BIGINT NOT NULL,
	uniqueInstanceValue SMALLINT NOT NULL,
	
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
	adjusterId BIGINT NULL,
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	dateInserted DATETIME2(0) NOT NULL
	CONSTRAINT PK_FireMarshalExtract_claimId_uniqueInstanceValue
		PRIMARY KEY CLUSTERED (claimId, uniqueInstanceValue)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO


COMMIT TRANSACTION