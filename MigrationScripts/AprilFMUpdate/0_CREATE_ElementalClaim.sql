SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	DROP VIEW dbo.V_ActiveElementalClaim
	DROP TABLE dbo.ElementalClaimActivityLog
	DROP TABLE dbo.ElementalClaim
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
Description: Most granular level of a claim. Stores data related to
				lossType\CoverageType\Claim\IP\$

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.ElementalClaim
(   
	elementalClaimId BIGINT IDENTITY(1,1) NOT NULL,
	claimId BIGINT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	adjusterId BIGINT NULL,
	lossType CHAR(4) NULL,
	coverageType CHAR(4) NULL,
	dateClaimClosed DATE NULL,
	coverageStatus VARCHAR(3) NULL,
	settlementAmount MONEY NULL,
	estimatedLossAmount MONEY NULL,
	reserveAmount MONEY NULL,
	totalInsuredAmount MONEY NULL,
	policyAmount MONEY NULL,
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
	buildingPaidAmount MONEY NULL,
	contentReserveAmount MONEY NULL,
	contentPaidAmount MONEY NULL,
	stockReserveAmount MONEY NULL,
	stockPaidAmount MONEY NULL,
	lossOfUseReserve MONEY NULL,
	lossOfUsePaid MONEY NULL,
	otherReserveAmount MONEY NULL,
	otherPaidAmount MONEY NULL,

	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,

	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	CONSTRAINT PK_ElementalClaim_elementalClaimId
		PRIMARY KEY CLUSTERED (elementalClaimId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ElementalClaim_isoClaimId_involvedPartySequenceId
	ON dbo.ElementalClaim (isoClaimId, involvedPartySequenceId)
	INCLUDE (claimId, involvedPartyId, adjusterId, lossType, coverageType, dateClaimClosed, coverageStatus,
				settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount,
				policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount,
				buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount,
				buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount,
				contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount,
				stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount,
				stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount,
				lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount,
				lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
				otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount,
				buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount,
				stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount,
				otherPaidAmount);
CREATE NONCLUSTERED INDEX NIX_ElementalClaim_claimId_involvedPartyId_adjusterId
	ON dbo.ElementalClaim (claimId, involvedPartyId, adjusterId)
	INCLUDE (lossType, coverageType, dateClaimClosed, coverageStatus,
				settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount,
				policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount,
				buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount,
				buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount,
				contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount,
				stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount,
				stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount,
				lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount,
				lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
				otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount,
				buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount,
				stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount,
				otherPaidAmount, isoClaimId, involvedPartySequenceId);
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
CREATE VIEW dbo.V_ActiveElementalClaim
WITH SCHEMABINDING
AS
(
	SELECT
		ElementalClaim.elementalClaimId,
		ElementalClaim.claimId,
		ElementalClaim.involvedPartyId,
		ElementalClaim.adjusterId,
		ElementalClaim.lossType,
		ElementalClaim.coverageType,
		ElementalClaim.dateClaimClosed,
		ElementalClaim.coverageStatus,
		ElementalClaim.settlementAmount,
		ElementalClaim.estimatedLossAmount,
		ElementalClaim.reserveAmount,
		ElementalClaim.totalInsuredAmount,
		ElementalClaim.policyAmount,
		ElementalClaim.replacementAmount,
		ElementalClaim.actualCashAmount,
		ElementalClaim.buildingPolicyAmount,
		ElementalClaim.buildingTotalInsuredAmount,
		ElementalClaim.buildingReplacementAmount,
		ElementalClaim.buildingActualCashAmount,
		ElementalClaim.buildingEstimatedLossAmount,
		ElementalClaim.contentPolicyAmount,
		ElementalClaim.contentTotalInsuredAmount,
		ElementalClaim.contentReplacementAmount,
		ElementalClaim.contentActualCashAmount,
		ElementalClaim.contentEstimatedLossAmount,
		ElementalClaim.stockPolicyAmount,
		ElementalClaim.stockTotalInsuredAmount,
		ElementalClaim.stockReplacementAmount,
		ElementalClaim.stockActualCashAmount,
		ElementalClaim.stockEstimatedLossAmount,
		ElementalClaim.lossOfUsePolicyAmount,
		ElementalClaim.lossOfUseTotalInsuredAmount,
		ElementalClaim.lossOfUseReplacementAmount,
		ElementalClaim.lossOfUseActualCashAmount,
		ElementalClaim.lossOfUseEstimatedLossAmount,
		ElementalClaim.otherPolicyAmount,
		ElementalClaim.otherTotalInsuredAmount,
		ElementalClaim.otherReplacementAmount,
		ElementalClaim.otherActualCashAmount,
		ElementalClaim.otherEstimatedLossAmount,
		ElementalClaim.buildingReserveAmount,
		ElementalClaim.buildingPaidAmount,
		ElementalClaim.contentReserveAmount,
		ElementalClaim.contentPaidAmount,
		ElementalClaim.stockReserveAmount,
		ElementalClaim.stockPaidAmount,
		ElementalClaim.lossOfUseReserve,
		ElementalClaim.lossOfUsePaid,
		ElementalClaim.otherReserveAmount,
		ElementalClaim.otherPaidAmount,
		/*ElementalClaim.isActive,*/
		ElementalClaim.dateInserted,
		ElementalClaim.isoClaimId,
		ElementalClaim.involvedPartySequenceId
	FROM
		dbo.ElementalClaim
	WHERE
		ElementalClaim.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveElementalClaim_elementalClaimId
	ON dbo.V_ActiveElementalClaim (elementalClaimId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveElementalClaim_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveElementalClaim (isoClaimId, involvedPartySequenceId)
	INCLUDE (claimId, involvedPartyId, adjusterId, lossType, coverageType, dateClaimClosed, coverageStatus,
			settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount,
			policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount,
			buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount,
			buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount,
			contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount,
			stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount,
			stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount,
			lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount,
			lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
			otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount,
			buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount,
			stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount,
			otherPaidAmount);
CREATE NONCLUSTERED INDEX NIX_ActiveElementalClaim_claimId_involvedPartyId_adjusterId
	ON dbo.V_ActiveElementalClaim (claimId, involvedPartyId, adjusterId)
	INCLUDE (lossType, coverageType, dateClaimClosed, coverageStatus,
				settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount,
				policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount,
				buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount,
				buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount,
				contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount,
				stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount,
				stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount,
				lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount,
				lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
				otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount,
				buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount,
				stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount,
				otherPaidAmount, isoClaimId, involvedPartySequenceId);
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
Description: Logging table for InvolvedPartyAddressMap
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.ElementalClaimActivityLog
(
	elementalClaimActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
	productCode VARCHAR(50) NULL,
	sourceDateTime DATETIME2(0) NOT NULL,
	executionDateTime DATETIME2(0) NOT NULL,
	stepId TINYINT NOT NULL,
	stepDescription VARCHAR(1000) NULL,
	stepStartDateTime DATETIME2(0) NULL, 
	stepEndDateTime DATETIME2(0) NULL,
	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
	recordsAffected BIGINT NULL,
	isSuccessful BIT NOT NULL,
	stepExecutionNotes VARCHAR(1000) NULL,
	CONSTRAINT PK_ElementalClaim_elementalClaimActivityLogId
		PRIMARY KEY CLUSTERED (elementalClaimActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ElementalClaimActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.ElementalClaimActivityLog (isSuccessful, stepId, executionDateTime);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC sp_help 'dbo.ElementalClaim'
--EXEC sp_help 'dbo.V_ActiveElementalClaim'
--EXEC sp_help 'dbo.ElementalClaimActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/