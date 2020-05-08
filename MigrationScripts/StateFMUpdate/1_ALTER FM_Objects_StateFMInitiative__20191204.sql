SET NOEXEC OFF;

USE ClaimSearch_Prod;
--USE ClaimSearch_Dev;

/******MSGLog Snippet. Can be added to comment block at end of query after execute for recordkeeping.******/
DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(2) = CHAR(13) + CHAR(10);
DECLARE @currentDBEnv VARCHAR(100) = CAST(@@SERVERNAME + '.' + DB_NAME() AS VARCHAR(100));
DECLARE @currentUser VARCHAR(100) = CAST(CURRENT_USER AS VARCHAR(100));
DECLARE @executeTimestamp VARCHAR(20) = CAST(GETDATE() AS VARCHAR(20));
Print '*****************************************' + @newLine
	+ '*' + @tab + 'Env: ' + 
	+ CASE
	WHEN
		LEN(@currentDBEnv) >=27
	THEN
		@currentDBEnv
	ELSE
		@currentDBEnv + @tab
	END
	+ @tab + '*' +@newLine
	+ '*' + @tab + 'User: ' + @currentUser + @tab + @tab + @tab + @tab + '*' +@newLine
	+ '*' + @tab + 'Time: ' + @executeTimestamp + @tab + @tab + @tab + '*' +@newLine
	+'*****************************************';
/**********************************************************************************************************/
BEGIN TRANSACTION
--/*
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
ALTER TABLE dbo.FireMarshalPendingClaim
ADD
	lossGeoCountyFipsCode CHAR(5) NULL;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
ALTER TABLE dbo.FireMarshalClaimSendHistory
ADD
	lossGeoCountyFipsCode CHAR(5) NULL;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
ALTER TABLE dbo.FireMarshalExtract
ADD
	lossGeoCountyFipsCode CHAR(5) NULL,
	adjusterName VARCHAR(70) NULL,
	adjusterPhoneNumber INT NULL;
	/*No Adjuster "zip code" available in GIM, && I spoke with the product owner\business authority on FireMarshal (Adams, Stephen)
		who said that the AdjusterZip is not required for this release. 20191204*/
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
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
************************************************/
ALTER VIEW dbo.V_ActiveFireMarshalClaimSendHistory
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
		FireMarshalClaimSendHistory.involvedPartyFullName,
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
		FireMarshalClaimSendHistory.dateInserted,
		FireMarshalClaimSendHistory.lossGeoCountyFipsCode
	FROM
		dbo.FireMarshalClaimSendHistory
	WHERE
		FireMarshalClaimSendHistory.isActive = 1
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
Date: 2019-04-30
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
************************************************/
ALTER VIEW dbo.V_ActiveCurrentPendingFMClaim
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
		FireMarshalPendingClaim.involvedPartyFullName,
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
		FireMarshalPendingClaim.dateInserted,
		FireMarshalPendingClaim.lossGeoCountyFipsCode
	FROM
		dbo.FireMarshalPendingClaim
	WHERE
		FireMarshalPendingClaim.isActive = 1
		AND FireMarshalPendingClaim.isCurrent = 1
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

--PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  1:48PM			*
*****************************************
COMMIT TRANSACTION
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  1:58PM			*
*****************************************
COMMIT TRANSACTION

*/