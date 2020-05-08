SET NOEXEC OFF;

--USE CLAIMSEARCH_DEV;
USE CLAIMSEARCH_PROD;

BEGIN TRANSACTION;

--SELECT COUNT(*) AS table_viewCount
--FROM INFORMATION_SCHEMA.TABLES;

--SELECT COUNT(*) AS table_indexCount
--FROM sys.indexes;

SELECT * FROM sys.indexes;

/*DROP VIEWS (because schemabinding and I didn't save a schema bind togle*/
DROP VIEW dbo.V_ActiveLocationOfLoss;
DROP VIEW dbo.V_ActiveNonLocationOfLoss;
DROP VIEW dbo.V_ActiveAdjuster;
DROP VIEW dbo.V_ActivePolicy;
DROP VIEW dbo.V_ActiveClaim;
DROP VIEW dbo.V_ActiveInvolvedParty;
DROP VIEW dbo.V_ActiveAliaseInvolvedParty;
DROP VIEW dbo.V_ActiveNonAliaseServiceProvider;
DROP VIEW dbo.V_ActiveAliaseServiceProvider;
DROP VIEW dbo.V_ActiveIPAddressMap;
DROP VIEW dbo.V_ActiveElementalClaim;
DROP VIEW dbo.V_ActiveFireMarshalClaimSendHistory;
DROP VIEW dbo.V_ActiveCurrentPendingFMClaim;

/*Truncate tables*/
TRUNCATE TABLE dbo.FireMarshalDriver;
TRUNCATE TABLE dbo.Address;
TRUNCATE TABLE dbo.Adjuster;
TRUNCATE TABLE dbo.Policy;
TRUNCATE TABLE dbo.Claim;
TRUNCATE TABLE dbo.InvolvedParty;
TRUNCATE TABLE dbo.InvolvedPartyAddressMap;
TRUNCATE TABLE dbo.ElementalClaim;
TRUNCATE TABLE dbo.FireMarshalPendingClaim;
TRUNCATE TABLE dbo.FireMarshalClaimSendHistory;
TRUNCATE TABLE dbo.FireMarshalExtract;

UPDATE dbo.FireMarshalGenerationLog
	SET FireMarshalGenerationLog.isSuccessful = 0
WHERE
	FireMarshalGenerationLog.stepId = 200 /*stepId for secondary-finalStep of UpdateInsert HSP*/
	AND FireMarshalGenerationLog.isSuccessful = 1;
/*****************************************************************/
/*****************************************************************/
/*CREATE VIEW 1;*/
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
CREATE VIEW dbo.V_ActiveIPAddressMap
WITH SCHEMABINDING
AS
(
	SELECT
		InvolvedPartyAddressMap.involvedPartyId,
		InvolvedPartyAddressMap.claimId,
		InvolvedPartyAddressMap.nonLocationOfLossAddressId,
		InvolvedPartyAddressMap.claimRoleCode,
		/*InvolvedPartyAddressMap.isActive*/
		InvolvedPartyAddressMap.dateInserted,
		InvolvedPartyAddressMap.isoClaimId,
		InvolvedPartyAddressMap.involvedPartySequenceId
	FROM
		dbo.InvolvedPartyAddressMap
	WHERE
		InvolvedPartyAddressMap.isActive = 1
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveIPAddressMap_involvedPartyId_claimId_addressId_claimRoleCode
	ON dbo.V_ActiveIPAddressMap (involvedPartyId, claimId, nonLocationOfLossAddressId, claimRoleCode);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveIPAddressMap_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveIPAddressMap (isoClaimId, involvedPartySequenceId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*****************************************************************/
/*****************************************************************/
/*CREATE VIEW 2,3,4,5;*/
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
Description: Filtered Indexed View specific to InvolvedParty records
				that are both NOT Aliases, and NOT ServiceProviders.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveInvolvedParty
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty.isBusiness,
	/*InvolvedParty.involvedPartyRoleCode,*/
	InvolvedParty.taxIdentificationNumberObfuscated,
	InvolvedParty.taxIdentificationNumberLastFour,
	InvolvedParty.socialSecurityNumberObfuscated,
	InvolvedParty.socialSecurityNumberLastFour,
	InvolvedParty.hICNObfuscated,
	InvolvedParty.driversLicenseNumberObfuscated,
	InvolvedParty.driversLicenseNumberLast3,
	InvolvedParty.driversLicenseClass,
	InvolvedParty.driversLicenseState,
	InvolvedParty.genderCode,
	InvolvedParty.passportID,
	InvolvedParty.professionalMedicalLicense,
	InvolvedParty.isUnderSiuInvestigation,
	InvolvedParty.isLawEnforcementAction,
	InvolvedParty.isReportedToFraudBureau,
	InvolvedParty.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty.isMedicareEligible, */
	InvolvedParty.dateOfBirth,
	InvolvedParty.fullName,
	InvolvedParty.firstName,
	InvolvedParty.middleName,
	InvolvedParty.lastName,
	InvolvedParty.suffix,
	InvolvedParty.businessArea,
	InvolvedParty.businessTel,
	InvolvedParty.cellArea,
	InvolvedParty.cellTel,
	InvolvedParty.faxArea,
	InvolvedParty.faxTel,
	InvolvedParty.homeArea,
	InvolvedParty.homeTel,
	InvolvedParty.pagerArea,
	InvolvedParty.pagerTel,
	InvolvedParty.otherArea,
	InvolvedParty.otherTel,
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveInvolvedParty_involvedPartyId
	ON dbo.V_ActiveInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveInvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveInvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isBusiness, /*involvedPartyRoleCode, */taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour, hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction, isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel, cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel, dateInserted);
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
Description: Filtered Indexed View specific to InvolvedParty records
				that ARE Aliases, but are NOT ServiceProviders.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAliaseInvolvedParty
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty.involvedPartyId,
	isAliasOfInvolvedPartyId,
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty.isBusiness,
	/*InvolvedParty.involvedPartyRoleCode,*/
	InvolvedParty.taxIdentificationNumberObfuscated,
	InvolvedParty.taxIdentificationNumberLastFour,
	InvolvedParty.socialSecurityNumberObfuscated,
	InvolvedParty.socialSecurityNumberLastFour,
	InvolvedParty.hICNObfuscated,
	InvolvedParty.driversLicenseNumberObfuscated,
	InvolvedParty.driversLicenseNumberLast3,
	InvolvedParty.driversLicenseClass,
	InvolvedParty.driversLicenseState,
	InvolvedParty.genderCode,
	InvolvedParty.passportID,
	InvolvedParty.professionalMedicalLicense,
	InvolvedParty.isUnderSiuInvestigation,
	InvolvedParty.isLawEnforcementAction,
	InvolvedParty.isReportedToFraudBureau,
	InvolvedParty.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty.isMedicareEligible, */
	InvolvedParty.dateOfBirth,
	InvolvedParty.fullName,
	InvolvedParty.firstName,
	InvolvedParty.middleName,
	InvolvedParty.lastName,
	InvolvedParty.suffix,
	InvolvedParty.businessArea,
	InvolvedParty.businessTel,
	InvolvedParty.cellArea,
	InvolvedParty.cellTel,
	InvolvedParty.faxArea,
	InvolvedParty.faxTel,
	InvolvedParty.homeArea,
	InvolvedParty.homeTel,
	InvolvedParty.pagerArea,
	InvolvedParty.pagerTel,
	InvolvedParty.otherArea,
	InvolvedParty.otherTel,
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAliaseInvolvedParty_involvedPartyId
	ON dbo.V_ActiveAliaseInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAliaseInvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveAliaseInvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isBusiness, /*involvedPartyRoleCode, */taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour, hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction, isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel, cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel, dateInserted);
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
Description: Filtered Indexed View specific to InvolvedParty records
				that are NOT Aliases, but ARE ServiceProviders.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveNonAliaseServiceProvider
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	InvolvedParty.isServiceProviderOfInvolvedPartyId,
	InvolvedParty.isBusiness,
	/*InvolvedParty.involvedPartyRoleCode,*/
	InvolvedParty.taxIdentificationNumberObfuscated,
	InvolvedParty.taxIdentificationNumberLastFour,
	InvolvedParty.socialSecurityNumberObfuscated,
	InvolvedParty.socialSecurityNumberLastFour,
	InvolvedParty.hICNObfuscated,
	InvolvedParty.driversLicenseNumberObfuscated,
	InvolvedParty.driversLicenseNumberLast3,
	InvolvedParty.driversLicenseClass,
	InvolvedParty.driversLicenseState,
	InvolvedParty.genderCode,
	InvolvedParty.passportID,
	InvolvedParty.professionalMedicalLicense,
	InvolvedParty.isUnderSiuInvestigation,
	InvolvedParty.isLawEnforcementAction,
	InvolvedParty.isReportedToFraudBureau,
	InvolvedParty.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty.isMedicareEligible, */
	InvolvedParty.dateOfBirth,
	InvolvedParty.fullName,
	InvolvedParty.firstName,
	InvolvedParty.middleName,
	InvolvedParty.lastName,
	InvolvedParty.suffix,
	InvolvedParty.businessArea,
	InvolvedParty.businessTel,
	InvolvedParty.cellArea,
	InvolvedParty.cellTel,
	InvolvedParty.faxArea,
	InvolvedParty.faxTel,
	InvolvedParty.homeArea,
	InvolvedParty.homeTel,
	InvolvedParty.pagerArea,
	InvolvedParty.pagerTel,
	InvolvedParty.otherArea,
	InvolvedParty.otherTel,
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveNonAliaseServiceProvider_involvedPartyId
	ON dbo.V_ActiveNonAliaseServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveNonAliaseServiceProvider_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveNonAliaseServiceProvider (isoClaimId, involvedPartySequenceId)
	INCLUDE (isServiceProviderOfInvolvedPartyId, isBusiness, /*involvedPartyRoleCode, */taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour, hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction, isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel, cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel, dateInserted);
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
Description: Filtered Indexed View specific to InvolvedParty records
				that ARE BOTH Aliases, AND ServiceProviders.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAliaseServiceProvider
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty.involvedPartyId,
	InvolvedParty.isAliasOfInvolvedPartyId,
	InvolvedParty.isServiceProviderOfInvolvedPartyId,
	InvolvedParty.isBusiness,
	/*InvolvedParty.involvedPartyRoleCode,*/
	InvolvedParty.taxIdentificationNumberObfuscated,
	InvolvedParty.taxIdentificationNumberLastFour,
	InvolvedParty.socialSecurityNumberObfuscated,
	InvolvedParty.socialSecurityNumberLastFour,
	InvolvedParty.hICNObfuscated,
	InvolvedParty.driversLicenseNumberObfuscated,
	InvolvedParty.driversLicenseNumberLast3,
	InvolvedParty.driversLicenseClass,
	InvolvedParty.driversLicenseState,
	InvolvedParty.genderCode,
	InvolvedParty.passportID,
	InvolvedParty.professionalMedicalLicense,
	InvolvedParty.isUnderSiuInvestigation,
	InvolvedParty.isLawEnforcementAction,
	InvolvedParty.isReportedToFraudBureau,
	InvolvedParty.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty.isMedicareEligible, */
	InvolvedParty.dateOfBirth,
	InvolvedParty.fullName,
	InvolvedParty.firstName,
	InvolvedParty.middleName,
	InvolvedParty.lastName,
	InvolvedParty.suffix,
	InvolvedParty.businessArea,
	InvolvedParty.businessTel,
	InvolvedParty.cellArea,
	InvolvedParty.cellTel,
	InvolvedParty.faxArea,
	InvolvedParty.faxTel,
	InvolvedParty.homeArea,
	InvolvedParty.homeTel,
	InvolvedParty.pagerArea,
	InvolvedParty.pagerTel,
	InvolvedParty.otherArea,
	InvolvedParty.otherTel,
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAliaseServiceProvider_involvedPartyId
	ON dbo.V_ActiveAliaseServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAliaseServiceProvider_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveAliaseServiceProvider (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isServiceProviderOfInvolvedPartyId, isBusiness, /*involvedPartyRoleCode, */taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour, hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction, isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel, cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel, dateInserted);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*****************************************************************/
/*CREATE VIEW 6;*/

/*****************************************************************/
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
Author: Daniel Ravaglia and Robert David Warner
Description: Indexed view for Generic Policy Object,
				filtered to exclude non Active rows.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActivePolicy
WITH SCHEMABINDING
AS
(
	SELECT
		Policy.policyId,
		Policy.insuranceProviderCompanyCode,
		Policy.insuranceProviderOfficeCode,
		Policy.originalPolicyNumber,
		Policy.policyTypeCode,
		Policy.policyTypeDescription,
		Policy.originalPolicyInceptionDate,
		Policy.originalPolicyExperiationDate,
		Policy.dateInserted,
		Policy.isoClaimId
	FROM
		dbo.Policy
	WHERE
		Policy.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActivePolicy_policyId
	ON dbo.V_ActivePolicy (policyId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActivePolicy_isoClaimId
	ON dbo.V_ActivePolicy (isoClaimId)
	INCLUDE (insuranceProviderCompanyCode, insuranceProviderOfficeCode, originalPolicyNumber, policyTypeCode, policyTypeDescription, originalPolicyInceptionDate, originalPolicyExperiationDate, dateInserted);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*****************************************************************/
/*CREATE VIEW 7;*/

/*****************************************************************/
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
/*DONT UNCOMMMENT: THIS IS DEPRECATED!!! 20200123
CREATE UNIQUE CLUSTERED INDEX PK_ActiveCurrentPendingFMClaim_elementalClaimId_uniqueInstanceValue
	ON dbo.V_ActiveCurrentPendingFMClaim (elementalClaimId, uniqueInstanceValue)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO*/
/*****************************************************************/
/*CREATE VIEW 8,9;*/

/*****************************************************************/
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
Description: INDEXED VIEW representing addresses that correlate
				to the covered-incident of insurance claims.
************************************************/
CREATE VIEW dbo.V_ActiveLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
		originalAddressLine1,
		originalAddressLine2,
		originalCityName,
		originalStateCode,
		originalZipCode,
		scrubbedAddressLine1,
		scrubbedAddressLine2,
		scrubbedCityName,
		scrubbedStateCode,
		scrubbedZipCode,
		scrubbedZipCodeExtended,
		scrubbedCountyName,
		scrubbedCountyFIPS,
		scrubbedCountryCode,
		latitude,
		longitude,
		/*geolocation,*/
		geoAccuracy,
		dateInserted,
		melissaMappingKey,
		isoClaimId
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveLocationOfLoss_addressId
	ON dbo.V_ActiveLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_melissaMappingKey
	ON dbo.V_ActiveLocationOfLoss (melissaMappingKey)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_isoClaimId
	ON dbo.V_ActiveLocationOfLoss (isoClaimId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
/*CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_originalStateCode
	ON dbo.V_ActiveLocationOfLoss (originalStateCode)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
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
Description: INDEXED VIEW representing addresses that correlate
				to the mailing address for the Insured at the time of a claim.
************************************************/
CREATE VIEW dbo.V_ActiveNonLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
		originalAddressLine1,
		originalAddressLine2,
		originalCityName,
		originalStateCode,
		originalZipCode,
		scrubbedAddressLine1,
		scrubbedAddressLine2,
		scrubbedCityName,
		scrubbedStateCode,
		scrubbedZipCode,
		scrubbedZipCodeExtended,
		scrubbedCountyName,
		scrubbedCountyFIPS,
		scrubbedCountryCode,
		latitude,
		longitude,
		/*geolocation,*/
		geoAccuracy,
		dateInserted,
		melissaMappingKey,
		isoClaimId,
		involvedPartySequenceId
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 0
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveNonLocationOfLos_addressId
	ON dbo.V_ActiveNonLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_melissaMappingKey
	ON dbo.V_ActiveNonLocationOfLoss (melissaMappingKey)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_isoClaimId
	ON dbo.V_ActiveNonLocationOfLoss (isoClaimId, involvedPartySequenceId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*****************************************************************/
/*CREATE VIEW 10;*/

/*****************************************************************/
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
Description: Filtered Clustered Index for Active Insurance Adjuster(s)
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAdjuster
WITH SCHEMABINDING
AS
(
	SELECT
		Adjuster.adjusterId,
		Adjuster.adjusterCompanyCode,
		Adjuster.adjusterCompanyName,
		Adjuster.adjusterOfficeCode,
		Adjuster.adjusterDateSubmitted,
		Adjuster.adjusterName, 
		Adjuster.adjusterAreaCode,
		Adjuster.adjusterPhoneNumber,
		Adjuster.dateInserted,
		Adjuster.isoClaimId,
		Adjuster.involvedPartySequenceId,
		adjusterSequenceId
	FROM
		dbo.Adjuster
	WHERE
		Adjuster.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAdjuster_adjusterId
	ON dbo.V_ActiveAdjuster (adjusterId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAdjuster_isoClaimId_involvedPartySequenceId_adjusterSequenceId
	ON dbo.V_ActiveAdjuster (isoClaimId, involvedPartySequenceId, adjusterSequenceId)
	INCLUDE (adjusterCompanyCode, adjusterCompanyName, adjusterOfficeCode, adjusterDateSubmitted, adjusterName, adjusterAreaCode, adjusterPhoneNumber, dateInserted);	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*****************************************************************/
/*CREATE VIEW 11;*/
/*****************************************************************/
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
Description: Filtered Clustered Index for Active Insurance Claims.
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveClaim
WITH SCHEMABINDING
AS
(
	SELECT
		Claim.claimId,
		Claim.originalClaimNumber,
		Claim.locationOfLossAddressId,
		Claim.policyId,
		Claim.claimSearchSourceSystem,
		Claim.claimEntryMethod,
		Claim.isVoidedByInsuranceCarrier,
		Claim.lossDescription,
		Claim.lossDescriptionExtended,
		/*Claim.catastropheId,*/
		Claim.isClaimSearchProperty,
		Claim.isClaimSearchAuto,
		Claim.isClaimSearchCasualty,
		Claim.isClaimSearchAPD,
		isClaimUnderSIUInvestigation,
		siuCompanyName,
		siuRepresentativeFullName,
		siuWorkPhoneNumber,
		siuCellPhoneNumber,
		Claim.dateOfLoss,
		Claim.insuranceCompanyReceivedDate,
		Claim.systemDateReceived,
		/*Claim.isActive,*/
		Claim.dateInserted,
		Claim.isoClaimId
	FROM
		dbo.Claim
	WHERE
		Claim.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveClaim_claimId
	ON dbo.V_ActiveClaim (claimId)
	WITH (FILLFACTOR = 100);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveClaim_isoClaimId
	ON dbo.V_ActiveClaim (isoClaimId)
	INCLUDE (originalClaimNumber, policyId, claimSearchSourceSystem, claimEntryMethod, isVoidedByInsuranceCarrier, lossDescription, lossDescriptionExtended, isClaimSearchProperty, isClaimSearchAuto, isClaimSearchCasualty, isClaimSearchAPD,
	isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName, siuWorkPhoneNumber, siuCellPhoneNumber,
	dateOfLoss, insuranceCompanyReceivedDate, systemDateReceived, dateInserted)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/**********************************************************************************/
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
		ElementalClaim.lossTypeCode,
		ElementalClaim.lossTypeDescription,
		ElementalClaim.coverageTypeCode,
		ElementalClaim.coverageTypeDescription,
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
	INCLUDE (claimId, involvedPartyId, adjusterId, lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed, coverageStatus,
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
	INCLUDE (lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed, coverageStatus,
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
/**********************************************************************************/
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
/* DONT UNCOMMMENT: THIS IS DEPRECATED!!! 20200123
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFireMarshalClaimSendHistory_elementalClaimId
	ON dbo.V_ActiveFireMarshalClaimSendHistory (elementalClaimId)
	WITH (FILLFACTOR = 90);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO*/
/**********************************************************************************/
--SELECT COUNT(*) AS objectCount_postChange FROM INFORMATION_SCHEMA.TABLES;
--SELECT COUNT(*) AS objectCount_postChange FROM sys.indexes;
--SELECT * FROM sys.indexes;
--SELECT
--	*
--FROM sys.indexes
--WHERE
--	indexes.name IN
--(	
--'PK_ActiveFireMarshalClaimSendHistory_elementalClaimId',
--'PK_ActiveCurrentPendingFMClaim_elementalClaimId_uniqueInstanceValue'
--)

/*move on to script 2	*/
--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;