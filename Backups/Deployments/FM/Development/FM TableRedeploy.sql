SET NOEXEC OFF;

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

SET NOCOUNT ON;
BEGIN TRANSACTION

/***********************************************
Prep for Table Re-CREATE:
************************************************/
	
	DROP VIEW dbo.V_ActiveInvolvedParty
	DROP VIEW dbo.V_ActiveAliaseInvolvedParty
	DROP VIEW dbo.V_ActiveNonAliaseServiceProvider
	DROP VIEW dbo.V_ActiveAliaseServiceProvider
	
	DROP VIEW dbo.V_ActiveIPAddressMap
	DROP VIEW dbo.V_ActiveElementalClaim

	DROP VIEW dbo.V_ActiveCurrentPendingFMClaim
	DROP VIEW dbo.V_ActiveFireMarshalClaimSendHistory

	DROP TABLE dbo.ElementalClaim
	DROP TABLE dbo.InvolvedPartyAddressMap
	DROP TABLE dbo.InvolvedParty
	
	DROP TABLE dbo.FireMarshalPendingClaim
	DROP TABLE dbo.FireMarshalClaimSendHistory
	DROP TABLE dbo.FireMarshalExtract
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
	lossTypeCode CHAR(4) NULL,
	lossTypeDescription VARCHAR(42) NULL,
	coverageTypeCode CHAR(4) NULL,
	coverageTypeDescription VARCHAR(42) NULL,
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
CREATE NONCLUSTERED INDEX NIX_ElementalClaim_claimId_involvedPartyId_adjusterId
	ON dbo.ElementalClaim (claimId, involvedPartyId, adjusterId)
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
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner, Daniel Ravaglia
Description: Generic Person object for representation of people
				their aliases, and their service providers.
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedParty /*People*/
(
	involvedPartyId BIGINT IDENTITY(1,1) NOT NULL,
	isAliasOfInvolvedPartyId BIGINT NULL,
	isServiceProviderOfInvolvedPartyId BIGINT NULL,
	isBusiness BIT NOT NULL,
	/*Deprecated: moved to claimIPMap*//*involvedPartyRoleCode VARCHAR(2) NULL, /*C_ROLE */*/
	taxIdentificationNumberObfuscated VARCHAR(36) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	taxIdentificationNumberLastFour CHAR(4) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	socialSecurityNumberObfuscated VARCHAR(36) NULL,			-- DCR (N_SSN4 from clt7a
	socialSecurityNumberLastFour CHAR(4) NULL,			-- DCR (N_SSN4 from clt7a
	hICNObfuscated VARCHAR(36) NULL,							---4
	driversLicenseNumberObfuscated VARCHAR(52) NULL,			-- DCR (N_DRV_LIC from clt8a
	driversLicenseNumberLast3 CHAR(3) NULL,			-- DCR (N_DRV_LIC from clt8a
	driversLicenseClass VARCHAR(3) NULL,			-- DCR (C_DRV_LIC_CLASS from clt8a
	driversLicenseState CHAR(2) NULL,				-- DCR (C_ST_ALPH from clt8a
	genderCode CHAR(2) NULL,						-- DCR {C_GEND from clt4
	passportID VARCHAR(9) NULL,							-- DCR {N_PSPRT from clt4
	professionalMedicalLicense VARCHAR(20) NULL,	-- DCR (N_PROF_MED_LIC FROM CLT10
	isUnderSiuInvestigation BIT NOT NULL,			-- DCR {F_SIU_INVST from clt4
	isLawEnforcementAction BIT NOT NULL,			-- DCR {F_ENF_ACTN
	isReportedToFraudBureau BIT NOT NULL,			-- DCR {F_FRAUD_BUR_RPT
	isFraudReported BIT NOT NULL,					-- DCR {F_FRAUD_OCUR
	dateOfBirth DATE NULL,
	fullName /*M_FUL_NM*/ VARCHAR(70) NULL,
	firstName VARCHAR(100) NULL,
	middleName VARCHAR(100) NULL,
	lastName VARCHAR(100) NULL,
	suffix VARCHAR(50) NULL,
	businessArea CHAR(3) NULL,
	businessTel CHAR(7) NULL,
	cellArea CHAR(3) NULL,
	cellTel CHAR(7) NULL,
	faxArea CHAR(3) NULL,
	faxTel CHAR(7) NULL,
	homeArea CHAR(3) NULL,
	homeTel CHAR(7) NULL,
	pagerArea CHAR(3) NULL,
	pagerTel CHAR(7) NULL,
	otherArea CHAR(3) NULL,
	otherTel CHAR(7) NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	CONSTRAINT PK_InvolvedParty_involvedPartyId
		PRIMARY KEY (involvedPartyId)
	/*CONSTRAINT FK_InvolvedParty_isAliasOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isAliasOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty (involvedPartyId),
	CONSTRAINT FK_InvolvedParty_isServiceProviderOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isServiceProviderOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty (involvedPartyId)
	*/
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.InvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isServiceProviderOfInvolvedPartyId, isBusiness, /*involvedPartyRoleCode,*/ taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour, hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction, isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel, cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel, dateInserted);
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
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-24
Author: Daniel Ravaglia and Robert David Warner
Description: Maping between InvolvedParty and HighLevel-Claim.
			  This is already a many-to-many, althoug there exists
			  an even lower-level of granularity to be aware of (elemental claim).

			  The thing to keep in mind here is that the non PrimaryKeyValues be
			  functionally dependent upon the entire primary key.
			  
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedPartyAddressMap
(   
	involvedPartyId BIGINT NOT NULL,
	claimId BIGINT NOT NULL,
	nonLocationOfLossAddressId BIGINT NOT NULL,
	claimRoleCode VARCHAR(2) NOT NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	CONSTRAINT PK_IPAddressMap_involvedPartyId_claimId_addressId_claimRoleCode
		PRIMARY KEY CLUSTERED (involvedPartyId, claimId, nonLocationOfLossAddressId, claimRoleCode) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyAddressMap_isoClaimId_involvedPartySequenceId
	ON dbo.InvolvedPartyAddressMap (isoClaimId, involvedPartySequenceId);
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
	involvedPartyFullName VARCHAR(250) NULL,	
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
	involvedPartyFullName VARCHAR(250) NULL,
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
			lossTypeCode, lossTypeDescription, policyTypeCode, policyTypeDescription, coverageTypeCode, coverageTypeDescription, settlementAmount, estimatedLossAmount, policyAmount, isIncendiaryFire, isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName, siuWorkPhoneNumber, siuCellPhoneNumber, involvedPartyId, involvedPartyFullName, adjusterId, involvedPartySequenceId, reserveAmount, totalInsuredAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount,
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
UPDATE dbo.InvolvedPartyActivityLog
	SET
		InvolvedPartyActivityLog.isSuccessful =0,
		InvolvedPartyActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	InvolvedPartyActivityLog.isSuccessful = 1;
UPDATE dbo.IPAddressMapActivityLog
	SET
		IPAddressMapActivityLog.isSuccessful =0,
		IPAddressMapActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	IPAddressMapActivityLog.isSuccessful = 1;
	
UPDATE dbo.ElementalClaimActivityLog
	SET
		ElementalClaimActivityLog.isSuccessful =0,
		ElementalClaimActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	ElementalClaimActivityLog.isSuccessful = 1;

UPDATE dbo.FMClaimSendHistoryActivityLog
	SET
		FMClaimSendHistoryActivityLog.isSuccessful =0,
		FMClaimSendHistoryActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	FMClaimSendHistoryActivityLog.isSuccessful = 1;
UPDATE dbo.FMPendingClaimActivityLog
	SET
		FMPendingClaimActivityLog.isSuccessful =0,
		FMPendingClaimActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	FMPendingClaimActivityLog.isSuccessful = 1;
UPDATE dbo.FireMarshalExtractActivityLog
	SET
		FireMarshalExtractActivityLog.isSuccessful =0,
		FireMarshalExtractActivityLog.stepExecutionNotes = 'FullHistoryLoad_20190617'
WHERE
	FireMarshalExtractActivityLog.isSuccessful = 1;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*Alter statement for IP Sproc*/
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for data-refresh of the InvolvedParty Table.
				Also inserts new associations into the IPACRMap.

			Performance:
				Use of TempTable index (adds 6 seconds, but significantly reduces runtime of subsequent steps
				Use of WindowfunctionPartition for Duplicate removal (35% performance gain over Distinct).
				Explore LOCK_ESCALATION at the partition level 
					set the LOCK_ESCALATION option of the ALTER TABLE statement to AUTO.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertInvolvedParty
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.InvolvedPartyActivityLog
				WHERE
					InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND InvolvedPartyActivityLog.isSuccessful = 1
					AND InvolvedPartyActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(InvolvedPartyActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.InvolvedPartyActivityLog WITH (NOLOCK)
			WHERE
				InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND InvolvedPartyActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CreateSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			/*NOTE: potentially update this step if/when IP Claim-Role changes source*/	
			SELECT
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyId,
				DuplicateDataSetPerformanceHackMelissaNameMap.isBusiness,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyRoleCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.hICNObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberLast3,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseClass,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseState,
				DuplicateDataSetPerformanceHackMelissaNameMap.genderCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.passportID,
				DuplicateDataSetPerformanceHackMelissaNameMap.professionalMedicalLicense,
				DuplicateDataSetPerformanceHackMelissaNameMap.isUnderSiuInvestigation,
				DuplicateDataSetPerformanceHackMelissaNameMap.isLawEnforcementAction,
				DuplicateDataSetPerformanceHackMelissaNameMap.isReportedToFraudBureau,
				DuplicateDataSetPerformanceHackMelissaNameMap.isFraudReported,
				DuplicateDataSetPerformanceHackMelissaNameMap.dateOfBirth,
				DuplicateDataSetPerformanceHackMelissaNameMap.fullName,
				DuplicateDataSetPerformanceHackMelissaNameMap.firstName,
				DuplicateDataSetPerformanceHackMelissaNameMap.middleName,
				DuplicateDataSetPerformanceHackMelissaNameMap.lastName,
				DuplicateDataSetPerformanceHackMelissaNameMap.suffix,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartySequenceId
				INTO #ScrubbedNameData
			FROM
				(
					SELECT
						ExistingInvolvedParty.involvedPartyId,
						NULLIF(CAST(LTRIM(RTRIM(CLT00004.I_ALLCLM)) AS VARCHAR(11)),'') AS isoClaimId,
						CAST(CLT00004.I_NM_ADR AS INT) AS involvedPartySequenceId,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00004.I_ALLCLM,
								CLT00004.I_NM_ADR
							ORDER BY
								CLT00004.Date_Insert DESC
						) AS uniqueInstanceValue,
						CASE
							WHEN
								CLT00004.C_NM_TYP = 'B'
							THEN
								CAST(1 AS BIT)
							ELSE
								CAST(0 AS BIT)
						END AS isBusiness,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_ROLE)),'') AS VARCHAR(2)) AS involvedPartyRoleCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00035.N_TIN)),'') AS VARCHAR(36)) AS taxIdentificationNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0035A.TIN_4)),'') AS CHAR(4)) AS taxIdentificationNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00007.N_SSN)),'') AS VARCHAR(36)) AS socialSecurityNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0007A.SSN_4)),'') AS VARCHAR(4)) AS socialSecurityNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_HICN_MEDCR)),'') AS VARCHAR(36)) AS hICNObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.N_DRV_LIC)),'') AS VARCHAR(52)) AS driversLicenseNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.N_DRV_LIC)),'') AS VARCHAR(3)) AS driversLicenseNumberLast3,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_ST_ALPH)),'') AS VARCHAR(2)) AS driversLicenseState,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_GEND)),'') AS CHAR(2)) AS genderCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.N_PSPRT)),'') AS VARCHAR(9)) passportID,
						CAST(NULLIF(LTRIM(RTRIM(CLT00010.N_PROF_MED_LIC)),'') AS VARCHAR(20)) professionalMedicalLicense,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isUnderSiuInvestigation,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isLawEnforcementAction,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isReportedToFraudBureau,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)*/
							ELSE
								CAST(0 AS BIT)
						END AS isFraudReported,
						CLT00004.D_BRTH AS dateOfBirth,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.M_FUL_NM)),'') AS VARCHAR(70)) AS fullName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_FirstName1*/NULL)),'') AS VARCHAR(100)) AS firstName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_MiddleName1*/NULL)),'') AS VARCHAR(100)) AS middleName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_LastName1*/NULL)),'') AS VARCHAR(100)) AS lastName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_Suffix1*/NULL)),'') AS VARCHAR(50)) suffix,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[B])-6,
								7,
								''
							),
						3) AS businessArea,
						RIGHT(CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),7) AS businessTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[C])-6,
								7,
								''
							),
						3) AS cellArea,
						RIGHT(CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),7) AS cellTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[F])-6,
								7,
								''
							),
						3) AS faxArea,
						RIGHT(CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),7) AS faxTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[H])-6,
								7,
								''
							),
						3) AS homeArea,
						RIGHT(CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),7) AS homeTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[P])-6,
								7,
								''
							),
						3) AS pagerArea,
						RIGHT(CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),7) AS pagerTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[*])-6,
								7,
								''
							),
						3) AS otherArea,
						RIGHT(CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),7) AS otherTel
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
						/*DEVNOTE: Deprecated; not using scrubed value at present and significant performance hit*//*
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
							ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
							ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey*/
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0007A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0007A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0007A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0008A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0008A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0008A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0035A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0035A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0035A.I_NM_ADR
						LEFT OUTER JOIN (
							SELECT
								CLT00009.I_ALLCLM,
								CLT00009.I_NM_ADR,
								CAST(
									CAST(
										RIGHT('00000' + CAST(CLT00009.N_AREA AS VARCHAR(3)),3)
										+ RIGHT('00000' + CAST(CLT00009.N_TEL AS VARCHAR(7)),7)
									AS CHAR(10))
								 AS BIGINT) AS phoneNumberValue,
								ISNULL(CLT00009.C_TEL_TYP, '*') AS phoneNumberType
							FROM
								dbo.FireMarshalDriver AS INNERFM_ExtractFile WITH (NOLOCK)
								INNER JOIN ClaimSearch_Prod.dbo.CLT00009 WITH (NOLOCK)
									ON INNERFM_ExtractFile.isoClaimId = CLT00009.I_ALLCLM
							) PhoneNumbers PIVOT
							(
								SUM(phoneNumberValue)
								FOR phoneNumberType IN
								(
									[B],
									[C],
									[F],
									[H],
									[P],
									[*]
								)
						) AS PhoneNumbersPivoted
							ON CLT00004.I_ALLCLM = PhoneNumbersPivoted.I_ALLCLM
								AND CLT00004.I_NM_ADR = PhoneNumbersPivoted.I_NM_ADR
						LEFT OUTER JOIN dbo.InvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
								AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
					WHERE
						/*Deprecating due to performance costs, and current profile state. RDW 20190306:
							NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
							AND CLT00004.I_NM_ADR IS NOT NULL
						*/
						CLT00004.Date_Insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
				) AS DuplicateDataSetPerformanceHackMelissaNameMap
			WHERE
				DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;	

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'AddIndexToSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			CREATE UNIQUE CLUSTERED INDEX PK_ScrubbedNameData_isoClaimId_IPSequenceId
				ON #ScrubbedNameData (isoClaimId, involvedPartySequenceId);
			CREATE NONCLUSTERED INDEX NIX_ScrubbedNameData_involvedPartyId
				ON #ScrubbedNameData (involvedPartyId);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;	

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'CaptureNonAliasedInvolvedPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				/*NULL AS isAliasOfInvolvedPartyId,*/
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMNonAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'UpdateFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 5,
				@stepDescription = 'InsertNewFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.involvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 6,
				@stepDescription = 'CaptureAliasedInvolvedPartyToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				#ScrubbedNameData.involvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 7,
				@stepDescription = 'UpdateFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 8,
				@stepDescription = 'InsertNewFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.involvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 9,
				@stepDescription = 'CaptureNonAliasedServiceProviderDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				/*NULL isAliasOfInvolvedPartyId,*/
				COALESCE(ServiceProviderInvolvedParty.involvedPartyId, DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId) AS isServiceProviderOfInvolvedPartyId, /*COALESECE to protect validity of IP*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMNonAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty WITH (NOLOCK)
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 10,
				@stepDescription = 'UpdateFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 11,
				@stepDescription = 'InsertNewFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.involvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 12,
				@stepDescription = 'CaptureFMAliasedServiceProviderPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				ServiceProviderInvolvedParty.involvedPartyId AS isServiceProviderOfInvolvedPartyId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 13,
				@stepDescription = 'UpdateFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 14,
				@stepDescription = 'InsertNewFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#FMAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.involvedPartyId IS NULL;
			--OPTION (RECOMPILE);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 15,
				@stepDescription = 'CreateIPAMDataTempTable',
				@stepStartDateTime = GETDATE();

			SELECT
				InvolvedParty.involvedPartyId,
				V_ActiveClaim.claimId,
				V_ActiveNonLocationOfLoss.addressId AS nonLocationOfLossAddressId,
				ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK') AS claimRoleCode,
				1 AS isActive,
				/*@dateInserted AS dateInserted,*/
				InvolvedParty.isoClaimId,
				InvolvedParty.involvedPartySequenceId,
				InvolvedPartyAddressMap.claimId as existingIPAMClaimId
				INTO #IPAMData
			FROM
				#ScrubbedNameData
				INNER JOIN dbo.V_ActiveClaim
					ON #ScrubbedNameData.isoClaimId = V_ActiveClaim.isoClaimId
				INNER JOIN dbo.InvolvedParty
					ON #ScrubbedNameData.isoClaimId = InvolvedParty.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedParty.involvedPartySequenceId
				INNER JOIN dbo.V_ActiveNonLocationOfLoss
					ON #ScrubbedNameData.isoClaimId = V_ActiveNonLocationOfLoss.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = V_ActiveNonLocationOfLoss.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedPartyAddressMap
					ON #ScrubbedNameData.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 16,
				@stepDescription = 'UpdateIPAMData',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.InvolvedPartyAddressMap
				SET
					InvolvedPartyAddressMap.involvedPartyId = SOURCE.involvedPartyId,
					InvolvedPartyAddressMap.claimId = SOURCE.claimId,
					InvolvedPartyAddressMap.nonLocationOfLossAddressId = SOURCE.nonLocationOfLossAddressId,
					InvolvedPartyAddressMap.claimRoleCode = SOURCE.claimRoleCode,
					/*InvolvedPartyAddressMap.isActive = SOURCE.isActive,*/
					InvolvedPartyAddressMap.dateInserted = @dateInserted
					/*isoClaimId*/
					/*involvedPartySequenceId*/
			FROM
				#IPAMData AS SOURCE	
				INNER JOIN dbo.InvolvedPartyAddressMap
					ON SOURCE.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND SOURCE.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId
			WHERE
				SOURCE.existingIPAMClaimId IS NOT NULL
				AND
				(
					InvolvedPartyAddressMap.involvedPartyId <> SOURCE.involvedPartyId
					OR InvolvedPartyAddressMap.claimId <> SOURCE.claimId
					OR InvolvedPartyAddressMap.nonLocationOfLossAddressId <> SOURCE.nonLocationOfLossAddressId
					OR InvolvedPartyAddressMap.claimRoleCode <> SOURCE.claimRoleCode
					/*OR InvolvedPartyAddressMap.isActive <> SOURCE.isActive
					OR InvolvedPartyAddressMap.dateInserted <> SOURCE.dateInserted
					OR InvolvedPartyAddressMap.isoClaimId <> SOURCE.isoClaimId
					OR InvolvedPartyAddressMap.involvedPartySequenceId <> SOURCE.involvedPartySequenceId*/
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;			
				
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'InsertNewIPAddressMapping(s)',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedPartyAddressMap
			(
				involvedPartyId,
				claimId,
				nonLocationOfLossAddressId,
				claimRoleCode,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				InvolvedParty.involvedPartyId,
				V_ActiveClaim.claimId,
				V_ActiveNonLocationOfLoss.addressId,
				ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK'),
				1 AS isActive,
				@dateInserted,
				InvolvedParty.isoClaimId,
				InvolvedParty.involvedPartySequenceId
			FROM
				#ScrubbedNameData
				INNER JOIN dbo.V_ActiveClaim
					ON #ScrubbedNameData.isoClaimId = V_ActiveClaim.isoClaimId
				INNER JOIN dbo.InvolvedParty
					ON #ScrubbedNameData.isoClaimId = InvolvedParty.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedParty.involvedPartySequenceId
				INNER JOIN dbo.V_ActiveNonLocationOfLoss
					ON #ScrubbedNameData.isoClaimId = V_ActiveNonLocationOfLoss.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = V_ActiveNonLocationOfLoss.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedPartyAddressMap
					ON #ScrubbedNameData.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId
			WHERE
				InvolvedPartyAddressMap.isoCLaimId IS NULL;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		PRINT'in catch';
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			PRINT'ROLLBACK in catch, int tran count: ' + CAST(@internalTransactionCount AS VARCHAR(100));
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();
		PRINT'pre log activity';

		/*Log Activity*/
		INSERT INTO dbo.InvolvedPartyActivityLog
		(
			productCode,
			sourceDateTime,
			executionDateTime,
			stepId,
			stepDescription,
			stepStartDateTime,
			stepEndDateTime,
			recordsAffected,
			isSuccessful,
			stepExecutionNotes
		)
		SELECT
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		PRINT'post log activity';
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
		PRINT'in raiseError Ifblock (int-tran-count=0)';
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertInvolvedParty; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

EXEC dbo.hsp_UpdateInsertInvolvedParty

/*Instructions:
	After you run this script,
	Please do a manual execute of:
		1.) ElementalClaim
			EXEC dbo.hsp_UpdateInsertElementalClaim
		2.) PendingClaim
			EXEC dbo.hsp_UpdateInsertFMPendingClaim
		3.) Historic
			EXEC dbo.hsp_UpdateInsertFMHistoricClaim
		4.) Open and execute the 'FM_OneTimeMonthlyGenerateHack_20190618' script; (see email attachment)
			FM_InvolvedPartyAlter_withDataRefresh__20190618.sql
		5.) Extract
			EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	*/

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/