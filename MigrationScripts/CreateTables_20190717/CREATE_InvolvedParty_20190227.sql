SET NOEXEC OFF;

USE ClaimSearch_DEV
/*
*/	

BEGIN TRANSACTION

--/*
	--/*
	DROP VIEW dbo.V_ActiveInvolvedParty
	DROP VIEW dbo.V_ActiveAliaseInvolvedParty
	DROP VIEW dbo.V_ActiveNonAliaseServiceProvider
	DROP VIEW dbo.V_ActiveAliaseServiceProvider
	--*/
	--DROP TABLE dbo.InvolvedPartyActivityLog
	DROP TABLE dbo.InvolvedParty
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
--/***********************************************
--WorkItem: ISCCINTEL-2316
--Date: 2019-01-07
--Author: Robert David Warner
--Description: Logging table for InvolvedParty and the respective IP AddressMap.
				
--			Performance: No current notes.
--************************************************/
--CREATE TABLE dbo.InvolvedPartyActivityLog
--(
--	involvedPartyActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
--	CONSTRAINT PK_InvolvedPartyActivityLog_involvedPartyActivityLogId
--		PRIMARY KEY CLUSTERED (involvedPartyActivityLogId)
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_InvolvedPartyActivityLog_isSuccessful_stepId_executionDateTime
--	ON dbo.InvolvedPartyActivityLog (isSuccessful, stepId, executionDateTime);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--EXEC sp_help 'dbo.InvolvedParty'
--EXEC sp_help 'dbo.V_ActiveAliaseInvolvedParty'
--EXEC sp_help 'dbo.V_ActiveNonAliaseServiceProvider'
--EXEC sp_help 'dbo.V_ActiveAliaseServiceProvider'
--EXEC sp_help 'dbo.InvolvedPartyActivityLog' 

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;

/*
*/