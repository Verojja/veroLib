SET NOEXEC OFF;
--USE ClaimSearch_PROD

/*
	observe team naming conventions,
	remove indexes for SF(snowflake) implementation,
*/	

BEGIN TRANSACTION

/*
	/*Deprecated: Performane reasons*//*
	DROP VIEW dbo.V_ActiveNonAliasedInvolvedParty
	DROP VIEW dbo.V_ActiveAliasedInvolvedParty
	DROP VIEW dbo.V_ActiveNonAliasedServiceProvider
	DROP VIEW dbo.V_ActiveAliasedServiceProvider
	--*/
	
	DROP VIEW dbo.V_ActiveFMNonAliasedInvolvedParty
	DROP VIEW dbo.V_ActiveFMAliasedInvolvedParty
	DROP VIEW dbo.V_ActiveFMNonAliasedServiceProvider
	DROP VIEW dbo.V_ActiveFMAliasedServiceProvider
	
	DROP TABLE dbo.InvolvedPartyActivityLog
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
WorkItem: ??????
Date: 2019-01-07
Author: Robert David Warner, Daniel Ravaglia
Description: Generic Person object for representation of data captured about people and their aliases.
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedParty /*People*/
(
	involvedPartyId BIGINT IDENTITY(1,1) NOT NULL,
	isAliasOfInvolvedPartyId BIGINT NULL,
	isServiceProviderOfInvolvedPartyId BIGINT NULL,
	isBusiness BIT NOT NULL,
	involvedPartyRoleCode VARCHAR(2) NULL, /*C_ROLE */
	taxIdentificationNumberObfuscated VARCHAR(36) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	taxIdentificationNumberLastFour CHAR(4) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	socialSecurityNumberObfuscated VARCHAR(36) NULL,			-- DCR (N_SSN4 from clt7a
	socialSecurityNumberLastFour CHAR(4) NULL,			-- DCR (N_SSN4 from clt7a
	hICNObfuscated VARCHAR(36) NULL,							---4
	driversLicenseNumberObfuscated VARCHAR(15) NULL,			-- DCR (N_DRV_LIC from clt8a
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
	ON dbo.InvolvedParty (isoClaimId, involvedPartySequenceId);
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isAliasOfInvolvedPartyId
	ON dbo.InvolvedParty (isAliasOfInvolvedPartyId)
		INCLUDE (fullName);
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isServiceProviderOfInvolvedPartyId
	ON dbo.InvolvedParty (isServiceProviderOfInvolvedPartyId)
		INCLUDE (isAliasOfInvolvedPartyId, fullName);
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_socialSecurityNumber
	ON dbo.InvolvedParty (socialSecurityNumberLast4)
		WHERE
			InvolvedParty.isActive = 1
			AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
			AND InvolvedParty.socialSecurityNumber IS NOT NULL
*/
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_taxIdentificationNumber
	ON dbo.InvolvedParty (taxIdentificationNumberLast4)
		WHERE
			InvolvedParty.isActive = 1
			AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL;
*/
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_driversLicenseNumberLast3         
	ON dbo.InvolvedParty (driversLicenseNumberLast3)                   
		WHERE
			InvolvedParty.isActive = 1
			AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL;			
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMNonAliasedInvolvedParty
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty.isBusiness,
	InvolvedParty.involvedPartyRoleCode,
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
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMNonAliasedInvolvedParty_involvedPartyId
	ON dbo.V_ActiveFMNonAliasedInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMAliasedInvolvedParty
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty.involvedPartyId,
	isAliasOfInvolvedPartyId,
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty.isBusiness,
	InvolvedParty.involvedPartyRoleCode,
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
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMAliasedInvolvedParty_involvedPartyId
	ON dbo.V_ActiveFMAliasedInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMNonAliasedServiceProvider
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	InvolvedParty.isServiceProviderOfInvolvedPartyId,
	InvolvedParty.isBusiness,
	InvolvedParty.involvedPartyRoleCode,
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
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMNonAliasedServiceProvider_involvedPartyId
	ON dbo.V_ActiveFMNonAliasedServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMAliasedServiceProvider
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty.involvedPartyId,
	InvolvedParty.isAliasOfInvolvedPartyId,
	InvolvedParty.isServiceProviderOfInvolvedPartyId,
	InvolvedParty.isBusiness,
	InvolvedParty.involvedPartyRoleCode,
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
	/*isActive*/
	InvolvedParty.dateInserted,
	InvolvedParty.isoClaimId,
	InvolvedParty.involvedPartySequenceId
	FROM
		dbo.InvolvedParty
		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMAliasedServiceProvider_involvedPartyId
	ON dbo.V_ActiveFMAliasedServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.InvolvedPartyActivityLog
(
	involvedPartyActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_InvolvedPartyActivityLog_involvedPartyActivityLogId
		PRIMARY KEY CLUSTERED (involvedPartyActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyActivityLog_executionDateTime
	ON dbo.InvolvedPartyActivityLog (executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC sp_help 'dbo.InvolvedParty'
--EXEC sp_help 'dbo.V_ActiveFMNonAliasedInvolvedParty'
--EXEC sp_help 'dbo.V_ActiveFMAliasedInvolvedParty'
--EXEC sp_help 'dbo.V_ActiveFMNonAliasedServiceProvider'
--EXEC sp_help 'dbo.V_ActiveFMAliasedServiceProvider'
--EXEC sp_help 'dbo.InvolvedPartyActivityLog' 

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;

/*
COMMIT
20190130

		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty.isoClaimId = FM_ExtractFile.I_ALLCLM
*/