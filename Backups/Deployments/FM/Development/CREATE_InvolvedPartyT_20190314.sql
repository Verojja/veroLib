SET NOEXEC OFF;
USE ClaimSearch_DEV

/*
	obf comes from #a
*/	

BEGIN TRANSACTION

--/*
	--/*
	DROP VIEW dbo.V_ActiveInvolvedParty_T
	DROP VIEW dbo.V_ActiveFMAliaseInvolvedParty_T
	DROP VIEW dbo.V_ActiveFMNonAliaseServiceProvider_T
	DROP VIEW dbo.V_ActiveFMAliaseServiceProvider_T
	--*/
	DROP TABLE dbo.InvolvedParty_TActivityLog
	DROP TABLE dbo.InvolvedParty_T
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
CREATE TABLE dbo.InvolvedParty_T /*People*/
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
	CONSTRAINT PK_InvolvedPartyT_involvedPartyId
		PRIMARY KEY (involvedPartyId)
	/*CONSTRAINT FK_InvolvedParty_isAliasOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isAliasOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty_T (involvedPartyId),
	CONSTRAINT FK_InvolvedParty_isServiceProviderOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isServiceProviderOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty_T (involvedPartyId)
	*/
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyT_isoClaimId_involvedPartySequenceId
	ON dbo.InvolvedParty_T (isoClaimId, involvedPartySequenceId);
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyT_isAliasOfInvolvedPartyId
	ON dbo.InvolvedParty_T (isAliasOfInvolvedPartyId)
		INCLUDE (fullName);
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyT_isServiceProviderOfInvolvedPartyId
	ON dbo.InvolvedParty_T (isServiceProviderOfInvolvedPartyId)
		INCLUDE (isAliasOfInvolvedPartyId, fullName);
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_socialSecurityNumber
	ON dbo.InvolvedParty_T (socialSecurityNumberLast4)
		WHERE
			InvolvedParty_T.isActive = 1
			AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NULL
			AND InvolvedParty_T.socialSecurityNumber IS NOT NULL
*/
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_taxIdentificationNumber
	ON dbo.InvolvedParty_T (taxIdentificationNumberLast4)
		WHERE
			InvolvedParty_T.isActive = 1
			AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NULL;
*/
/*DevNote: Opting to not use the FUK here since the data could be bad and we dont have time 
	to sanitize right now*//*CREATE UNIQUE NONCLUSTERED INDEX FUK_driversLicenseNumberLast3         
	ON dbo.InvolvedParty_T (driversLicenseNumberLast3)                   
		WHERE
			InvolvedParty_T.isActive = 1
			AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NULL;			
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveInvolvedParty_T
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty_T.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty_T.isBusiness,
	InvolvedParty_T.involvedPartyRoleCode,
	InvolvedParty_T.taxIdentificationNumberObfuscated,
	InvolvedParty_T.taxIdentificationNumberLastFour,
	InvolvedParty_T.socialSecurityNumberObfuscated,
	InvolvedParty_T.socialSecurityNumberLastFour,
	InvolvedParty_T.hICNObfuscated,
	InvolvedParty_T.driversLicenseNumberObfuscated,
	InvolvedParty_T.driversLicenseNumberLast3,
	InvolvedParty_T.driversLicenseClass,
	InvolvedParty_T.driversLicenseState,
	InvolvedParty_T.genderCode,
	InvolvedParty_T.passportID,
	InvolvedParty_T.professionalMedicalLicense,
	InvolvedParty_T.isUnderSiuInvestigation,
	InvolvedParty_T.isLawEnforcementAction,
	InvolvedParty_T.isReportedToFraudBureau,
	InvolvedParty_T.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty_T.isMedicareEligible, */
	InvolvedParty_T.dateOfBirth,
	InvolvedParty_T.fullName,
	InvolvedParty_T.firstName,
	InvolvedParty_T.middleName,
	InvolvedParty_T.lastName,
	InvolvedParty_T.suffix,
	InvolvedParty_T.businessArea,
	InvolvedParty_T.businessTel,
	InvolvedParty_T.cellArea,
	InvolvedParty_T.cellTel,
	InvolvedParty_T.faxArea,
	InvolvedParty_T.faxTel,
	InvolvedParty_T.homeArea,
	InvolvedParty_T.homeTel,
	InvolvedParty_T.pagerArea,
	InvolvedParty_T.pagerTel,
	InvolvedParty_T.otherArea,
	InvolvedParty_T.otherTel,
	/*isActive*/
	InvolvedParty_T.dateInserted,
	InvolvedParty_T.isoClaimId,
	InvolvedParty_T.involvedPartySequenceId
	FROM
		dbo.InvolvedParty_T
		--INNER JOIN dbo.FM_ExtractFile
		--	ON InvolvedParty_T.isoClaimId = FM_ExtractFile.I_ALLCLM
	WHERE
		InvolvedParty_T.isActive = 1
		AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty_T.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveInvolvedPartyT_involvedPartyId
	ON dbo.V_ActiveInvolvedParty_T (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMAliaseInvolvedParty_T
WITH SCHEMABINDING
AS
SELECT
	InvolvedParty_T.involvedPartyId,
	isAliasOfInvolvedPartyId,
	/*isServiceProviderOfInvolvedPartyId*/
	InvolvedParty_T.isBusiness,
	InvolvedParty_T.involvedPartyRoleCode,
	InvolvedParty_T.taxIdentificationNumberObfuscated,
	InvolvedParty_T.taxIdentificationNumberLastFour,
	InvolvedParty_T.socialSecurityNumberObfuscated,
	InvolvedParty_T.socialSecurityNumberLastFour,
	InvolvedParty_T.hICNObfuscated,
	InvolvedParty_T.driversLicenseNumberObfuscated,
	InvolvedParty_T.driversLicenseNumberLast3,
	InvolvedParty_T.driversLicenseClass,
	InvolvedParty_T.driversLicenseState,
	InvolvedParty_T.genderCode,
	InvolvedParty_T.passportID,
	InvolvedParty_T.professionalMedicalLicense,
	InvolvedParty_T.isUnderSiuInvestigation,
	InvolvedParty_T.isLawEnforcementAction,
	InvolvedParty_T.isReportedToFraudBureau,
	InvolvedParty_T.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty_T.isMedicareEligible, */
	InvolvedParty_T.dateOfBirth,
	InvolvedParty_T.fullName,
	InvolvedParty_T.firstName,
	InvolvedParty_T.middleName,
	InvolvedParty_T.lastName,
	InvolvedParty_T.suffix,
	InvolvedParty_T.businessArea,
	InvolvedParty_T.businessTel,
	InvolvedParty_T.cellArea,
	InvolvedParty_T.cellTel,
	InvolvedParty_T.faxArea,
	InvolvedParty_T.faxTel,
	InvolvedParty_T.homeArea,
	InvolvedParty_T.homeTel,
	InvolvedParty_T.pagerArea,
	InvolvedParty_T.pagerTel,
	InvolvedParty_T.otherArea,
	InvolvedParty_T.otherTel,
	/*isActive*/
	InvolvedParty_T.dateInserted,
	InvolvedParty_T.isoClaimId,
	InvolvedParty_T.involvedPartySequenceId
	FROM
		dbo.InvolvedParty_T
		--INNER JOIN dbo.FM_ExtractFile
		--	ON InvolvedParty_T.isoClaimId = FM_ExtractFile.I_ALLCLM
	WHERE
		InvolvedParty_T.isActive = 1
		AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty_T.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMAliaseInvolvedPartyT_involvedPartyId
	ON dbo.V_ActiveFMAliaseInvolvedParty_T (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMNonAliaseServiceProvider_T
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty_T.involvedPartyId,
	/*isAliasOfInvolvedPartyId*/
	InvolvedParty_T.isServiceProviderOfInvolvedPartyId,
	InvolvedParty_T.isBusiness,
	InvolvedParty_T.involvedPartyRoleCode,
	InvolvedParty_T.taxIdentificationNumberObfuscated,
	InvolvedParty_T.taxIdentificationNumberLastFour,
	InvolvedParty_T.socialSecurityNumberObfuscated,
	InvolvedParty_T.socialSecurityNumberLastFour,
	InvolvedParty_T.hICNObfuscated,
	InvolvedParty_T.driversLicenseNumberObfuscated,
	InvolvedParty_T.driversLicenseNumberLast3,
	InvolvedParty_T.driversLicenseClass,
	InvolvedParty_T.driversLicenseState,
	InvolvedParty_T.genderCode,
	InvolvedParty_T.passportID,
	InvolvedParty_T.professionalMedicalLicense,
	InvolvedParty_T.isUnderSiuInvestigation,
	InvolvedParty_T.isLawEnforcementAction,
	InvolvedParty_T.isReportedToFraudBureau,
	InvolvedParty_T.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty_T.isMedicareEligible, */
	InvolvedParty_T.dateOfBirth,
	InvolvedParty_T.fullName,
	InvolvedParty_T.firstName,
	InvolvedParty_T.middleName,
	InvolvedParty_T.lastName,
	InvolvedParty_T.suffix,
	InvolvedParty_T.businessArea,
	InvolvedParty_T.businessTel,
	InvolvedParty_T.cellArea,
	InvolvedParty_T.cellTel,
	InvolvedParty_T.faxArea,
	InvolvedParty_T.faxTel,
	InvolvedParty_T.homeArea,
	InvolvedParty_T.homeTel,
	InvolvedParty_T.pagerArea,
	InvolvedParty_T.pagerTel,
	InvolvedParty_T.otherArea,
	InvolvedParty_T.otherTel,
	/*isActive*/
	InvolvedParty_T.dateInserted,
	InvolvedParty_T.isoClaimId,
	InvolvedParty_T.involvedPartySequenceId
	FROM
		dbo.InvolvedParty_T
		--INNER JOIN dbo.FM_ExtractFile
		--	ON InvolvedParty_T.isoClaimId = FM_ExtractFile.I_ALLCLM
	WHERE
		InvolvedParty_T.isActive = 1
		AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty_T.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMNonAliaseServiceProviderT_involvedPartyId
	ON dbo.V_ActiveFMNonAliaseServiceProvider_T (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMAliaseServiceProvider_T
WITH SCHEMABINDING
AS
	SELECT
	InvolvedParty_T.involvedPartyId,
	InvolvedParty_T.isAliasOfInvolvedPartyId,
	InvolvedParty_T.isServiceProviderOfInvolvedPartyId,
	InvolvedParty_T.isBusiness,
	InvolvedParty_T.involvedPartyRoleCode,
	InvolvedParty_T.taxIdentificationNumberObfuscated,
	InvolvedParty_T.taxIdentificationNumberLastFour,
	InvolvedParty_T.socialSecurityNumberObfuscated,
	InvolvedParty_T.socialSecurityNumberLastFour,
	InvolvedParty_T.hICNObfuscated,
	InvolvedParty_T.driversLicenseNumberObfuscated,
	InvolvedParty_T.driversLicenseNumberLast3,
	InvolvedParty_T.driversLicenseClass,
	InvolvedParty_T.driversLicenseState,
	InvolvedParty_T.genderCode,
	InvolvedParty_T.passportID,
	InvolvedParty_T.professionalMedicalLicense,
	InvolvedParty_T.isUnderSiuInvestigation,
	InvolvedParty_T.isLawEnforcementAction,
	InvolvedParty_T.isReportedToFraudBureau,
	InvolvedParty_T.isFraudReported,
	/*Deprecated 20190130 RDW*//*InvolvedParty_T.isMedicareEligible, */
	InvolvedParty_T.dateOfBirth,
	InvolvedParty_T.fullName,
	InvolvedParty_T.firstName,
	InvolvedParty_T.middleName,
	InvolvedParty_T.lastName,
	InvolvedParty_T.suffix,
	InvolvedParty_T.businessArea,
	InvolvedParty_T.businessTel,
	InvolvedParty_T.cellArea,
	InvolvedParty_T.cellTel,
	InvolvedParty_T.faxArea,
	InvolvedParty_T.faxTel,
	InvolvedParty_T.homeArea,
	InvolvedParty_T.homeTel,
	InvolvedParty_T.pagerArea,
	InvolvedParty_T.pagerTel,
	InvolvedParty_T.otherArea,
	InvolvedParty_T.otherTel,
	/*isActive*/
	InvolvedParty_T.dateInserted,
	InvolvedParty_T.isoClaimId,
	InvolvedParty_T.involvedPartySequenceId
	FROM
		dbo.InvolvedParty_T
		--INNER JOIN dbo.FM_ExtractFile
		--	ON InvolvedParty_T.isoClaimId = FM_ExtractFile.I_ALLCLM
	WHERE
		InvolvedParty_T.isActive = 1
		AND InvolvedParty_T.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty_T.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMAliaseServiceProviderT_involvedPartyId
	ON dbo.V_ActiveFMAliaseServiceProvider_T (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.InvolvedParty_TActivityLog
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
	CONSTRAINT PK_InvolvedPartyActivityLogT_involvedPartyActivityLogId
		PRIMARY KEY CLUSTERED (involvedPartyActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyActivityLogT_executionDateTime
	ON dbo.InvolvedParty_TActivityLog (executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC sp_help 'dbo.InvolvedParty_T'
--EXEC sp_help 'dbo.V_ActiveFMNonAliaseInvolvedParty'
--EXEC sp_help 'dbo.V_ActiveFMAliaseInvolvedParty_T'
--EXEC sp_help 'dbo.V_ActiveFMNonAliaseServiceProvider_T'
--EXEC sp_help 'dbo.V_ActiveFMAliaseServiceProvider_T'
--EXEC sp_help 'dbo.InvolvedParty_TActivityLog' 

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;

/*
COMMIT
20190130

		INNER JOIN dbo.FM_ExtractFile
			ON InvolvedParty_T.isoClaimId = FM_ExtractFile.I_ALLCLM
*/