SET NOEXEC OFF;
/*
	todo:
		analyze which county to import
*/
USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	DROP VIEW dbo.V_ActiveClaim
	DROP TABLE dbo.Claim
	DROP TABLE dbo.ClaimActivityLog
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
Description: Generic Claim object. The relationship between
				Claim and Policy is N:1; however this is
				difficult to capture while entitiy resolution is
				still being implemented.
				
			Performance: No current notes.

************************************************/
CREATE TABLE dbo.Claim
(
	claimId BIGINT IDENTITY (1,1) NOT NULL,
	originalClaimNumber VARCHAR(30) NULL, /*N_CLM*/
	locationOfLossAddressId BIGINT NOT NULL,
	policyId BIGINT NULL,
	claimSearchSourceSystem CHAR(1) NULL, /*C_CLM_SRCE*/
	claimEntryMethod CHAR(1) NULL, /*C_RPT_SRCE*/
	
	isVoidedByInsuranceCarrier BIT NOT NULL, /*F_VOID*/
		/*isUpdatedViaWeb BIT NULL, /*F_UPD*/*/
	
	lossDescription VARCHAR(50) NULL, /*T_LOSS_DSC*/
	lossDescriptionExtended VARCHAR(200) NULL, /*T_LOSS_DSC_EXT*/
	
	/*Deprecated:*//*catastropheId VARCHAR(4) NULL, /*C_CAT CLT1A*/*/
	isClaimSearchProperty BIT NULL, /*F_PROP*/
	isClaimSearchAuto BIT NULL, /*F_AUTO*/
	isClaimSearchCasualty BIT NULL, /*F_CSLTY*/
	isClaimSearchAPD BIT NULL, /*F_APD*/
	
	dateOfLoss DATETIME2(0) NULL, /*D_OCUR*/
	insuranceCompanyReceivedDate DATETIME2(0) NULL, /*D_INS_CO_RCV*/
	systemDateReceived DATETIME2(0) NULL, /*D_RCV*/

	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	isoClaimId VARCHAR(11) NULL /*I_ALLCLM*/
	
	CONSTRAINT PK_Claim_claimId
		PRIMARY KEY CLUSTERED (claimId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Claim_isoClaimId
	ON dbo.Claim (isoClaimId)
	INCLUDE (originalClaimNumber, policyId, claimSearchSourceSystem, claimEntryMethod, isVoidedByInsuranceCarrier, lossDescription, lossDescriptionExtended, isClaimSearchProperty, isClaimSearchAuto, isClaimSearchCasualty, isClaimSearchAPD, dateOfLoss, insuranceCompanyReceivedDate, systemDateReceived, isActive, dateInserted)
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
	INCLUDE (originalClaimNumber, policyId, claimSearchSourceSystem, claimEntryMethod, isVoidedByInsuranceCarrier, lossDescription, lossDescriptionExtended, isClaimSearchProperty, isClaimSearchAuto, isClaimSearchCasualty, isClaimSearchAPD, dateOfLoss, insuranceCompanyReceivedDate, systemDateReceived, dateInserted)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--/*
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Logging table for Insurance Claim object.
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.ClaimActivityLog
(
	claimActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_ClaimActivityLog_claimActivityLogId
		PRIMARY KEY CLUSTERED (claimActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ClaimActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.ClaimActivityLog (isSuccessful, stepId, executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--*/


--EXEC sp_help 'dbo.Claim'
--EXEC sp_help 'dbo.V_ActiveClaim'
--EXEC sp_help 'dbo.ClaimActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*
*/