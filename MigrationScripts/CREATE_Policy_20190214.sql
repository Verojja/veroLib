SET NOEXEC OFF;
/*
	todo:
*/
--USE ClaimSearch_Dev

BEGIN TRANSACTION


/*
	DROP VIEW dbo.V_ActiveFMPolicy
	DROP TABLE dbo.PolicyActivityLog
	DROP TABLE dbo.Policy
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
Date: 2019-01-24
Author: Daniel Ravaglia and Robert David Warner
Description: Adjuster Object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Policy
(
	policyId BIGINT IDENTITY (1,1) NOT NULL,
	insuranceProviderCompanyCode CHAR(4) NOT NULL, /*I_CUST*/
	insuranceProviderOfficeCode CHAR(5) NOT NULL, /*I_REGOFF*/ 
	originalPolicyNumber VARCHAR(30) NOT NULL,  /*N_POL*/
	policyTypeCode CHAR(4) NOT NULL, /*C_POL_TYP*/
	originalPolicyInceptionDate DATE NULL, /*D_POL_INCP*/
	originalPolicyExperiationDate DATE NULL, /*D_POL_EXPIR*/
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,	
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	CONSTRAINT PK_Policy_policyId
		PRIMARY KEY CLUSTERED (policyId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMPolicy
WITH SCHEMABINDING
AS
(
	SELECT
		Policy.policyId,
		Policy.insuranceProviderCompanyCode,
		Policy.insuranceProviderOfficeCode,
		Policy.originalPolicyNumber,
		Policy.policyTypeCode,
		Policy.originalPolicyInceptionDate,
		Policy.originalPolicyExperiationDate,
		Policy.dateInserted,
		Policy.isoClaimId
	FROM
		dbo.Policy
		INNER JOIN dbo.FM_ExtractFile
			ON Policy.isoClaimId = FM_ExtractFile.I_ALLCLM 
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMPolicy_policyId
	ON dbo.V_ActiveFMPolicy (policyId);
CREATE NONCLUSTERED INDEX NIX_ActiveFMPolicy_isoClaimId
	ON dbo.V_ActiveFMPolicy (isoClaimId);	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.PolicyActivityLog
(
	policyActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_PolicyActivityLog_policyActivityLogId
		PRIMARY KEY CLUSTERED (policyActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_PolicyActivityLog_executionDateTime
	ON dbo.PolicyActivityLog (executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC sp_help 'dbo.Policy'
--EXEC sp_help 'dbo.V_ActiveFMPolicy'
--EXEC sp_help 'dbo.PolicyActivityLog'


--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*
COMMIT
201902014
*/