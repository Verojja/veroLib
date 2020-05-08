SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

/*
	DROP VIEW dbo.V_ActiveIPAddressMap
	DROP TABLE dbo.IPAddressMapActivityLog
	DROP TABLE dbo.InvolvedPartyAddressMap
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
Description: Logging table for InvolvedPartyAddressMap
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.IPAddressMapActivityLog
(
	iPAddressMapActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_IPAddressMapActivityLog_iPAddressMapActivityLogId
		PRIMARY KEY CLUSTERED (iPAddressMapActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_IPAddressMapActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.IPAddressMapActivityLog (isSuccessful, stepId, executionDateTime);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC sp_help 'dbo.Adjuster'
--EXEC sp_help 'dbo.V_ActiveAdjuster'
--EXEC sp_help 'dbo.AdjusterActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/