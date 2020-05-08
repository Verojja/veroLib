SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

/*
	DROP VIEW dbo.V_ActiveAdjuster
	DROP TABLE dbo.AdjusterActivityLog
	DROP TABLE dbo.Adjuster
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
Description: Generic Adjuster Object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Adjuster
(   
	adjusterId BIGINT IDENTITY (1,1) NOT NULL,
	adjusterCompanyCode CHAR(4) NOT NULL, /*I_CUST*/
	adjusterOfficeCode CHAR(5) NOT NULL, /*I_REGOFF*/ 
	adjusterDateSubmitted date NULL, /*D_ADJ_SUBM*/
	adjusterName VARCHAR(70) NULL, /*M_FUL_NM*/ 
    adjusterAreaCode SMALLINT NULL, /*N_AREA_WK_SIU*/ 	
    adjusterPhoneNumber INT NULL, /*N_TEL_WK_SIU*/
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,	
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	adjusterSequenceId SMALLINT NULL /*N_ADJ_SEQ*/
	CONSTRAINT PK_Adjsuster_adjusterId
		PRIMARY KEY CLUSTERED (adjusterId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Adjuster_isoClaimId_involvedPartySequenceId_adjusterSequenceId
	ON dbo.Adjuster (isoClaimId, involvedPartySequenceId, adjusterSequenceId)
	INCLUDE (adjusterCompanyCode, adjusterOfficeCode, adjusterDateSubmitted, adjusterName, adjusterAreaCode, adjusterPhoneNumber, isActive, dateInserted);
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
	INCLUDE (adjusterCompanyCode, adjusterOfficeCode, adjusterDateSubmitted, adjusterName, adjusterAreaCode, adjusterPhoneNumber, dateInserted);	
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
Description: Logging table for Insurance Adjuster(s)
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.AdjusterActivityLog
(
	adjusterActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_AdjusterActivityLog_adjusterActivityLogId
		PRIMARY KEY CLUSTERED (adjusterActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_AdjusterActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.AdjusterActivityLog (isSuccessful, stepId, executionDateTime);
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