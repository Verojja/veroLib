SET NOEXEC OFF;

BEGIN TRANSACTION

USE ClaimSearch_Prod;

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
CREATE TABLE dbo.FireMarshalExtractActivityLog
(
	fireMarshalExtractActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_FireMarshalExtract_fireMarshalExtractActivityLogId
		PRIMARY KEY CLUSTERED (fireMarshalExtractActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_FireMarshalExtractActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.FireMarshalExtractActivityLog (isSuccessful, stepId, executionDateTime);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
PRINT 'COMMIT';COMMIT TRANSACTION;