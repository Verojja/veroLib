SET NOEXEC OFF;

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

BEGIN TRANSACTION;

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2755
Date: 2019-06-20
Author: Robert David Warner
Description: Logging table for DecisionNet Product.
				
			Performance: No current notes.
************************************************/
CREATE TABLE DecisionNet.ProductActivityLog
(
	productActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_ProductActivityLog_productActivityLogId
		PRIMARY KEY CLUSTERED (productActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ProductActivityLog_isSuccessful_stepId_executionDateTime
	ON DecisionNet.ProductActivityLog (isSuccessful, stepId, executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2755
Date: 2019-06-20
Author: Robert David Warner
Description: Logging table for DecisionNet Product.
				
			Performance: No current notes.
************************************************/
CREATE TABLE DecisionNet.ExpenditureActivityLog
(
	expenditureActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_ExpenditureActivityLog_expenditureActivityLogId
		PRIMARY KEY CLUSTERED (expenditureActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ExpenditureActivityLog_isSuccessful_stepId_executionDateTime
	ON DecisionNet.ExpenditureActivityLog (isSuccessful, stepId, executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2755
Date: 2019-06-20
Author: Robert David Warner
Description: Logging table for DecisionNet Product.
				
			Performance: No current notes.
************************************************/
CREATE TABLE DecisionNet.ClaimReferenceActivityLog
(
	claimReferenceActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_ClaimReferenceActivityLog_claimReferenceActivityLogId
		PRIMARY KEY CLUSTERED (claimReferenceActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ClaimReferenceActivityLog_isSuccessful_stepId_executionDateTime
	ON DecisionNet.ClaimReferenceActivityLog (isSuccessful, stepId, executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2755
Date: 2019-06-20
Author: Robert David Warner
Description: Logging table for DecisionNet Product.
				
			Performance: No current notes.
************************************************/
CREATE TABLE DecisionNet.ClaimReferenceExtractActivityLog
(
	cRExtractActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_CRExtractActivityLog_cRExtractActivityLogId
		PRIMARY KEY CLUSTERED (cRExtractActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_CRExtractActivityLog_isSuccessful_stepId_executionDateTime
	ON DecisionNet.ClaimReferenceExtractActivityLog (isSuccessful, stepId, executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT'COMMIT';COMMIT TRANSACTION;