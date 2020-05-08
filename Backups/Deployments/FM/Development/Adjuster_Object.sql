SET NOEXEC OFF;
/*
	todo:
		analyze which county to import
*/
--USE ClaimSearch_Dev

USE ClaimSearch_Dev

BEGIN TRANSACTION

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
Author: Daniel Ravaglia
Description: Adjuster Object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Adjuster
(   
	adjusterId BIGINT IDENTITY (1,1) NOT NULL,
	adjusterPartyId BIGINT IDENTITY(1,1) NOT NULL,
	adjusterPartySequenceId TINYINT NULL, /*N_ADJ_SEQ*/  
	adjusterCompanyCode CHAR(4) NOT NULL, /*I_CUST*/
	adjusterOfficeCode CHAR(5) NOT NULL, /*I_REGOFF*/ 
	adjusterDateSubmitted date NULL, /*D_ADJ_SUBM*/
	adjusterName VARCHAR(70) NULL, /*M_FUL_NM*/ 
    adjusterAreaCode SMALLINT NULL, /*N_AREA_WK_SIU*/ 	
    adjusterPhoneNumber INT NULL, /*N_TEL_WK_SIU*/
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,	
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	CONSTRAINT Adjsuster_adjusterId
		PRIMARY KEY CLUSTERED (adjusterId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_Adjuster
WITH SCHEMABINDING
AS
(
	SELECT
	adjusterId,
	adjusterPartyId,
	adjusterPartySequenceId,
	adjusterCompanyCode,
	adjusterOfficeCode,
	adjusterDateSubmitted,
	adjusterName, 
    adjusterAreaCode,
    adjusterPhoneNumber,	
	isoClaimId
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
CREATE UNIQUE CLUSTERED INDEX PK_V_isoClaimId
	ON dbo.V_Adjuster (isoClaimId);

IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END

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
	stepExecutionNotes VARCHAR(1000) NULL
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC sp_help 'dbo.Adjuster'
PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
--PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/