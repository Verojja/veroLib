SET NOEXEC OFF;

--USE ClaimSearch_Dev

BEGIN TRANSACTION

/*
	/*DROP VIEW dbo.V_ActiveFireMarshalDriver*/
	DROP TABLE dbo.FireMarshalPerspectiveType
	DROP TABLE dbo.FireMarshalDriverActivityLog
	DROP TABLE dbo.FireMarshalDriver
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
Date: 2019-03-01
Author: Daniel Ravaglia and Robert David Warner
Description: Fire Marshal Perspective Type Object, used in the FM Driver and Export process.
Argument could be made for this object to live in a [FireMarshal] schema.

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalPerspectiveType
(   
	fmPerspectiveTypeId TINYINT NOT NULL,
	perspectiveName VARCHAR(35) NOT NULL,
	perspectiveDescription VARCHAR(250) NULL,
	dateInserted DATETIME2(0) NOT NULL,
	CONSTRAINT PK_FMPerspectiveType_fmPerspectiveTypeId
		PRIMARY KEY CLUSTERED (fmPerspectiveTypeId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
DECLARE @insertDate DATETIME2(0) = GETDATE();
INSERT INTO dbo.FireMarshalPerspectiveType
(
	fmPerspectiveTypeId,
	perspectiveName,
	perspectiveDescription,
	dateInserted
)
SELECT
	InitializationSet.perspectiveTypeId,
	InitializationSet.fmPerspectiveTypeName,
	NULL AS fmPerspectiveTypeDescription,
	@insertDate AS dateInserted
FROM
	(
		VALUES
		(1, 'Basic'),
		(2, 'Basic + Lighting/Fire'),
		(3, 'Basic + Auto'		),
		(4, 'Basic + Auto + Lighting/Fire'),
		(5, 'Lighting/Fire only'),
		(6, 'Auto Fire only')
	) AS InitializationSet (perspectiveTypeId, fmPerspectiveTypeName);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-03-01
Author: Daniel Ravaglia and Robert David Warner
Description: Fire Marshal Driver Object. Currently dependent on isoClaimId;
				This table is TRUNC & LOAD, and will need to be altered
				  if we ever migrate away from using isoClaimID.
************************************************/
CREATE TABLE dbo.FireMarshalDriver
(   
	isoClaimId VARCHAR(11) NOT NULL /*I_ALLCLM*/,
	/*
		isLegacyFire BIT NOT NULL, /*FLAG_LEG_FIRE*/
		isUFFire BIT NOT NULL, /*FLAG_UF_FIRE*/
		isUFLightExplosion BIT NOT NULL, /*FLAG_UF_LightExpl*/
		isUFAuto BIT NOT NULL, /*FLAG_UF_AUTO*/
	*/
	includesValidLossDescription BIT NOT NULL, /*FLAG_VALID_LOSS_DESC*/
	includesValidLossDescriptionLength BIT NOT NULL, /*FLAG_VALID_LOSS_DESC_LENGTH*/
	includesValidLossEstimate BIT NOT NULL, /*FLAG_VALID_LOSS_ESTIMATE*/
	includesValidLossSettlement BIT NOT NULL, /*FLAG_VALID_LOSS_SETTLEMENT*/
	/*
		lossEstimateAmount DECIMAL(19,2) NOT NULL, /*A_EST_LOSS*/
		settlementAmount DECIMAL(19,2) NOT NULL,/*A_STTLMT*/
	*/
	isActive BIT NOT NULL, 
	fmPerspectiveTypeId TINYINT NULL,
	fmStateStatusCode CHAR(1) NULL,
		/*A - Active (recieves FM in some physical explicit "file" format),
		 I - Inactive (not currently subscribed to any FM data),
		 P - Passive (only subscribed to Dashboard FM data)
		*/
	dateInserted DATETIME2(0) NOT NULL,
	CONSTRAINT PK_FireMarshalDriver_isoClaimId
		PRIMARY KEY CLUSTERED (isoClaimId) WITH (FILLFACTOR=80)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-03-01
Author: Robert David Warner
Description: Logging table for Fire Marshal Driver Object.
************************************************/
CREATE TABLE dbo.FireMarshalDriverActivityLog
(
	fireMarshalDriverActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
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
	CONSTRAINT PK_FMDriverActivityLog_FMDriverActivityLogId
		PRIMARY KEY CLUSTERED (fireMarshalDriverActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_FMDriverActivityLog_isSuccessful_stepId_executionDateTime
	ON dbo.FireMarshalDriverActivityLog (isSuccessful, stepId, executionDateTime);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC sp_help 'dbo.FireMarshalPerspectiveType'
--EXEC sp_help 'dbo.FireMarshalDriver'
--EXEC sp_help 'dbo.FireMarshalDriverActivityLog'
--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/