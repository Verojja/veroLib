SET NOEXEC OFF;

USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	DROP VIEW dbo.V_ActiveFMVehicle
	--DROP TABLE dbo.VehicleActivityLog
	DROP TABLE dbo.Vehicle
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
Author: Robert David Warner
Description: Vehicle Object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Vehicle
(
	vehicleId BIGINT IDENTITY (1,1) NOT NULL,

	vinNumber VARCHAR(20) NOT NULL, /*N_VIN*/

	vehicleYear SMALLINT NULL, /*D_VEH_YR_CRR, D_VEH_YR*/ 
	vehicleMake VARCHAR(4) NULL, /*C_VEH_MK_CRR, C_VEH_MK*/
	vehicleModelNumber  VARCHAR(3) NULL, /*C_VEH_MODL_CRR, C_VEH_MODL*/
	licensePlateNumber VARCHAR(10) NULL, /*could probably use VARCHAR(8) N_LIC_PLT*/

	vehicleStyleCode VARCHAR(2) NULL, /*C_VEH_STY*/
	vehicleTypeCode VARCHAR(2) NULL, /*C_VEH_TYP*/
	vehicleColor VARCHAR(50) NULL /*C_VEH_CLR*/,

	vehicleStateCode CHAR(2) NULL, /*C_ST_ALPH*/

	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,	
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL /*I_NM_ADR*/
	CONSTRAINT PK_Vehicle_vehicleId
		PRIMARY KEY CLUSTERED (vehicleId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Vehicle_isoClaimId_involvedPartySequenceId
	ON dbo.Vehicle (isoClaimId, involvedPartySequenceId)
	INCLUDE (vehicleYear, vehicleMake, vehicleModelNumber, licensePlateNumber, vehicleStyleCode, vehicleTypeCode, vehicleColor, vehicleStateCode);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMVehicle
WITH SCHEMABINDING
AS
(
	SELECT
		Vehicle.vehicleId,
		Vehicle.vinNumber,
		Vehicle.vehicleYear,
		Vehicle.vehicleMake,
		Vehicle.vehicleModelNumber,
		Vehicle.licensePlateNumber,
		Vehicle.vehicleStyleCode,
		Vehicle.vehicleTypeCode,
		Vehicle.vehicleColor,
		Vehicle.vehicleStateCode,
		Vehicle.dateInserted,
		Vehicle.isoClaimId,
		Vehicle.involvedPartySequenceId
	FROM
		dbo.Vehicle
		INNER JOIN dbo.FireMarshalDriver
			ON Vehicle.isoClaimId = FireMarshalDriver.isoClaimId
	WHERE
		Vehicle.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMVehicle_vehicleId
	ON dbo.V_ActiveFMVehicle (vehicleId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--CREATE TABLE dbo.VehicleActivityLog
--(
--	VehicleActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
--	productCode VARCHAR(50) NULL,
--	sourceDateTime DATETIME2(0) NOT NULL,
--	executionDateTime DATETIME2(0) NOT NULL,
--	stepId TINYINT NOT NULL,
--	stepDescription VARCHAR(1000) NULL,
--	stepStartDateTime DATETIME2(0) NULL,
--	stepEndDateTime DATETIME2(0) NULL,
--	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
--	recordsAffected BIGINT NULL,
--	isSuccessful BIT NOT NULL,
--	stepExecutionNotes VARCHAR(1000) NULL
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_VehicleActivityLog_executionDateTime
--	ON dbo.VehicleActivityLog (executionDateTime)
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
EXEC sp_help 'dbo.Vehicle'
--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/