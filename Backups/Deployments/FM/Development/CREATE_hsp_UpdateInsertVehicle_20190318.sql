SET NOEXEC OFF;
USE ClaimSearch_Dev
/*
TODO:
	1.) find&repalce "VehicleActivityLog" with current object activity log table.
	2.) Look for the /*Update this name*/ tags on the left side of the screen.
*/

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: 
Date: 20190220
Author: Dan Ravaglia and Robert David Warner
Description: Mechanism for data-refresh of the Adjuster Table.
			
			Performance: 34.56% performance improvement using windowing function
						to exclude duplication vs. using Distinct.

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertVehicle
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.VehicleActivityLog
				WHERE
					VehicleActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND VehicleActivityLog.isSuccessful = 1
					AND DATEDIFF(HOUR,GETDATE(),VehicleActivityLog.executionDateTime) < 12
			)
		)
		BEGIN
			DECLARE @internalTransactionCount TINYINT = 0;
			IF (@@TRANCOUNT = 0)
			BEGIN
				BEGIN TRANSACTION;
				SET @internalTransactionCount = 1;
			END
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(VehicleActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.VehicleActivityLog
			WHERE
				VehicleActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND VehicleActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureVehicleDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingVehicle.vehicleId,
				UniqueVehicleData.vinNumber,
				UniqueVehicleData.vehicleYear,
				UniqueVehicleData.vehicleMake,
				UniqueVehicleData.vehicleModelNumber,
				UniqueVehicleData.licensePlateNumber,
				UniqueVehicleData.vehicleStyleCode,
				UniqueVehicleData.vehicleTypeCode,
				UniqueVehicleData.vehicleColor,
				UniqueVehicleData.vehicleStateCode,
				UniqueVehicleData.isoClaimId,
				UniqueVehicleData.involvedPartySequenceId
				INTO #VehicleData
			FROM
				(
					SELECT
						CLT00061.I_ALLCLM AS isoClaimId,
						CLT00061.I_NM_ADR AS involvedPartySequenceId,
						CLT00061.N_VIN AS vinNumber,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00061.I_ALLCLM,
								CLT00061.I_NM_ADR,
								CLT00061.N_VIN
							ORDER BY
								CLT00061.Date_Insert DESC
						) AS uniqueInstanceValue,
						CAST(COALESCE(NULLIF(LTRIM(RTRIM(CLT00061.D_VEH_YR_CRR)),''), NULLIF(LTRIM(RTRIM(CLT00061.D_VEH_YR)),'')) AS SMALLINT) AS vehicleYear,
						COALESCE(NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_MK_CRR)),''), NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_MK)),'')) AS vehicleMake,
						COALESCE(NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_MODL_CRR)),''), NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_MODL_CRR)),'')) AS vehicleModelNumber,
						NULLIF(LTRIM(RTRIM(CLT00061.N_LIC_PLT)),'') AS licensePlateNumber,
						NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_STY)),'') AS vehicleStyleCode,
						NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_TYP)),'') AS vehicleTypeCode,
						NULLIF(LTRIM(RTRIM(CLT00061.C_VEH_CLR)),'') AS vehicleColor,
						NULLIF(LTRIM(RTRIM(CLT00061.C_ST_ALPH)),'') AS vehicleStateCode
					FROM
						ClaimSearch_Prod.dbo.CLT00061
						INNER JOIN dbo.FireMarshalDriver
							ON CLT00061.I_ALLCLM = FireMarshalDriver.isoClaimId
					WHERE
						CLT00061.Date_Insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
						/*NOTE: THIS following line of code will\would nuke any potential performance on the column (if it were indexed)
							but it can not be avoided since the source system allows blank values into this field.
						 */
						AND NULLIF(LTRIM(RTRIM(CLT00061.N_VIN)),'') IS NOT NULL
				) AS UniqueVehicleData
				LEFT OUTER JOIN dbo.Vehicle AS ExistingVehicle
					ON UniqueVehicleData.isoClaimId = ExistingVehicle.isoClaimId
						AND UniqueVehicleData.involvedPartySequenceId = ExistingVehicle.involvedPartySequenceId
						AND UniqueVehicleData.vinNumber = ExistingVehicle.vinNumber
			WHERE
				UniqueVehicleData.uniqueInstanceValue = 1;
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.VehicleActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdateExistingVehicleData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Vehicle
				SET
					/*Vehicle.vehicleId = SOURCE.vehicleId,*/
					Vehicle.vinNumber = SOURCE.vinNumber,
					Vehicle.vehicleYear = SOURCE.vehicleYear,
					Vehicle.vehicleMake = SOURCE.vehicleMake,
					Vehicle.vehicleModelNumber = SOURCE.vehicleModelNumber,
					Vehicle.licensePlateNumber = SOURCE.licensePlateNumber,
					Vehicle.vehicleStyleCode = SOURCE.vehicleStyleCode,
					Vehicle.vehicleTypeCode = SOURCE.vehicleTypeCode,
					Vehicle.vehicleColor = SOURCE.vehicleColor,
					Vehicle.vehicleStateCode = SOURCE.vehicleStateCode,
					/*Vehicle.isActive	= SOURCE.isActive,*/
					Vehicle.dateInserted = @dateInserted
					/*
					Vehicle.isoClaimId = SOURCE.isoClaimId,
					Vehicle.involvedPartySequenceId = SOURCE.involvedPartySequenceId
					*/
			FROM
				#VehicleData AS SOURCE
			WHERE
				SOURCE.vehicleId = Vehicle.vehicleId
				AND
				(
				/*
					ISNULL(Vehicle.vehicleId,'') <> ISNULL(SOURCE.vehicleId,'')*/
					Vehicle.vinNumber <> SOURCE.vinNumber
					OR ISNULL(Vehicle.vehicleYear,0) <> ISNULL(SOURCE.vehicleYear,0)
					OR ISNULL(Vehicle.vehicleMake,'') <> ISNULL(SOURCE.vehicleMake,'')
					OR ISNULL(Vehicle.vehicleModelNumber,'') <> ISNULL(SOURCE.vehicleModelNumber,'')
					OR ISNULL(Vehicle.licensePlateNumber,'') <> ISNULL(SOURCE.licensePlateNumber,'')
					OR ISNULL(Vehicle.vehicleStyleCode,'') <> ISNULL(SOURCE.vehicleStyleCode,'')
					OR ISNULL(Vehicle.vehicleTypeCode,'') <> ISNULL(SOURCE.vehicleTypeCode,'')
					OR ISNULL(Vehicle.vehicleColor,'') <> ISNULL(SOURCE.vehicleColor,'')
					OR ISNULL(Vehicle.vehicleStateCode,'') <> ISNULL(SOURCE.vehicleStateCode,'')
					/*
						OR ISNULL(Vehicle.isActive,'') <> ISNULL(SOURCE.isActive,'')
						OR ISNULL(Vehicle.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
						OR ISNULL(Vehicle.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
						OR ISNULL(Vehicle.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')
					*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.VehicleActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'InsertNewFMDriverData', /*Update this note*/
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Vehicle WITH (TABLOCKX)
			(
				/*vehicleId*/
				vinNumber,
				vehicleYear,
				vehicleMake,
				vehicleModelNumber,
				licensePlateNumber,
				vehicleStyleCode,
				vehicleTypeCode,
				vehicleColor,
				vehicleStateCode,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				/*SOURCE.vehicleId,*/
				SOURCE.vinNumber,
				SOURCE.vehicleYear,
				SOURCE.vehicleMake,
				SOURCE.vehicleModelNumber,
				SOURCE.licensePlateNumber,
				SOURCE.vehicleStyleCode,
				SOURCE.vehicleTypeCode,
				SOURCE.vehicleColor,
				SOURCE.vehicleStateCode,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#VehicleData AS SOURCE
			WHERE
				SOURCE.vehicleId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.VehicleActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();
		/*Log Activity*/
		INSERT INTO dbo.VehicleActivityLog
		(
			productCode,
			sourceDateTime,
			executionDateTime,
			stepId,
			stepDescription,
			stepStartDateTime,
			stepEndDateTime,
			recordsAffected,
			isSuccessful,
			stepExecutionNotes
		)
		SELECT
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertVehicle; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

--PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
	
*/