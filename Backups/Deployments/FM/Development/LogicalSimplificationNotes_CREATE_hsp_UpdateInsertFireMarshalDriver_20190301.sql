SET NOEXEC OFF;

/*
TODO:
	1.) find&repalce "FireMarshalDriverActivityLog" with current object activity log table.
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
CREATE PROCEDURE dbo.hsp_UpdateInsertFireMarshalDriver
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
				FROM dbo.FireMarshalDriverActivityLog
				WHERE
					FireMarshalDriverActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FireMarshalDriverActivityLog.isSuccessful = 1
					AND DATEDIFF(HOUR,GETDATE(),FireMarshalDriverActivityLog.executionDateTime) < 12
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
						MAX(FireMarshalDriverActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.FireMarshalDriverActivityLog
			WHERE
				FireMarshalDriverActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND FireMarshalDriverActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureFMClaimDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				I_ALLCLM AS isoClaimId,
				/*
					FLAG_LEG_FIRE AS isLegacyFire,
					FLAG_UF_FIRE AS isUFFire,
					FLAG_UF_LightExpl AS isUFLightExplosion,
					FLAG_UF_AUTO AS isUFAuto,
				*/
				FLAG_VALID_LOSS_DESC AS isValidLossDescription,
				FLAG_VALID_LOSS_DESC_LENGTH AS isValidLossDescriptionLength,
				FLAG_VALID_LOSS_ESTIMATE AS isValidLossEstimate,
				FLAG_VALID_LOSS_SETTLEMENT AS isValidLossSettlement,
				
				CASE
					WHEN
						(
							[FLAG_LEG_FIRE] > 0 /*is LegacyFire*/
							OR [FLAG_UF_FIRE] > 0 /*is UFFire*/
						) 
					THEN
						CASE
							WHEN
								[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/
								AND [FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
							THEN
								CAST(2 AS TINYINT) /*Basic + Lighting/Fire*/
							WHEN
								[FLAG_UF_AUTO] > 0 /*is UF Auto*/
								AND [FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
							THEN
								CAST(3 AS TINYINT) /*Basic + Auto*/
							WHEN
								[FLAG_UF_AUTO] > 0 /*is UF Auto*/
								AND [FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
							THEN
								CAST(4 AS TINYINT) /*Basic + Auto + Lighting/Fire*/
							ELSE
								/*Implicitly:
									[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/ 
									AND [FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
								*/	
								CAST(1 AS TINYINT) /*ONLY Basic*/
						END
					WHEN /*Implicitly NOT Basic*/
						[FLAG_UF_AUTO] = 0
						AND [FLAG_UF_LightExpl] > 0
					THEN
						CAST(5 AS TINYINT) /*Lighting/Fire only*/
					WHEN
						[FLAG_UF_AUTO] > 0
						AND [FLAG_UF_LightExpl] = 0
					THEN
						CAST(6 AS TINYINT) /*Auto Fire only*/
					ELSE
						CAST(NULL AS TINYINT)
				END AS fmPerspectiveTypeId,
				CASE 
					WHEN
						UniqueInstanceStateStatus.F_FM = 'A'
					THEN
						CAST('A' AS CHAR(1)) /*Active (recieves FM in some physical explicit "file" format)*/
					WHEN
						UniqueInstanceStateStatus.F_FM = 'I'
					THEN
						CAST('I' AS CHAR(1)) /*I - Inactive (not currently subscribed to any FM data)*/
					WHEN
						UniqueInstanceStateStatus.F_FM = 'P'
					THEN
						CAST('P' AS CHAR(1)) /*Passive (only subscribed to Dashboard FM data)*/
					ELSE
						CAST(NULL AS CHAR(1)) /*Not a valid state for consideration*/
				END AS fmStateStatusCode,
				@dateInserted AS dateInserted
				INTO #FMClaimData
			FROM
				(
					SELECT
						clt1.I_ALLCLM,
						ROW_NUMBER() OVER(
							PARTITION BY
								clt1.I_ALLCLM
							ORDER BY
								clt1.Date_Insert DESC
						) uniqueInstanceValue
					FROM
						ClaimSearch_Prod.dbo.CLT0001A AS InnerCLT0001A
						INNER JOIN ClaimSearch_Prod.dbo.CLT00014 AS InnerCLT00014
							ON InnerCLT0001A.I_ALLCLM = InnerCLT00014.I_ALLCLM
					WHERE
						InnerCLT0001A.date_insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						
						AND
						(
							(
								CLT1.C_CLM_SRCE = 'P'
								AND CLT1.C_LOSS_TYP = 'FIRE'
							)
							OR 
							(
								CLT1.C_CLM_SRCE = 'U'
								AND clt14.C_LOSS_TYP IN
								(
									'FIRE','LGHT','EXPL'
								)
							)
							OR
							(
								CLT1.C_CLM_SRCE = 'U'
								AND clt14.C_LOSS_TYP = 'FIRE'
								AND
								(
									clt1.F_AUTO = 'Y'
									OR clt1.F_APD = 'Y'
								)
							)
							OR
							(
								CLT1.C_CLM_SRCE = 'U'
								AND clt14.C_LOSS_TYP = 'FIRE'
								AND clt1.C_POL_TYP IN
								(
									'PAPP',
									'CAPP',
									'PPMH'
								)
								AND clt14.C_CVG_TYP IN
								(
									'COMP',
									'OTAU'
								)
							)
						)
				) AS UniqueFireMarshalClaim
				(
					SELECT
						Dashboard_COM_State.[state],
						Dashboard_COM_State.F_FM,
						ROW_NUMBER() OVER
						(
							PARTITION BY
								LTRIM(RTRIM(Dashboard_COM_State.[state]))
							ORDER BY
								Dashboard_COM_State.DAte_Insert DESC
						) AS uniqueInstanceValue
					FROM
						dbo.Dashboard_COM_State
				) AS UniqueInstanceStateStatus
					ON  = UniqueInstanceStateStatus.[state] = 
			WHERE
				UniqueInstanceStateStatus.uniqueInstanceValue = 1;
				
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
			INSERT INTO dbo.FireMarshalDriverActivityLog
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
				@stepDescription = 'TruncateExistingFMDriverData',
				@stepStartDateTime = GETDATE();

			TRUNCATE TABLE dbo.FireMarshalDriver;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalDriverActivityLog
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

			INSERT INTO dbo.Adjuster WITH (TABLOCKX)
			(
				isoClaimId,
				isLegacyFire,
				isUFFire,
				isUFLightExplosion,
				isUFAuto,
				isValidLossDescription,
				isValidLossDescriptionLength,
				isValidLossEstimate,
				isValidLossSettlement,
				fmPerspectiveTypeId,
				fmStateStatusCode,
				dateInserted
			)
			SELECT
				SOURCE.isoClaimId,
				SOURCE.isLegacyFire,
				SOURCE.isUFFire,
				SOURCE.isUFLightExplosion,
				SOURCE.isUFAuto,
				SOURCE.isValidLossDescription,
				SOURCE.isValidLossDescriptionLength,
				SOURCE.isValidLossEstimate,
				SOURCE.isValidLossSettlement,
				SOURCE.fmPerspectiveTypeId,
				SOURCE.fmStateStatusCode,
				@dateInserted
			FROM
				#FMClaimData AS SOURCE
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalDriverActivityLog
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
		INSERT INTO dbo.FireMarshalDriverActivityLog
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
					+ 'of hsp_UpdateInsertFireMarshalDriver; ErrorMsg: '
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

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
	
*/