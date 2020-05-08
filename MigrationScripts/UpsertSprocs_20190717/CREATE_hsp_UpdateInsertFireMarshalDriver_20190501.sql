SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 20190220
Author: Dan Ravaglia and Robert David Warner
Description: Mechanism for data-refresh of the FM Driver.
			
			Performance: 34.56% performance improvement using windowing function
						to exclude duplication vs. using Distinct.
						Worth exploring performance benefits of MergeIntoSyntax

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertFireMarshalDriver
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
					AND FireMarshalDriverActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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
				@defaultNumberOfYearsToLookBack TINYINT = 5, /*This value remains consistent for variableInit step, initialize now*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CASE
					WHEN
						CAST(CAST(YEAR(GETDATE())-@defaultNumberOfYearsToLookBack AS CHAR(4)) + '0101' AS DATE) < ISNULL(@dateFilterParam,CAST('99990101' AS DATE))
					THEN
						CAST(CAST(YEAR(GETDATE())-@defaultNumberOfYearsToLookBack AS CHAR(4)) + '0101' AS DATE)
					ELSE
						@dateFilterParam
				END
			/* Deprecated old style for Setting Logging Variables for execution
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
			*/
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureFMClaimDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				UniqueFireMarshalClaim.I_ALLCLM AS isoClaimId,
				/*
					FLAG_LEG_FIRE AS isLegacyFire,
					FLAG_UF_FIRE AS isUFFire,
					FLAG_UF_LightExpl AS isUFLightExplosion,
					FLAG_UF_AUTO AS isUFAuto,
				*/
				CASE
					WHEN
						UniqueFireMarshalClaim.FLAG_VALID_LOSS_DESC > 0
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS adhocIncludesValidLossDescription,
				CASE
					WHEN
						UniqueFireMarshalClaim.FLAG_VALID_LOSS_DESC_LENGTH > 0
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS adhocIncludesValidLossDescriptionLength,
				CASE
					WHEN
						UniqueFireMarshalClaim.FLAG_VALID_LOSS_ESTIMATE > 0
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS adhocIncludesValidLossEstimate,
				CASE
					WHEN
						UniqueFireMarshalClaim.FLAG_VALID_LOSS_SETTLEMENT > 0
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS adhocIncludesValidLossSettlement,
				
				CASE
					WHEN
						(
							UniqueFireMarshalClaim.[FLAG_LEG_FIRE] > 0 /*is LegacyFire*/
							OR UniqueFireMarshalClaim.[FLAG_UF_FIRE] > 0 /*is UFFire*/
						) 
					THEN
						CASE
							WHEN
								UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/
								AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
							THEN
								CAST(2 AS TINYINT) /*Basic + Lighting/Fire*/
							WHEN
								UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0 /*is UF Auto*/
								AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
							THEN
								CAST(3 AS TINYINT) /*Basic + Auto*/
							WHEN
								UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0 /*is UF Auto*/
								AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
							THEN
								CAST(4 AS TINYINT) /*Basic + Auto + Lighting/Fire*/
							ELSE
								/*Implicitly:
									UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/ 
									AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
								*/	
								CAST(1 AS TINYINT) /*ONLY Basic*/
						END
					WHEN /*Implicitly NOT Basic*/
						UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0
						AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0
					THEN
						CAST(5 AS TINYINT) /*Lighting/Fire only*/
					WHEN
						UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0
						AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0
					THEN
						CAST(6 AS TINYINT) /*Auto Fire only*/
					ELSE
						CAST(NULL AS TINYINT)
				END AS adhocFMPerspectiveTypeId,
				CASE 
					WHEN
						UniqueInstanceStateStatus.F_FM = 'A'
					THEN
						CAST('A' AS CHAR(1)) /*A - Active Participating State (Requires FireMarshal to recieve Fire claims in some format; Print,email,FTP, dash)*/
					WHEN
						UniqueInstanceStateStatus.F_FM = 'I'
					THEN
						CAST('I' AS CHAR(1)) /*I - Inactive (No current requirement or FireMarshal subscription. New states would need to be added to the FMSchedule helper table)*/
					WHEN
						UniqueInstanceStateStatus.F_FM = 'P'
					THEN
						CAST('P' AS CHAR(1)) /*P - Passive Participating State (Simply doing business with Verisk\ClaimSearch satisfies any requirements by the FireMarshal)*/
					ELSE
						CAST(NULL AS CHAR(1)) /*Not a valid state for consideration*/
				END AS adhocFMStateStatusCode
				INTO #FMClaimData
			FROM
				(
					SELECT
						InnerCLT0001A.I_ALLCLM,
						InnerCLT0001A.C_LOL_ST_ALPH,
						/*1*/SUM(
							CASE
								WHEN
									InnerCLT0001A.C_CLM_SRCE = 'P'
									AND InnerCLT0001A.C_LOSS_TYP = 'FIRE'
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_LEG_FIRE,
						/*2*/SUM(
							CASE
								WHEN
									InnerCLT0001A.C_CLM_SRCE = 'U'
									AND InnerCLT00014.C_LOSS_TYP = 'FIRE'
									AND InnerCLT0001A.F_AUTO = 'N'
									AND InnerCLT0001A.F_APD = 'N'
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_UF_FIRE,
						/*3*/SUM(
							CASE
								WHEN
									InnerCLT0001A.C_CLM_SRCE = 'U'
									AND InnerCLT00014.C_LOSS_TYP IN ('LGHT','EXPL')
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_UF_LightExpl,
						/*4*/SUM(
							CASE
								WHEN
									InnerCLT0001A.C_CLM_SRCE = 'U'
									AND InnerCLT00014.C_LOSS_TYP = 'FIRE'
									AND (
											(
												(
													InnerCLT0001A.F_AUTO = 'Y'
													OR InnerCLT0001A.F_APD = 'Y'
												)
												AND
												(
													InnerCLT0001A.F_PROP = 'N'
													OR InnerCLT0001A.F_CSLTY = 'N'
												)
											)
											OR 
											(
												InnerCLT0001A.C_POL_TYP in ('PAPP','CAPP','PPMH') 
												AND InnerCLT00014.C_CVG_TYP in ('COMP','OTAU') 
											)
										)
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_UF_AUTO,
						/*5*/SUM(
							CASE 
								WHEN
									ISNULL(InnerCLT0001A.T_LOSS_DSC,'Fire') NOT IN (
										'Fire',
										'',
										'blank'
									)
									AND ISNULL(InnerCLT0001A.T_LOSS_DSC_EXT,'Fire') NOT IN (
										'Fire',
										'',
										'blank'
									)
								THEN
									1
								ELSE
									0
								END
							) AS FLAG_VALID_LOSS_DESC,
						/*6*/SUM(
							CASE
								WHEN
									LEN(InnerCLT0001A.T_LOSS_DSC) >6
								THEN
									1
								ELSE 0
							END
						) AS FLAG_VALID_LOSS_DESC_LENGTH,
						/*7*/SUM(
							CASE
								WHEN
									InnerCLT00014.A_EST_LOSS > 0
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_VALID_LOSS_ESTIMATE,
						/*8*/SUM(
							CASE
								WHEN
									InnerCLT00014.A_STTLMT > 0
								THEN
									1
								ELSE
									0
							END
						) AS FLAG_VALID_LOSS_SETTLEMENT
						/*
						,ROW_NUMBER() OVER(
							PARTITION BY
								InnerCLT0001A.I_ALLCLM
							ORDER BY
								InnerCLT0001A.Date_Insert DESC
						) uniqueInstanceValue
						--*/
					FROM
						ClaimSearch_Prod.dbo.CLT0001A AS InnerCLT0001A WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00014 AS InnerCLT00014 WITH (NOLOCK)
							ON InnerCLT0001A.I_ALLCLM = InnerCLT00014.I_ALLCLM
					WHERE
						/* Note, despite this being a dateFilterParam, it's really a MaxDate filter (prevents greater than 5 year old claims coming in)*/
						InnerCLT0001A.date_insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
						AND
						(
							(
								InnerCLT0001A.C_CLM_SRCE = 'P'
								AND InnerCLT0001A.C_LOSS_TYP = 'FIRE'
							)
							OR 
							(
								InnerCLT0001A.C_CLM_SRCE = 'U'
								AND InnerCLT00014.C_LOSS_TYP = 'FIRE'
							)
							OR 
							(
								InnerCLT0001A.C_CLM_SRCE = 'U'
								/*AND InnerCLT0001A.C_LOL_ST_ALPH IN ('KY', 'FL')*/
								AND InnerCLT00014.C_LOSS_TYP = 'EXPL'
							)
							OR 
							(
								InnerCLT0001A.C_CLM_SRCE = 'U'
								/*AND InnerCLT0001A.C_LOL_ST_ALPH = 'KY'*/
								AND InnerCLT00014.C_LOSS_TYP = 'LGHT'
							)
						)
						AND InnerCLT0001A.C_LOL_ST_ALPH IS NOT NULL
					GROUP BY
						InnerCLT0001A.I_ALLCLM,
						InnerCLT0001A.C_LOL_ST_ALPH
				) AS UniqueFireMarshalClaim
				INNER JOIN (
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
						dbo.Dashboard_COM_State WITH (NOLOCK)
				) AS UniqueInstanceStateStatus
					ON UniqueFireMarshalClaim.C_LOL_ST_ALPH = UniqueInstanceStateStatus.[state]
						/*AND UniqueFireMarshalClaim.uniqueInstanceValue = UniqueInstanceStateStatus.uniqueInstanceValue*/
			WHERE
				UniqueInstanceStateStatus.uniqueInstanceValue = 1;
				
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
				@stepDescription = 'Deactivate Previous FM Claims that no longer qualify', /*currently unimpactful, as no FM object filters by NON-ACTIVE Driver row*/
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.FireMarshalDriver
				SET
					FireMarshalDriver.isActive = 0
			FROM
				dbo.FireMarshalDriver
				LEFT OUTER JOIN #FMClaimData AS SOURCE
					ON FireMarshalDriver.isoClaimId = SOURCE.isoClaimId
			WHERE
				SOURCE.isoClaimId IS NULL;

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
				@stepId = 3,
				@stepDescription = 'Update Existing FM Claim', /*really just an update to the adhoc\meta data rows*/
				@stepStartDateTime = GETDATE();
			
			UPDATE FireMarshalDriver
				SET
					FireMarshalDriver.adhocIncludesValidLossDescription = SOURCE.adhocIncludesValidLossDescription,
					FireMarshalDriver.adhocIncludesValidLossDescriptionLength = SOURCE.adhocIncludesValidLossDescriptionLength,
					FireMarshalDriver.adhocIncludesValidLossEstimate = SOURCE.adhocIncludesValidLossEstimate,
					FireMarshalDriver.adhocIncludesValidLossSettlement = SOURCE.adhocIncludesValidLossSettlement,
					FireMarshalDriver.isActive = 1,
					FireMarshalDriver.adhocFMPerspectiveTypeId = SOURCE.adhocFMPerspectiveTypeId,
					FireMarshalDriver.adhocFMStateStatusCode = SOURCE.adhocFMStateStatusCode,
					FireMarshalDriver.dateInserted = @dateInserted
			FROM
				#FMClaimData AS SOURCE
			WHERE
				FireMarshalDriver.isoClaimId = SOURCE.isoClaimId
				AND (
					/*isoClaimId*/
					FireMarshalDriver.adhocIncludesValidLossDescription <> SOURCE.adhocIncludesValidLossDescription
					OR FireMarshalDriver.adhocIncludesValidLossDescriptionLength <> SOURCE.adhocIncludesValidLossDescriptionLength
					OR FireMarshalDriver.adhocIncludesValidLossEstimate <> SOURCE.adhocIncludesValidLossEstimate
					OR FireMarshalDriver.adhocIncludesValidLossSettlement <> SOURCE.adhocIncludesValidLossSettlement
					OR FireMarshalDriver.isActive <> 1
					OR ISNULL(FireMarshalDriver.adhocFMPerspectiveTypeId,255) <> ISNULL(SOURCE.adhocFMPerspectiveTypeId,255)
					OR ISNULL(FireMarshalDriver.adhocFMStateStatusCode,'~') <> ISNULL(SOURCE.adhocFMStateStatusCode,'~')
					/*dateInserted*/
				);

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
				@stepDescription = 'Insert new FM Claims', /*really just an update to the adhoc\meta data rows*/
				@stepStartDateTime = GETDATE();
			
			INSERT INTO dbo.FireMarshalDriver
			(
				isoClaimId,
				/*
				isLegacyFire,
				isUFFire,
				isUFLightExplosion,
				isUFAuto,
				--*/
				adhocIncludesValidLossDescription,
				adhocIncludesValidLossDescriptionLength,
				adhocIncludesValidLossEstimate,
				adhocIncludesValidLossSettlement,
				isActive,
				adhocFMPerspectiveTypeId,
				adhocFMStateStatusCode,
				dateInserted
			)
			SELECT
				#FMClaimData.isoClaimId,
				#FMClaimData.adhocIncludesValidLossDescription,
				#FMClaimData.adhocIncludesValidLossDescriptionLength,
				#FMClaimData.adhocIncludesValidLossEstimate,
				#FMClaimData.adhocIncludesValidLossSettlement,
				1 AS isActive,
				#FMClaimData.adhocFMPerspectiveTypeId,
				#FMClaimData.adhocFMStateStatusCode,
				@dateInserted AS dateInserted
			FROM
				#FMClaimData
				LEFT OUTER JOIN dbo.FireMarshalDriver
					ON #FMCLaimData.isoClaimId = FireMarshalDriver.isoClaimId
			WHERE
				FireMarshalDriver.isoClaimId IS NULL;
				
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

--PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
	
*/