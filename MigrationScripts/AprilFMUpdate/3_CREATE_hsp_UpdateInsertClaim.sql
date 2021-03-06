	SET NOEXEC OFF;

/*
Tables referenced:
	--use as necessary
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
WorkItem: ISCCINTEL-2316
Date: 2019-03-11
Author: Robert David Warner
Description: Mechanism for data-refresh of the Claim Table.
			
			Performance: This SP only upcerts FM claims (for performance reasons)

************************************************/
CREATE PROCEDURE dbo.hsp_UpdateInsertFMClaim
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.ClaimActivityLog
				WHERE
					ClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND ClaimActivityLog.isSuccessful = 1
					AND ClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			
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
						MAX(ClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.ClaimActivityLog
			WHERE
				ClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND ClaimActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureClaimDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingClaim.claimId,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.N_CLM)),'') AS VARCHAR(30)) AS originalClaimNumber,
				CAST(/*ExistingMappedPolicy.policyNumber*/ NULL AS BIGINT) AS policyId,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_CLM_SRCE)),'') AS CHAR(1)) AS claimSearchSourceSystem,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_RPT_SRCE)),'') AS CHAR(1)) AS claimEntryMethod,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_VOID)) = 'X'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isVoidedByInsuranceCarrier,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.T_LOSS_DSC)),'') AS VARCHAR(50)) AS lossDescription,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.T_LOSS_DSC_EXT)),'') AS VARCHAR(200)) AS lossDescriptionExtended,
				/*Deprecated20190328*//*CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_CAT)),'') AS CHAR(1)) AS catastropheId,*/
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_PROP)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchProperty,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_AUTO)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchAuto,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_CSLTY)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchCasualty,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_APD)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchAPD,												
				CASE
					WHEN
						CLT0001A.D_OCUR IS NULL
					THEN
						CAST(NULL AS DATETIME2(0))
					ELSE
						CASE
							WHEN
								/*ISNULL(
									NULLIF(
										LTRIM(
											RTRIM(
												CLT0001A.H_OCUR
											)
										),
										''
									),
									'NULL'
								) NOT LIKE '[0-9][0-9][0-9][0-9]'
								*/
								ISNULL(
									CLT0001A.H_OCUR,
									'NULL'
								) NOT LIKE '[0-2][0-9][0-9][0-9]'
							THEN
								/*TODO: eventually add cases for the 1689/500,000,000+ rows that don't match the HHMM pattern*/
								CAST(CLT0001A.D_OCUR AS DATETIME2(0))
							ELSE
								CASE
									WHEN
										ISNULL(CLT0001A.F_AM_PM,'DefaultBehavior') = 'P'
										AND
										(
											(
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) + 12 <= 23
											)
											OR
											(
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) + 12 = 24
												AND CAST(
													RIGHT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) = 0
											)
										)
									THEN
										DATEADD(
											HOUR,
											CAST(
												LEFT(
													CLT0001A.H_OCUR,
													2
												)
												AS SMALLINT
											) + 12,
											DATEADD(
												MINUTE,
												CAST(
													RIGHT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												),
												CAST(CLT0001A.D_OCUR AS DATETIME2(0))
											)
										)
									ELSE
										/*AM or NULL-AMPM, with HHMM*/
										DATEADD(
											HOUR,
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												),
												DATEADD(
													MINUTE,
														CAST(
															RIGHT(
																CLT0001A.H_OCUR,
																2
															)
															AS SMALLINT
														)
														,
														CAST(CLT0001A.D_OCUR AS DATETIME2(0))
												)
										)
								END
						END
				END AS dateOfLoss,
				CASE
					WHEN
						LEN(CLT0001A.D_INS_CO_RCV) = 10
					THEN
						CAST(
							REPLACE(CLT0001A.D_INS_CO_RCV,'-','')
							AS DATETIME2(0)
						)
					ELSE
						CAST(NULL AS DATETIME2(0))
				END AS insuranceCompanyReceivedDate,
				CASE
					WHEN
						LEN(CLT0001A.D_RCV) = 26
					THEN
						CAST(
							SUBSTRING(CLT0001A.D_RCV,1,10)
							+ ' '
							+ REPLACE((SUBSTRING(CLT0001A.D_RCV,12,8)),'.',':')
							+ (SUBSTRING(CLT0001A.D_RCV,20,8))
							AS DATETIME2(0)
						)
					ELSE
						CAST(NULL AS DATETIME2(0))
				END AS systemDateReceived,
				/*isActive*/
				/*dateInserted*/
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.I_ALLCLM)),'') AS VARCHAR(11)) AS isoClaimId
				INTO #ClaimData
			FROM
				dbo.FireMarshalDriver WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT0001A
					ON FireMarshalDriver.isoClaimId = CLT0001A.I_ALLCLM
				LEFT OUTER JOIN dbo.Claim AS ExistingClaim
					ON CLT0001A.I_ALLCLM = ExistingClaim.isoClaimId
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190306:
					NULLIF(LTRIM(RTRIM(CLT0001A.I_ALLCLM)),'') IS NOT NULL
					*additionally: current count for CLT00001.Date_Insert IS NULL is 0, 20190401
				*/
				CLT0001A.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@stepDescription = 'UpdateExistingClaim',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Claim WITH (TABLOCKX)
				SET
					/*Claim.claimId = SOURCE.claimId,*/
					Claim.originalClaimNumber = SOURCE.originalClaimNumber,
					Claim.policyId = Source.policyId,
					Claim.claimSearchSourceSystem = SOURCE.claimSearchSourceSystem,
					Claim.claimEntryMethod = SOURCE.claimEntryMethod,
					Claim.isVoidedByInsuranceCarrier = SOURCE.isVoidedByInsuranceCarrier,
					Claim.lossDescription = SOURCE.lossDescription,
					Claim.lossDescriptionExtended = SOURCE.lossDescriptionExtended,
					/*Deprecated20190328*//*Claim.catastropheId = SOURCE.catastropheId,*/
					Claim.isClaimSearchProperty = SOURCE.isClaimSearchProperty,
					Claim.isClaimSearchAuto = SOURCE.isClaimSearchAuto,
					Claim.isClaimSearchCasualty = SOURCE.isClaimSearchCasualty,
					Claim.isClaimSearchAPD = SOURCE.isClaimSearchAPD,
					Claim.dateOfLoss = SOURCE.dateOfLoss,
					Claim.insuranceCompanyReceivedDate = SOURCE.insuranceCompanyReceivedDate,
					Claim.systemDateReceived = SOURCE.systemDateReceived,
					/*Claim.isActive = SOURCE.isActive,*/
					Claim.dateInserted = @dateInserted
					/*Claim.isoClaimId = SOURCE.isoClaimId*/
			FROM
				#ClaimData AS SOURCE
			WHERE
				SOURCE.ClaimId = Claim.ClaimId
				AND 
				(
					/*ISNULL(Claim.claimId,'') <> ISNULL(SOURCE.claimId,'')*/
					ISNULL(Claim.originalClaimNumber,'') <> ISNULL(SOURCE.originalClaimNumber,'')
					OR ISNULL(Claim.policyId,-1) = ISNULL(SOURCE.policyId,-1)
					OR ISNULL(Claim.claimSearchSourceSystem,'') <> ISNULL(SOURCE.claimSearchSourceSystem,'')
					OR ISNULL(Claim.claimEntryMethod,'') <> ISNULL(SOURCE.claimEntryMethod,'')
					OR ISNULL(Claim.isVoidedByInsuranceCarrier,'') <> ISNULL(SOURCE.isVoidedByInsuranceCarrier,'')
					OR ISNULL(Claim.lossDescription,'') <> ISNULL(SOURCE.lossDescription,'')
					OR ISNULL(Claim.lossDescriptionExtended,'') <> ISNULL(SOURCE.lossDescriptionExtended,'')
					/*Deprecated20190328*//*OR ISNULL(Claim.catastropheId,'') <> ISNULL(SOURCE.catastropheId,'')*/
					OR ISNULL(Claim.isClaimSearchProperty,0) <> ISNULL(SOURCE.isClaimSearchProperty,0)
					OR ISNULL(Claim.isClaimSearchAuto,0) <> ISNULL(SOURCE.isClaimSearchAuto,0)
					OR ISNULL(Claim.isClaimSearchCasualty,0) <> ISNULL(SOURCE.isClaimSearchCasualty,0)
					OR ISNULL(Claim.isClaimSearchAPD,0) <> ISNULL(SOURCE.isClaimSearchAPD,0)
					OR ISNULL(Claim.dateOfLoss,'19000101') <> ISNULL(SOURCE.dateOfLoss,'19000101')
					OR ISNULL(Claim.insuranceCompanyReceivedDate,'19000101') <> ISNULL(SOURCE.insuranceCompanyReceivedDate,'19000101')
					OR ISNULL(Claim.systemDateReceived,'19000101') <> ISNULL(SOURCE.systemDateReceived,'19000101')
					/*
						OR ISNULL(Claim.isActive,'') <> ISNULL(SOURCE.isActive,'')
						OR ISNULL(Claim.dateInserted,'19000101') <> ISNULL(SOURCE.dateInserted,'19000101')
						OR ISNULL(Claim.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@stepDescription = 'InsertNewClaimData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Claim WITH (TABLOCKX)
			(
				/*claimId,*/
				originalClaimNumber,
				locationOfLossAddressId,
				claimSearchSourceSystem,
				claimEntryMethod,
				isVoidedByInsuranceCarrier,
				lossDescription,
				lossDescriptionExtended,
				/*catastropheId,*/
				isClaimSearchProperty,
				isClaimSearchAuto,
				isClaimSearchCasualty,
				isClaimSearchAPD,
				dateOfLoss,
				insuranceCompanyReceivedDate,
				systemDateReceived,
				isActive,
				dateInserted,
				isoClaimId
			)
			SELECT
				/*SOURCE.claimId,*/
				SOURCE.originalClaimNumber,
				V_ActiveLocationOfLoss.addressId AS locationOfLossAddressId,
				SOURCE.claimSearchSourceSystem,
				SOURCE.claimEntryMethod,
				SOURCE.isVoidedByInsuranceCarrier,
				SOURCE.lossDescription,
				SOURCE.lossDescriptionExtended,
				/*SOURCE.catastropheId,*/
				SOURCE.isClaimSearchProperty,
				SOURCE.isClaimSearchAuto,
				SOURCE.isClaimSearchCasualty,
				SOURCE.isClaimSearchAPD,
				SOURCE.dateOfLoss,
				SOURCE.insuranceCompanyReceivedDate,
				SOURCE.systemDateReceived,
				1 isActive,
				@dateInserted dateInserted,
				SOURCE.isoClaimId
			FROM
				#ClaimData AS SOURCE
				INNER JOIN dbo.V_ActiveLocationOfLoss
					ON V_ActiveLocationOfLoss.isoClaimId = Source.isoClaimId
			WHERE
				SOURCE.claimId iS NULL;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
		INSERT INTO dbo.ClaimActivityLog
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
					+ 'of hsp_UpdateInsertFMClaim; ErrorMsg: '
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
COMMIT TRANSACTION
20190114 : 5:27PM
20190122 : 9:52AM
20190122 : 4:50PM
20190130 : 9:54AM
*/