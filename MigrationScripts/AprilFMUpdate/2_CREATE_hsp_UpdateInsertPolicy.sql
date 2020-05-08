SET NOEXEC OFF;

/*
TODO:
	Change the "Product" variable (line 59)
	Change the ActivityLogTable references
*/

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
Date: 20190225
Author: Robert David Warner
Description: Mechanism for data-refresh of the Policy Table
			
			Performance:

************************************************/
CREATE PROCEDURE dbo.hsp_UpdateInsertFMPolicy
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
				FROM dbo.PolicyActivityLog
				WHERE
					PolicyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND PolicyActivityLog.isSuccessful = 1
					AND PolicyActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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
						MAX(PolicyActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.PolicyActivityLog
			WHERE
				PolicyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND PolicyActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CapturePolicyDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingInvolvedParty.policyId, /*surrogate id*/
				NULLIF(LTRIM(RTRIM(CLT00001.I_CUST)),'') AS insuranceProviderCompanyCode,
				NULLIF(LTRIM(RTRIM(CLT00001.I_REGOFF)),'') AS insuranceProviderOfficeCode,
				ISNULL(LTRIM(RTRIM(CLT00001.N_POL)),'NA') AS originalPolicyNumber,
				NULLIF(LTRIM(RTRIM(CLT00001.C_POL_TYP)),'') AS policyTypeCode,
				NULLIF(LTRIM(RTRIM(CLT00001.D_POL_INCP)),'') AS originalPolicyInceptionDate,
				CLT00001.D_POL_EXPIR AS originalPolicyExperiationDate,
				/*dateInserted,*/
				CLT00001.I_ALLCLM AS isoClaimId
				INTO #PolicyData
			FROM
				dbo.FireMarshalDriver
				INNER JOIN ClaimSearch_Prod.dbo.CLT00001
					ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
				LEFT OUTER JOIN dbo.Policy AS ExistingInvolvedParty
					ON CLT00001.I_ALLCLM = ExistingInvolvedParty.isoClaimId
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190328:
					NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
					additionally: current count for CLT00001.Date_Insert IS NULL is 0, 20190401
				*/
				CLT00001.Date_Insert >= CAST(
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
			INSERT INTO dbo.PolicyActivityLog
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
				@stepDescription = 'UpdateExistingPolicy',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Policy WITH (TABLOCKX)
				SET
					/*policyId*/
					Policy.insuranceProviderCompanyCode = SOURCE.insuranceProviderCompanyCode,
					Policy.insuranceProviderOfficeCode = SOURCE.insuranceProviderOfficeCode,
					Policy.originalPolicyNumber = SOURCE.originalPolicyNumber,
					Policy.policyTypeCode = SOURCE.policyTypeCode,
					Policy.originalPolicyInceptionDate = SOURCE.originalPolicyInceptionDate,
					Policy.originalPolicyExperiationDate = SOURCE.originalPolicyExperiationDate,
					/*Policy.isActive = SOURCE.isActive,*/
					Policy.dateInserted = @dateInserted
					/*Policy.isoClaimId = SOURCE.isoClaimId*/
			FROM
				#PolicyData AS SOURCE
			WHERE
				SOURCE.policyId = Policy.policyId
				AND 
				(
					ISNULL(Policy.insuranceProviderCompanyCode,'') <> ISNULL(SOURCE.insuranceProviderCompanyCode,'')
					OR ISNULL(Policy.insuranceProviderOfficeCode,'') <> ISNULL(SOURCE.insuranceProviderOfficeCode,'')
					OR ISNULL(Policy.originalPolicyNumber,'') <> ISNULL(SOURCE.originalPolicyNumber,'')
					OR ISNULL(Policy.policyTypeCode,'') <> ISNULL(SOURCE.policyTypeCode,'')
					OR ISNULL(Policy.originalPolicyInceptionDate,'') <> ISNULL(SOURCE.originalPolicyInceptionDate,'')
					OR ISNULL(Policy.originalPolicyExperiationDate,'') <> ISNULL(SOURCE.originalPolicyExperiationDate,'')
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
				@stepDescription = 'InsertNewPolicyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Policy WITH (TABLOCKX)
			(
				/*policyId*/
				insuranceProviderCompanyCode,
				insuranceProviderOfficeCode,
				originalPolicyNumber,
				policyTypeCode,
				originalPolicyInceptionDate,
				originalPolicyExperiationDate,
				isActive,
				dateInserted,
				isoClaimId
			)
			SELECT
				SOURCE.insuranceProviderCompanyCode,
				SOURCE.insuranceProviderOfficeCode,
				SOURCE.originalPolicyNumber,
				SOURCE.policyTypeCode,
				SOURCE.originalPolicyInceptionDate,
				SOURCE.originalPolicyExperiationDate,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId
			FROM
				#PolicyData AS SOURCE
			WHERE
				SOURCE.policyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
		INSERT INTO dbo.PolicyActivityLog
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
					+ 'of hsp_UpdateInsertFMPolicy; ErrorMsg: '
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