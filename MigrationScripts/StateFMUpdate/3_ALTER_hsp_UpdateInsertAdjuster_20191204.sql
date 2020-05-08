SET NOEXEC OFF;

USE ClaimSearch_Prod;
--USE ClaimSearch_Dev;

/******MSGLog Snippet. Can be added to comment block at end of query after execute for recordkeeping.******/
DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(2) = CHAR(13) + CHAR(10);
DECLARE @currentDBEnv VARCHAR(100) = CAST(@@SERVERNAME + '.' + DB_NAME() AS VARCHAR(100));
DECLARE @currentUser VARCHAR(100) = CAST(CURRENT_USER AS VARCHAR(100));
DECLARE @executeTimestamp VARCHAR(20) = CAST(GETDATE() AS VARCHAR(20));
Print '*****************************************' + @newLine
	+ '*' + @tab + 'Env: ' + 
	+ CASE
	WHEN
		LEN(@currentDBEnv) >=27
	THEN
		@currentDBEnv
	ELSE
		@currentDBEnv + @tab
	END
	+ @tab + '*' +@newLine
	+ '*' + @tab + 'User: ' + @currentUser + @tab + @tab + @tab + @tab + '*' +@newLine
	+ '*' + @tab + 'Time: ' + @executeTimestamp + @tab + @tab + @tab + '*' +@newLine
	+'*****************************************';
/**********************************************************************************************************/
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
Description: Mechanism for data-refresh of the Adjuster Table.
			
			Performance:

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertAdjuster
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
				FROM dbo.AdjusterActivityLog
				WHERE
					AdjusterActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND AdjusterActivityLog.isSuccessful = 1
					AND AdjusterActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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
						MAX(AdjusterActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.AdjusterActivityLog
			WHERE
				AdjusterActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND AdjusterActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureAdjusterDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				DuplicateDataSetPerformanceHack.adjusterId,
				DuplicateDataSetPerformanceHack.adjusterCompanyCode,
				DuplicateDataSetPerformanceHack.adjusterOfficeCode,
				DuplicateDataSetPerformanceHack.adjusterDateSubmitted,
				NULLIF(LTRIM(RTRIM(DuplicateDataSetPerformanceHack.adjusterName)),'') AS adjusterName,
				DuplicateDataSetPerformanceHack.adjusterAreaCode,
				DuplicateDataSetPerformanceHack.adjusterCompanyName,
				DuplicateDataSetPerformanceHack.adjusterPhoneNumber,
				/*DuplicateDataSetPerformanceHack.isActive,*/
				/*DuplicateDataSetPerformanceHack.dateInserted,*/
				DuplicateDataSetPerformanceHack.isoClaimId,
				DuplicateDataSetPerformanceHack.involvedPartySequenceId,
				DuplicateDataSetPerformanceHack.adjusterSequenceId
				INTO #AdjusterData
			FROM
				(/*Notes on DuplicateDataSetPerformanceHack: dbo.Adjuster contains duplicate records
					performance of rowNumber/partition is noticeably better than using DISTINCT*/
					SELECT
						ExistingAdjuster.adjusterId, /*surrogate id*/
						CLT00002.I_ALLCLM AS isoClaimId,
						CLT00014.I_NM_ADR AS involvedPartySequenceId,
						CLT00002.N_ADJ_SEQ AS adjusterSequenceId,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00002.I_ALLCLM,
								CLT00014.I_NM_ADR,
								CLT00002.N_ADJ_SEQ 
							ORDER BY
								CLT00002.Date_Insert DESC
						) AS uniqueInstanceValue,
						CLT00002.I_CUST AS adjusterCompanyCode,
						CompanyHeirarchy.Customer_lvl0 AS adjusterCompanyName,
						CLT00002.I_REGOFF AS adjusterOfficeCode,
						CLT00002.D_ADJ_SUBM AS adjusterDateSubmitted,
						CLT00002.M_FUL_NM AS adjusterName,
						CLT00002.N_AREA AS adjusterAreaCode,
						CLT00002.N_TEL AS adjusterPhoneNumber
						/*isActive,*/
						/*dateInserted,*/
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00002 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00002.I_ALLCLM
						INNER JOIN ClaimSearch_Prod.dbo.CLT00014 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00014.I_ALLCLM
								AND CLT00002.N_ADJ_SEQ = CLT00014.I_NM_ADR
						INNER JOIN ClaimSearch_Prod.dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH (NOLOCK)
							ON CLT00002.I_CUST = CompanyHeirarchy.lvl0
						LEFT OUTER JOIN dbo.Adjuster AS ExistingAdjuster WITH (NOLOCK)
							ON ExistingAdjuster.isoClaimId = CLT00002.I_ALLCLM
								AND ExistingAdjuster.involvedPartySequenceId = CLT00014.I_NM_ADR
								AND ExistingAdjuster.adjusterSequenceId = CLT00002.N_ADJ_SEQ
					WHERE
						/*Deprecating due to performance costs, and current profile state. RDW 20190328:
							NULLIF(LTRIM(RTRIM(CLT00002.I_ALLCLM)),'') IS NOT NULL
						*/
						CLT00002.Date_Insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
				) AS DuplicateDataSetPerformanceHack
			WHERE
				DuplicateDataSetPerformanceHack.uniqueInstanceValue = 1;
				
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
			INSERT INTO dbo.AdjusterActivityLog
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
				@stepDescription = 'UpdateExistingAdjusterData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Adjuster WITH (TABLOCKX)
				SET
					Adjuster.adjusterCompanyCode = SOURCE.adjusterCompanyCode,
					Adjuster.adjusterCompanyName = SOURCE.adjusterCompanyName,
					Adjuster.adjusterOfficeCode = SOURCE.adjusterOfficeCode,
					Adjuster.adjusterDateSubmitted = SOURCE.adjusterDateSubmitted,
					Adjuster.adjusterName = SOURCE.adjusterName,
					Adjuster.adjusterAreaCode = SOURCE.adjusterAreaCode,
					Adjuster.adjusterPhoneNumber = SOURCE.adjusterPhoneNumber,
					/*Adjuster.isActive = SOURCE.isActive, / 1 */
					Adjuster.dateInserted = @dateInserted,
					Adjuster.isoClaimId = SOURCE.isoClaimId,
					Adjuster.involvedPartySequenceId = SOURCE.involvedPartySequenceId,
					Adjuster.adjusterSequenceId = SOURCE.adjusterSequenceId
			FROM
				#AdjusterData AS SOURCE
			WHERE
				SOURCE.adjusterId IS NOT NULL
				AND Adjuster.adjusterId = SOURCE.adjusterId
				AND 
				(
					ISNULL(Adjuster.adjusterCompanyCode,'~~~~') <> ISNULL(SOURCE.adjusterCompanyCode,'~~~~')
					OR ISNULL(Adjuster.adjusterCompanyName,'~~~') <> ISNULL(SOURCE.adjusterCompanyName,'~~~')
					OR ISNULL(Adjuster.adjusterOfficeCode,'~~~~~') <> ISNULL(SOURCE.adjusterOfficeCode,'~~~~~')
					OR ISNULL(Adjuster.adjusterDateSubmitted,CAST('19000115' AS DATE)) <> ISNULL(SOURCE.adjusterDateSubmitted,CAST('19000115' AS DATE))
					OR ISNULL(Adjuster.adjusterName,'~~~') <> ISNULL(SOURCE.adjusterName,'~~~')
					OR ISNULL(Adjuster.adjusterAreaCode,-1) <> ISNULL(SOURCE.adjusterAreaCode,-1)
					OR ISNULL(Adjuster.adjusterPhoneNumber,-1) <> ISNULL(SOURCE.adjusterPhoneNumber,-1)
					OR ISNULL(Adjuster.isoClaimId,'~~~') <> ISNULL(SOURCE.isoClaimId,'~~~')
					OR ISNULL(Adjuster.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
					OR ISNULL(Adjuster.adjusterSequenceId,-1) <> ISNULL(SOURCE.adjusterSequenceId,-1)
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
				@stepDescription = 'InsertNewAdjusterData', /*Update this note*/
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Adjuster WITH (TABLOCKX)
			(
				/*adjusterId*/
				adjusterCompanyCode,
				adjusterCompanyName,
				adjusterOfficeCode,
				adjusterDateSubmitted,
				adjusterName,
				adjusterAreaCode,
				adjusterPhoneNumber,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId,
				adjusterSequenceId
			)
			SELECT
				SOURCE.adjusterCompanyCode,
				SOURCE.adjusterCompanyName,
				SOURCE.adjusterOfficeCode,
				SOURCE.adjusterDateSubmitted,
				SOURCE.adjusterName,
				SOURCE.adjusterAreaCode,
				SOURCE.adjusterPhoneNumber,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.adjusterSequenceId
			FROM
				#AdjusterData AS SOURCE
			WHERE
				SOURCE.adjusterId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
		INSERT INTO dbo.AdjusterActivityLog
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
					+ 'of hsp_UpdateInsertAdjuster; ErrorMsg: '
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
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  5:31PM			*
*****************************************
COMMIT TRANSACTION

*/