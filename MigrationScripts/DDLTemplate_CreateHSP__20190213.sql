SET NOEXEC OFF;

/*
TODO:
	1.) find&repalce "__________ActivityLog" with current object activity log table.
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
Date: 
Author: 
Description: 
			
			Performance:

************************************************/
/*Update this name*/ALTER PROCEDURE dbo.hsp_UpdateInsert________
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
				FROM dbo.__________ActivityLog
				WHERE
					__________ActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND __________ActivityLog.isSuccessful = 1
					AND DATEDIFF(HOUR,GETDATE(),__________ActivityLog.executionDateTime) < 12
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
/*Update this note*/				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
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
						MAX(__________ActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.__________ActivityLog
			WHERE
				__________ActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND __________ActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
/*Update this name*/				@stepDescription = 'Capture___________DataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				DuplicateDataSetPerformanceHack.addressId,
				DuplicateDataSetPerformanceHack.isoClaimId/*,
/*Update this*/				...
				...
				...include necessary columns here*/
				INTO #_____Data
			FROM
				(/*Notes on DuplicateDataSetPerformanceHack: dbo.________ contains duplicate records
					performance of rowNumber/partition is noticeably better than using DISTINCT*/
					SELECT
						ExistingAddress.addressId, /*surrogate id*/
						CAST(LTRIM(RTRIM(CLT00001.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId,  /*matching values/sudoPK*/
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00001.ALLCLMROWID /*matching values/sudoPK*/
							ORDER BY
								CLT00001.Date_Insert DESC
						) AS uniqueInstanceValue/*,
/*Update this*/						...
						...
						...include necessary columns here*/
					FROM
/*Update this*/						dbo.________ WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
							ON FM_ExtractFile.I_ALLCLM = CLT00001.I_ALLCLM
						LEFT OUTER JOIN dbo.V_ActiveFMLocationOfLoss AS ExistingAddress WITH (NOLOCK)
							ON ExistingAddress.isoClaimId = CLT00001.I_ALLCLM
					WHERE
						NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
						AND CAST(
							ISNULL(
								CAST(CLT00001.Date_Insert AS CHAR(8)),
								'99990101'
							)
							AS DATE
						) >= @dateFilterParam
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
			INSERT INTO dbo.__________ActivityLog
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
/*Update this*/				@stepDescription = 'UpdateExisting___________Data',
				@stepStartDateTime = GETDATE();

/*Update this whole statement*/			UPDATE dbo.________ WITH (TABLOCKX)
				SET
					/*,
					...
					...
					...include necessary columns here*/
					________.dateInserted = @dateInserted
			FROM
				#_____Data AS SOURCE
			WHERE
				SOURCE.addressId IS NOT NULL
				AND SOURCE.surrogateKey = ________.surrogateKey
				/*AND 
				(
					...
					...
					...include necessary columns here, [remember to handle NULLS ISNULL(columnName,'') etc.]					
				);*/
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.__________ActivityLog
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
/*Update this line*/				@stepDescription = 'InsertNew___________Data', /*Update this note*/
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Address WITH (TABLOCKX)
			(
/*Update this whole statement*/				/*
				...
				...
				...include necessary columns here*/
				isActive,
				dateInserted/*,
				...
				...
				...include necessary columns here*/
			)
			SELECT
				/*
				...
				...
				...include necessary columns here*/
				1 AS isActive,
				@dateInserted AS dateInserted/*,
				...
				...
				...include necessary columns here*/
			FROM
				#_____Data AS SOURCE
			WHERE
				SOURCE.surrogateKey IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.__________ActivityLog
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
		INSERT INTO dbo.__________ActivityLog
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
/*Update this*/					+ 'of hsp_UpdateInsertAddress; ErrorMsg: '
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