SET NOEXEC OFF;

/*
TODO:
	add additional param for manual override if the jobs are scheduled on some kind of high-frequency repetative cycle.
*/

/*
Tables referenced:
	[ClaimSearch].dbo.CS_Lookup_Melissa_InvolvedParty_Mapping_to_CLT00001
	[ClaimSearch].dbo.CS_Lookup_Unique_InvolvedPartyes_Melissa_Output
	[ClaimSearch_Prod].dbo.CLT00001
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
WorkItem: ?????
Date: 2019-01-08
Author: Robert David Warner
Description: Test for TransactionSproc.
************************************************/
CREATE PROCEDURE dbo.hsp_TransactionSproc
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	DECLARE @internalTransactionCount TINYINT = 0;
	
	SELECT
		1 AS outPutStatement,
		@@TRANCOUNT AS globalTranCount;
		
	IF (@@TRANCOUNT = 0)
	BEGIN
		BEGIN TRANSACTION;
		SET @internalTransactionCount = 1;
		
		SELECT
			2 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
	END
	BEGIN TRY
		
		SELECT 'inside the actual try statement';
		
		SELECT
			3 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
		IF (@internalTransactionCount = 1)
		BEGIN
		
		SELECT
			'insideIf, pre commit' AS note,
			4 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
			COMMIT TRANSACTION;
			
			
		SELECT
			'insideIf, post commit' AS note,
			5 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
		END
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		
		SELECT
			'insideCatch, pre If' AS note,
			6 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
		IF (@internalTransactionCount = 1)
		BEGIN
		
		SELECT
			'insideCatch, inside if, pre rollback' AS note,
			7 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
			ROLLBACK TRANSACTION;
			
		SELECT
			'insideCatch, inside if, post rollback' AS note,
			8 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.InvolvedPartyActivityLog
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
			
				
		SELECT
			'insideCatch, post firs if, post rollback, pre second if' AS note,
			9 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
		
			SELECT
			'insideCatch, post firs if, post rollback, inside second if' AS note,
			10 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertInvolvedParty; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
			
			SELECT
			'insideCatch, post firs if, post rollback, inside second if, postRaiseError' AS note,
			11 AS outPutStatement,
			@@TRANCOUNT AS globalTranCount,
			@internalTransactionCount AS internalTranCountVar;
			
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