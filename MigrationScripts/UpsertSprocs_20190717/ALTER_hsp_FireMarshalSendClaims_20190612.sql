SET NOEXEC OFF;

--USE ClaimSearch_Dev;
USE ClaimSearch_Prod;

BEGIN TRANSACTION

IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for Monthly Generate .
			
************************************************/
ALTER PROCEDURE dbo.hsp_FireMarshalSendClaims
	@mustMatchDB2FMProcess BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Wrapper to allow for daily-execution schedule of otherwise monthly process*/
		IF(
			NOT EXISTS
			(
				SELECT NULL
				FROM dbo.FireMarshalGenerationLog
				WHERE
					FireMarshalGenerationLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FireMarshalGenerationLog.isSuccessful = 1
					AND MONTH(FireMarshalGenerationLog.executionDateTime) = MONTH(GETDATE())
					AND DAY(GETDATE()) >= 5
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'Migrate for FM Claims with qualifying ProjectedGenDate',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.FireMarshalPendingClaim
				SET
					FireMarshalPendingClaim.isActive = 0
				OUTPUT
					deleted.elementalClaimId,
					deleted.uniqueInstanceValue,
					deleted.claimId,
					deleted.isoFileNumber,
					'Sent' AS reportingStatus,
					CASE
						WHEN
							@mustMatchDB2FMProcess = 1 
						THEN
							(CAST(DATENAME(MONTH, DuplicateFMExtractPartition.dateInserted) AS VARCHAR(255)) + ' ' + CAST(YEAR(DuplicateFMExtractPartition.dateInserted) AS CHAR(4)))
						ELSE
							(CAST(DATENAME(MONTH, @dateInserted) AS VARCHAR(255)) + ' ' + CAST(YEAR(@dateInserted) AS CHAR(4)))
					END AS fireMarshallStatus,
					CASE
						WHEN
							@mustMatchDB2FMProcess = 1 
						THEN
							DuplicateFMExtractPartition.dateInserted
						ELSE
							@dateInserted
					END AS fireMarshallDate,
					deleted.claimIsOpen,
					deleted.dateSubmittedToIso,
					deleted.originalClaimNumber,
					deleted.originalPolicyNumber,
					deleted.insuranceProviderOfficeCode,
					deleted.insuranceProviderCompanyCode,
					deleted.companyName,
					deleted.affiliate1Code,
					deleted.affiliate1Name,
					deleted.affiliate2Code,
					deleted.affiliate2Name,
					deleted.groupCode,
					deleted.groupName,
					deleted.lossAddressLine1,
					deleted.lossAddressLine2,
					deleted.lossCityName,
					deleted.lossStateCode,
					deleted.lossStateName,
					deleted.lossZipCode,
					deleted.lossGeoCounty,
					deleted.lossLatitude,
					deleted.lossLongitude,
					deleted.lossDescription,
					deleted.lossDescriptionExtended,
					deleted.dateOfLoss,
					deleted.lossTypeCode,
					deleted.lossTypeDescription,
					deleted.policyTypeCode,
					deleted.policyTypeDescription,
					deleted.coverageTypeCode,
					deleted.coverageTypeDescription,
					deleted.estimatedLossAmount,
					deleted.settlementAmount,
					deleted.policyAmount,
					deleted.buildingPaidAmount,
					deleted.contentReserveAmount,
					deleted.contentPaidAmount,
					deleted.isIncendiaryFire,
					deleted.isClaimUnderSIUInvestigation,
					deleted.siuCompanyName,
					deleted.siuRepresentativeFullName,
					deleted.siuWorkPhoneNumber,
					deleted.siuCellPhoneNumber,
					deleted.involvedPartyId,
					deleted.involvedPartyFullName,
					deleted.adjusterId,
					deleted.involvedPartySequenceId,
					deleted.reserveAmount,
					deleted.totalInsuredAmount,
					deleted.replacementAmount,
					deleted.actualCashAmount,
					deleted.buildingPolicyAmount,
					deleted.buildingTotalInsuredAmount,
					deleted.buildingReplacementAmount,
					deleted.buildingActualCashAmount,
					deleted.buildingEstimatedLossAmount,
					deleted.contentPolicyAmount,
					deleted.contentTotalInsuredAmount,
					deleted.contentReplacementAmount,
					deleted.contentActualCashAmount,
					deleted.contentEstimatedLossAmount,
					deleted.stockPolicyAmount,
					deleted.stockTotalInsuredAmount,
					deleted.stockReplacementAmount,
					deleted.stockActualCashAmount,
					deleted.stockEstimatedLossAmount,
					deleted.lossOfUsePolicyAmount,
					deleted.lossOfUseTotalInsuredAmount,
					deleted.lossOfUseReplacementAmount,
					deleted.lossOfUseActualCashAmount,
					deleted.lossOfUseEstimatedLossAmount,
					deleted.otherPolicyAmount,
					deleted.otherTotalInsuredAmount,
					deleted.otherReplacementAmount,
					deleted.otherActualCashAmount,
					deleted.otherEstimatedLossAmount,
					deleted.buildingReserveAmount,
					deleted.stockReserveAmount,
					deleted.stockPaidAmount,
					deleted.lossOfUseReserve,
					deleted.lossOfUsePaid,
					deleted.otherReserveAmount,
					deleted.otherPaidAmount,
					deleted.isActive,
					@dateInserted AS dateInserted
				INTO dbo.FireMarshalClaimSendHistory
				FROM
					dbo.FireMarshalPendingClaim
					INNER JOIN dbo.FireMarshalController
						ON FireMarshalPendingClaim.lossStateCode = FireMarshalController.fmStateCode
							AND FireMarshalController.endDate IS NULL
					LEFT OUTER JOIN dbo.V_ActiveFireMarshalClaimSendHistory
						ON V_ActiveFireMarshalClaimSendHistory.elementalClaimId = FireMarshalPendingClaim.elementalClaimId
					LEFT OUTER JOIN 
					(
						SELECT
							V_Extract_FM_V1.I_ALLCLM,
							V_Extract_FM_V1.M_FUL_NM,
							CAST(CAST(V_Extract_FM_V1.DATE_INSERT AS CHAR(8))AS DATE) AS dateInserted,
							ROW_NUMBER() OVER (
								PARTITION BY
									V_Extract_FM_V1.I_ALLCLM,
									V_Extract_FM_V1.M_FUL_NM
								ORDER BY
									V_Extract_FM_V1.DATE_INSERT
							) AS uniqueInstanceValue
						FROM
							ClaimSearch_Prod.dbo.V_Extract_FM_V1
					) AS DuplicateFMExtractPartition
						ON FireMarshalPendingClaim.isoFileNumber = DuplicateFMExtractPartition.I_ALLCLM
						AND FireMarshalPendingClaim.involvedPartyFullName = DuplicateFMExtractPartition.M_FUL_NM
				WHERE
					FireMarshalPendingClaim.reportingStatus = 'Pending'
					AND V_ActiveFireMarshalClaimSendHistory.elementalClaimId IS NULL
					AND FireMarshalPendingClaim.isCurrent = 1
					AND FireMarshalPendingClaim.isActive = 1
					AND COALESCE(DuplicateFMExtractPartition.I_ALLCLM,CAST(NULLIF(@mustMatchDB2FMProcess,1) AS VARCHAR(11))) IS NOT NULL
					AND ISNULL(DuplicateFMExtractPartition.uniqueInstanceValue,1) = 1
					AND @dateInserted >= FireMarshalController.projectedGenerationDate

		/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@dateInserted,
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
				@stepDescription = 'Update Claim ProjectedGenerationDate, as needed',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.FireMarshalController
				SET
					FireMarshalController.endDate = @dateInserted	
				OUTPUT
					DELETED.fmStateCode,
					DELETED.fmQualificationRequirmentSetId,
					DELETED.fmStateStatusCode,
					DELETED.frequencyCode,
					DATEADD(
						MONTH,
						ProjectedDateToIncrementApply.projectedDateIncrementValue,
						DELETED.projectedGenerationDate
					) /*new projectedGenerationDate*/,
					DELETED.receivesPrint,
					DELETED.receivesFTP,
					DELETED.receivesEmail,
					DELETED.fmContactFirstName,
					DELETED.fmContactMiddleName,
					DELETED.fmContactLastName,
					DELETED.fmContactSuffixName,
					DELETED.fmContactDeptartmentName,
					DELETED.fmContactDivisionName,
					DELETED.fmContactDeliveryAddressLine1,
					DELETED.fmContactDeliveryAddressLine2,
					DELETED.fmContactDeliveryCity,
					DELETED.fmContactDeliveryStateCode,
					DELETED.fmContactZipCode,
					DELETED.fmContactTitleName,
					DELETED.fmContactSalutation,
					@dateInserted,
					DELETED.endDate
				INTO dbo.FireMarshalController
				(
					fmStateCode,
					fmQualificationRequirmentSetId,
					fmStateStatusCode,
					frequencyCode,
					projectedGenerationDate,
					receivesPrint,
					receivesFTP,
					receivesEmail,
					fmContactFirstName,
					fmContactMiddleName,
					fmContactLastName,
					fmContactSuffixName,
					fmContactDeptartmentName,
					fmContactDivisionName,
					fmContactDeliveryAddressLine1,
					fmContactDeliveryAddressLine2,
					fmContactDeliveryCity,
					fmContactDeliveryStateCode,
					fmContactZipCode,
					fmContactTitleName,
					fmContactSalutation,
					dateInserted,
					endDate
				)
			FROM
				dbo.FireMarshalController
				CROSS APPLY 
				(
					SELECT /*BehaviorLogic for daily\weekly\yearly NOT YET IMPLIMENTED*/
						CASE
							WHEN
								FireMarshalController.frequencyCode = 'Q'
								AND MONTH(FireMarshalController.projectedGenerationDate) < 4 
							THEN
								3
							WHEN
								FireMarshalController.frequencyCode = 'Q'
							THEN
								4
							WHEN
								FireMarshalController.frequencyCode = 'M'
							THEN
								1
							ELSE
								1
						END AS projectedDateIncrementValue
				) AS ProjectedDateToIncrementApply
			WHERE
				FireMarshalController.endDate IS NULL
				AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< CAST(@dateInserted AS DATE)

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@dateInserted,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

		END;
		IF (@internalTransactionCount = 1)
		BEGIN
			COMMIT TRANSACTION;
		END
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
		INSERT INTO dbo.FireMarshalGenerationLog
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
			@dateInserted,
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
					+ 'of hsp_FireMarshalSendClaims; ErrorMsg: '
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