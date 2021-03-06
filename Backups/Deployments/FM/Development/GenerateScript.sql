/*
	TODO: Will add further controlls around this
*/
BEGIN TRANSACTION
			DECLARE 
				@dateInserted DATETIME2(0) = GETDATE(),
				@UpdateDate BIT = 0;

			/*Set Logging Variables for Current Step_Start*/
			--SELECT
			--	@stepId = 1,
			--	@stepDescription = 'UpdateFMControllerProjectionDate',
			--	@stepStartDateTime = GETDATE();
			IF(@UpdateDate = 1)
			BEGIN
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
							COALESCE(PreviousDate.mostRecentGenerationDate, DELETED.projectedGenerationDate)
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
					LEFT OUTER JOIN (
						SELECT
							FireMarshalClaimSendHistory.lossStateCode AS fmStateCode,
							MAX(FireMarshalClaimSendHistory.fireMarshallDate) AS mostRecentGenerationDate
						FROM
							dbo.FireMarshalClaimSendHistory
						GROUP BY
							FireMarshalClaimSendHistory.lossStateCode
					) PreviousDate
						ON FireMarshalController.fmStateCode = PreviousDate.fmStateCode
				WHERE
					FireMarshalController.endDate IS NULL
					AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< CAST(@dateInserted AS DATE)
					AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('00010101' AS DATE)) <> DATEADD(
						MONTH,
						ProjectedDateToIncrementApply.projectedDateIncrementValue,
						COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
					);
			END;
			
		UPDATE dbo.FireMarshalPendingClaim
		SET FireMarshalPendingClaim.isActive = 0
			OUTPUT
				deleted.elementalClaimId,
				deleted.uniqueInstanceValue,
				deleted.claimId,
				deleted.isoFileNumber,
				'Sent' AS reportingStatus,
				deleted.fireMarshallStatus,
				deleted.fireMarshallDate,
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
	WHERE
		FireMarshalPendingClaim.reportingStatus = 'Pending'
		AND FireMarshalPendingClaim.isCurrent = 1
		AND FireMarshalPendingClaim.isActive = 1
		AND FireMarshalPendingClaim.dateSubmittedToIso < DATEADD(MONTH,-2,GETDATE());
		

--PRINT 'ROLLBACK' ROLLBACK TRANSACTION;
PRINT 'COMMIT' COMMIT TRANSACTION;