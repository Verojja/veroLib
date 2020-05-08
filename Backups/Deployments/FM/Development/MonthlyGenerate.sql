SET NOEXEC OFF;

BEGIN TRANSACTION;

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

DECLARE 
	@dateInserted DATETIME2(0) = GETDATE(),
	@UpdateDate BIT = 0;

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
	SET
		FireMarshalPendingClaim.isActive = 0
	OUTPUT
		deleted.elementalClaimId,
		deleted.uniqueInstanceValue,
		deleted.claimId,
		deleted.isoFileNumber,
		'Sent' AS reportingStatus,
		deleted.fireMarshallStatus,
		ISNULL(DuplicateFMExtractPartition.dateInserted, @dateInserted) AS fireMarshallDate,
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
		AND FireMarshalPendingClaim.isCurrent = 1
		AND FireMarshalPendingClaim.isActive = 1
		AND DuplicateFMExtractPartition.uniqueInstanceValue = 1
		AND ISNULL(DuplicateFMExtractPartition.dateInserted, FireMarshalPendingClaim.dateSubmittedToIso) < DATEADD(MONTH,-5,GETDATE());
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
	/****Up-sert Extract****/

EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	@dailyLoadOverride = 1
--*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
	/****doublecheck-counts****/

SELECT 
'Passive' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalClaimSendHistory
WHERE FireMarshalClaimSendHistory.reportingStatus = 'Passive'
UNION ALL
SELECT 
'Sent' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalClaimSendHistory
WHERE FireMarshalClaimSendHistory.reportingStatus = 'Sent'

SELECT 
'Pending' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus = 'Pending'
UNION ALL
SELECT 
'Exception' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus = 'Exception'


SELECT 
'Pending' as countName,
COUNT(*) AS countValue FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Pending'
UNION ALL
SELECT 
'Passive' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Passive'
UNION ALL
SELECT 
'Exception' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Exception'
UNION ALL
SELECT 
'Sent' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Sent'
--*/

PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT 'COMMIT';COMMIT TRANSACTION;