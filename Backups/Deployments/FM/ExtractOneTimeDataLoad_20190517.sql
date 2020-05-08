BEGIN TRANSACTION
DECLARE @dateInserted DATE = GETDATE();
INSERT INTO dbo.FireMarshalExtract
(
	claimId,
	uniqueInstanceValue,
	isoFileNumber,
	reportingStatus,
	fMstatus,
	fMDate,
	claimIsOpen,
	dateSubmittedToIso,
	originalClaimNumber,
	originalPolicyNumber,
	insuranceProviderOfficeCode,
	insuranceProviderCompanyCode,
	adjusterCompanyCode,
	adjusterCompanyName,
	companyName,
	affiliate1Code,
	affiliate1Name,
	affiliate2Code,
	affiliate2Name,
	groupCode,
	groupName,
	lossAddressLine1,
	lossAddressLine2,
	lossCityName,
	lossStateCode,
	lossStateName,
	lossGeoCounty,
	lossZipCode,
	lossLatitude,
	lossLongitude,
	lossDescription,
	lossDescriptionExtended,
	dateOfLoss,
	lossTypeCode,
	lossTypeDescription,
	policyTypeCode,
	policyTypeDescription,
	coverageTypeCode,
	coverageTypeDescription,
	settlementAmount,
	estimatedLossAmount,
	policyAmount,
	buildingPaidAmount,
	contentReserveAmount,
	contentPaidAmount,
	isIncendiaryFire,
	isClaimUnderSIUInvestigation,
	siuCompanyName,
	siuRepresentativeFullName,
	siuWorkPhoneNumber,
	siuCellPhoneNumber,
	isActive,
	isCurrent,
	involvedPartyId,
	adjusterId,
	involvedPartySequenceId,
	dateInserted
)
SELECT
	DashboardAggregationByClaim.claimId,
	DashboardAggregationByClaim.uniqueInstanceValue,
	DashboardAggregationByClaim.isoFileNumber,
	DashboardAggregationByClaim.reportingStatus,
	DashboardAggregationByClaim.fMstatus,
	DashboardAggregationByClaim.fMDate,
	DashboardAggregationByClaim.claimIsOpen,
	DashboardAggregationByClaim.dateSubmittedToIso,
	DashboardAggregationByClaim.originalClaimNumber,
	DashboardAggregationByClaim.originalPolicyNumber,
	DashboardAggregationByClaim.insuranceProviderOfficeCode,
	DashboardAggregationByClaim.insuranceProviderCompanyCode,
	DashboardAggregationByClaim.adjusterCompanyCode,
	DashboardAggregationByClaim.adjusterCompanyName,
	DashboardAggregationByClaim.companyName,
	DashboardAggregationByClaim.affiliate1Code,
	DashboardAggregationByClaim.affiliate1Name,
	DashboardAggregationByClaim.affiliate2Code,
	DashboardAggregationByClaim.affiliate2Name,
	DashboardAggregationByClaim.groupCode,
	DashboardAggregationByClaim.groupName,
	DashboardAggregationByClaim.lossAddressLine1,
	DashboardAggregationByClaim.lossAddressLine2,
	DashboardAggregationByClaim.lossCityName,
	DashboardAggregationByClaim.lossStateCode,
	DashboardAggregationByClaim.lossStateName,
	DashboardAggregationByClaim.lossGeoCounty,
	DashboardAggregationByClaim.lossZipCode,
	DashboardAggregationByClaim.lossLatitude,
	DashboardAggregationByClaim.lossLongitude,
	DashboardAggregationByClaim.lossDescription,
	DashboardAggregationByClaim.lossDescriptionExtended,
	DashboardAggregationByClaim.dateOfLoss,
	DashboardAggregationByClaim.lossTypeCode,
	DashboardAggregationByClaim.lossTypeDescription,
	DashboardAggregationByClaim.policyTypeCode,
	DashboardAggregationByClaim.policyTypeDescription,
	DashboardAggregationByClaim.coverageTypeCode,
	DashboardAggregationByClaim.coverageTypeDescription,
	DashboardAggregationByClaim.settlementAmount,
	DashboardAggregationByClaim.estimatedLossAmount,
	DashboardAggregationByClaim.policyAmount,
	DashboardAggregationByClaim.buildingPaidAmount,
	DashboardAggregationByClaim.contentReserveAmount,
	DashboardAggregationByClaim.contentPaidAmount,
	DashboardAggregationByClaim.isIncendiaryFire,
	DashboardAggregationByClaim.isClaimUnderSIUInvestigation,
	DashboardAggregationByClaim.siuCompanyName,
	DashboardAggregationByClaim.siuRepresentativeFullName,
	DashboardAggregationByClaim.siuWorkPhoneNumber,
	DashboardAggregationByClaim.siuCellPhoneNumber,
	1 AS isActive,
	DashboardAggregationByClaim.isCurrent,
	DashboardAggregationByClaim.involvedPartyId,
	DashboardAggregationByClaim.adjusterId,
	DashboardAggregationByClaim.involvedPartySequenceId,
	@dateInserted AS dateInserted
	
FROM
	(
		SELECT
			FireMarshalPendingClaim.claimId,
			FireMarshalPendingClaim.uniqueInstanceValue,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					FireMarshalPendingClaim.claimId,
					FireMarshalPendingClaim.uniqueInstanceValue
				ORDER BY
					CASE
						WHEN
							V_ActiveAdjuster.adjusterCompanyCode IS NOT NULL
						THEN
							0
						ELSE
							1
					END
			) AS UniqueClaimUIValue,
			FireMarshalPendingClaim.isoFileNumber,
			FireMarshalPendingClaim.reportingStatus,
			FireMarshalPendingClaim.fireMarshallStatus AS fMstatus,
			FireMarshalPendingClaim.fireMarshallDate AS fMDate,
			FireMarshalPendingClaim.claimIsOpen,
			FireMarshalPendingClaim.dateSubmittedToIso,
			FireMarshalPendingClaim.originalClaimNumber,
			FireMarshalPendingClaim.originalPolicyNumber,
			FireMarshalPendingClaim.insuranceProviderOfficeCode,
			FireMarshalPendingClaim.insuranceProviderCompanyCode,

			V_ActiveAdjuster.adjusterCompanyCode,
			V_ActiveAdjuster.adjusterCompanyName,

			FireMarshalPendingClaim.companyName,
			FireMarshalPendingClaim.affiliate1Code,
			FireMarshalPendingClaim.affiliate1Name,
			FireMarshalPendingClaim.affiliate2Code,
			FireMarshalPendingClaim.affiliate2Name,
			FireMarshalPendingClaim.groupCode,
			FireMarshalPendingClaim.groupName,
			FireMarshalPendingClaim.lossAddressLine1,
			FireMarshalPendingClaim.lossAddressLine2,
			FireMarshalPendingClaim.lossCityName,
			FireMarshalPendingClaim.lossStateCode,
			FireMarshalPendingClaim.lossStateName,
			FireMarshalPendingClaim.lossZipCode,
			FireMarshalPendingClaim.lossGeoCounty,
			FireMarshalPendingClaim.lossLatitude,
			FireMarshalPendingClaim.lossLongitude,
			FireMarshalPendingClaim.lossDescription,
			FireMarshalPendingClaim.lossDescriptionExtended,
			FireMarshalPendingClaim.dateOfLoss,
			FireMarshalPendingClaim.lossTypeCode,
			FireMarshalPendingClaim.lossTypeDescription,
			FireMarshalPendingClaim.policyTypeCode,
			FireMarshalPendingClaim.policyTypeDescription,
			FireMarshalPendingClaim.coverageTypeCode,
			FireMarshalPendingClaim.coverageTypeDescription,

			FireMarshalPendingClaim.settlementAmount,
			FireMarshalPendingClaim.estimatedLossAmount,
			FireMarshalPendingClaim.policyAmount,
			FireMarshalPendingClaim.buildingPaidAmount,
			FireMarshalPendingClaim.contentReserveAmount,
			FireMarshalPendingClaim.contentPaidAmount,
			FireMarshalPendingClaim.isIncendiaryFire,
			FireMarshalPendingClaim.isClaimUnderSIUInvestigation,
			FireMarshalPendingClaim.siuCompanyName,
			FireMarshalPendingClaim.siuRepresentativeFullName,
			FireMarshalPendingClaim.siuWorkPhoneNumber,
			FireMarshalPendingClaim.siuCellPhoneNumber,
			/*isActive*/
			FireMarshalPendingClaim.isCurrent,
			FireMarshalPendingClaim.involvedPartyId,
			FireMarshalPendingClaim.adjusterId,
			FireMarshalPendingClaim.involvedPartySequenceId
			/*dateInserted*/
	
		FROM
			dbo.FireMarshalPendingClaim
			INNER JOIN dbo.V_ActiveAdjuster
				ON FireMarshalPendingClaim.adjusterId = V_ActiveAdjuster.adjusterId
		WHERE
			FireMarshalPendingClaim.isActive = 1
	) AS DashboardAggregationByClaim
WHERE
	DashboardAggregationByClaim.UniqueClaimUIValue = 1

ROLLBACK TRANSACTION