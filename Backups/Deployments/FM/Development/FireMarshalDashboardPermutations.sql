USE ClaimSearch_Dev
--SELECT TOP 10
--	ElementalClaim.claimId,
--	COUNT(*)
--FROM
--	dbo.ElementalClaim
--GROUP BY
--	ElementalClaim.claimId
--ORDER BY
--	COUNT(*) DESC
	
	/*
SELECT
	ElementalClaim.claimId,
	COUNT(*)
FROM
	dbo.ElementalClaim
	--INNER JOIN 
	--(
	--	SELECT
	--		INNERElementalClaim.claimId,
	--		SUM(INNERElementalClaim.settlementAmount) AS settlementAmount,
	--		SUM(INNERElementalClaim.estimatedLossAmount) AS estimatedAmount
	--	FROM
	--		dbo.ElementalClaim AS INNERElementalClaim
	--	GROUP BY
	--		INNERElementalClaim.claimId
	--	HAVING
	--		SUM(INNERElementalClaim.settlementAmount) > 0
	--		OR SUM(INNERElementalClaim.estimatedLossAmount) > 0
	--) AS ElementalClaimAggregate_hasMoneySubset
	--	ON ElementalClaim.claimId = ElementalClaimAggregate_hasMoneySubset.claimId
WHERE
	ElementalClaim.elementalClaimId IN 
	(
		SELECT
			FireMarshalPendingClaim.elementalClaimId
		FROM
			dbo.FireMarshalPendingClaim
		--WHERE
		--	FireMarshalPendingClaim.lossStateCode IN ('Fl','KY')
		UNION
		SELECT
			FireMarshalClaimSendHistory.elementalClaimId
		FROM	
			dbo.FireMarshalClaimSendHistory
		--WHERE
		--	FireMarshalClaimSendHistory.lossStateCode IN ('Fl','KY')
	)
	----AND ElementalClaim.lossTypeCode IN 
	--(
	--	'LGHT',
	--	'FIRE',
	--	'EXPL'
	--)
	--AND EXISTS
	--(
	--	SELECT
	--		NULL
	--	FROM
	--		dbo.ElementalClaim AS SECONDINNERElementalClaim
	--	WHERE
	--		SECONDINNERElementalClaim.claimId = ElementalClaim.claimId
	--		AND SECONDINNERElementalClaim.lossTypeCode IN 
	--		(
	--			'LGHT',
	--			'FIRE',
	--			'EXPL'
	--		)
	--		AND SECONDINNERElementalClaim.lossTypeCode <> ElementalClaim.lossTypeCode
		
	--)
GROUP BY
	ElementalClaim.claimId
ORDER BY
	COUNT(*) DESC
	
--*/
--/*
/*
claimId	(No column name)
1945658	26
1587906	25
385818	23
555965	18
1055749	17
	claimId	(No column name)
		1977226	2
		1950739	2
		1299645	2
		1399618	2
		1610145	2
		627497	2
		1315159	2
		1370261	2
		1605475	2
		1684506	2
*/
USE ClaimSearch_Prod
SELECT * FROM dbo.FireMarshalExtractActivityLog
/*
		ones to show sandra:	1855153 - fire & comp
								1977226 - fire & light KY, 1$			suggestion:	Order by $
								1114956 - fire & lightning non KY		suggestion:	prob sum fire$, filter out Lght
								1950739 - fire, light, laib KY, 1$	
								1945658 - 26x losstype, only 1 fire
								
								1855153 - fire & comp					suggestion:	prob sum fire$, filter out comp
								1005896 - fire & 8xprop					suggestion:	prob sum fire$, filter out prop
								1405128 - 10xFire Exception				suggestion:	prob sum fire$
								2044641 - 5x Fire Pending				suggestion:	prob sum fire$
								1855153	- 4x Fire 1 comp				suggestion:	prob sum fire$, filter out comp
--*/
--/*
SELECT * FROM 
dbo.ElementalClaim
WHERE
lossTypeCode IS NULL
OR coverageTypeCode IS NULL
DECLARE @elementalClaimIdToInvestigate BIGINT = 1855153;
SELECT
	NULL AS [Elemental Claim: ],
	InvolvedPartyAddressMap.claimId,
	*
FROM 
	dbo.ElementalClaim
	INNER JOIN dbo.InvolvedPartyAddressMap
		ON InvolvedPartyAddressMap.involvedPartyId = ElementalClaim.involvedPartyId
		--AND InvolvedPartyAddressMap.claimId =ElementalClaim.claimId
WHERE
	ElementalClaim.claimId = @elementalClaimIdToInvestigate
SELECT
	NULL AS [pending\Exception: ],*
FROM
	dbo.FireMarshalPendingClaim
WHERE
	FireMarshalPendingClaim.claimId = @elementalClaimIdToInvestigate
SELECT
		NULL AS [passive\Sent: ],*
FROM	
	dbo.FireMarshalClaimSendHistory
WHERE
	FireMarshalClaimSendHistory.claimId = @elementalClaimIdToInvestigate
--0Z004894704
--1D003801052
SELECT
	NULL AS [Dashboard Preview: ],
	isoFileNumber,
	reportingStatus,
	REPLACE(fMstatus,'Pending ','') AS [Fire Marshal Status],
	fMDate AS [Fire Marshal Date],
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
	lossZipCode,
	lossGeoCounty,
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
	siuCellPhoneNumber
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
			AND FireMarshalPendingClaim.claimId = @elementalClaimIdToInvestigate
	) AS DashboardAggregationByClaim
WHERE
	DashboardAggregationByClaim.UniqueClaimUIValue = 1	
UNION
SELECT
	NULL AS [Dashboard Preview: ],
	isoFileNumber,
	reportingStatus,
	REPLACE(fMstatus,'Pending ','') AS [Fire Marshal Status],
	fMDate AS [Fire Marshal Date],
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
	lossZipCode,
	lossGeoCounty,
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
	siuCellPhoneNumber
FROM	
	(
		SELECT
			FireMarshalClaimSendHistory.claimId,
			FireMarshalClaimSendHistory.uniqueInstanceValue,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					FireMarshalClaimSendHistory.claimId,
					FireMarshalClaimSendHistory.uniqueInstanceValue
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
			FireMarshalClaimSendHistory.isoFileNumber,
			FireMarshalClaimSendHistory.reportingStatus,
			FireMarshalClaimSendHistory.fireMarshallStatus AS fMstatus,
			FireMarshalClaimSendHistory.fireMarshallDate AS fMDate,
			FireMarshalClaimSendHistory.claimIsOpen,
			FireMarshalClaimSendHistory.dateSubmittedToIso,
			FireMarshalClaimSendHistory.originalClaimNumber,
			FireMarshalClaimSendHistory.originalPolicyNumber,
			FireMarshalClaimSendHistory.insuranceProviderOfficeCode,
			FireMarshalClaimSendHistory.insuranceProviderCompanyCode,

			V_ActiveAdjuster.adjusterCompanyCode,
			V_ActiveAdjuster.adjusterCompanyName,

			FireMarshalClaimSendHistory.companyName,
			FireMarshalClaimSendHistory.affiliate1Code,
			FireMarshalClaimSendHistory.affiliate1Name,
			FireMarshalClaimSendHistory.affiliate2Code,
			FireMarshalClaimSendHistory.affiliate2Name,
			FireMarshalClaimSendHistory.groupCode,
			FireMarshalClaimSendHistory.groupName,
			FireMarshalClaimSendHistory.lossAddressLine1,
			FireMarshalClaimSendHistory.lossAddressLine2,
			FireMarshalClaimSendHistory.lossCityName,
			FireMarshalClaimSendHistory.lossStateCode,
			FireMarshalClaimSendHistory.lossStateName,
			FireMarshalClaimSendHistory.lossZipCode,
			FireMarshalClaimSendHistory.lossGeoCounty,
			FireMarshalClaimSendHistory.lossLatitude,
			FireMarshalClaimSendHistory.lossLongitude,
			FireMarshalClaimSendHistory.lossDescription,
			FireMarshalClaimSendHistory.lossDescriptionExtended,
			FireMarshalClaimSendHistory.dateOfLoss,
			FireMarshalClaimSendHistory.lossTypeCode,
			FireMarshalClaimSendHistory.lossTypeDescription,
			FireMarshalClaimSendHistory.policyTypeCode,
			FireMarshalClaimSendHistory.policyTypeDescription,
			FireMarshalClaimSendHistory.coverageTypeCode,
			FireMarshalClaimSendHistory.coverageTypeDescription,

			FireMarshalClaimSendHistory.settlementAmount,
			FireMarshalClaimSendHistory.estimatedLossAmount,
			FireMarshalClaimSendHistory.policyAmount,
			FireMarshalClaimSendHistory.buildingPaidAmount,
			FireMarshalClaimSendHistory.contentReserveAmount,
			FireMarshalClaimSendHistory.contentPaidAmount,
			FireMarshalClaimSendHistory.isIncendiaryFire,
			FireMarshalClaimSendHistory.isClaimUnderSIUInvestigation,
			FireMarshalClaimSendHistory.siuCompanyName,
			FireMarshalClaimSendHistory.siuRepresentativeFullName,
			FireMarshalClaimSendHistory.siuWorkPhoneNumber,
			FireMarshalClaimSendHistory.siuCellPhoneNumber,
			/*isActive*/
			1 AS isCurrent,
			FireMarshalClaimSendHistory.involvedPartyId,
			FireMarshalClaimSendHistory.adjusterId,
			FireMarshalClaimSendHistory.involvedPartySequenceId
			/*dateInserted*/
	
		FROM
			dbo.FireMarshalClaimSendHistory
			INNER JOIN dbo.V_ActiveAdjuster
				ON FireMarshalClaimSendHistory.adjusterId = V_ActiveAdjuster.adjusterId
		WHERE
			FireMarshalClaimSendHistory.isActive = 1
			AND FireMarshalClaimSendHistory.claimId = @elementalClaimIdToInvestigate
	) AS DashboardAggregationByClaim
WHERE
	DashboardAggregationByClaim.UniqueClaimUIValue = 1	
	--*/