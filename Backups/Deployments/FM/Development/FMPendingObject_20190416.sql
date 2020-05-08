USE ClaimSearch_Dev

--DECLARE @dateInserted AS 
SELECT
	V_ActiveElementalClaim.elementalClaimId,
	CAST(ExistingPendingClaim.uniqueInstanceValue AS TINYINT) AS uniqueInstanceValue, 
	V_ActiveClaim.claimId,
	V_ActiveClaim.isoClaimId AS isoFileNumber,
	/*reportingStatus,*/
	/*fmStatus,*/
	/*fireMarshallDate*/
	FireMarshalController.projectedGenerationDate,
	/*claimIsOpen,*/
	V_ActiveClaim.systemDateReceived AS dateSubmittedToIso,
	V_ActiveClaim.originalClaimNumber,
	V_ActivePolicy.originalPolicyNumber,
	V_ActivePolicy.insuranceProviderOfficeCode,
	V_ActivePolicy.insuranceProviderCompanyCode,
	CompanyHeirarchy.Customer_lvl0 + ' (' + V_ActivePolicy.insuranceProviderCompanyCode + ')' AS companyName,
	CompanyHeirarchy.lvl1 AS affiliate1Code,
	CompanyHeirarchy.Customer_lvl1 + ' (' + CompanyHeirarchy.lvl1 + ')' AS affiliate1Name,
	CompanyHeirarchy.lvl2 AS affiliate2Code,
	CompanyHeirarchy.Customer_lvl2 + ' (' + CompanyHeirarchy.lvl2 + ')' AS affiliate2Name,
	CompanyHeirarchy.lvl3 AS groupCode,
	CompanyHeirarchy.Customer_lvl3 + ' (' + CompanyHeirarchy.lvl3 + ')' AS groupDisplayName,
	
	V_ActiveLocationOfLoss.originalAddressLine1 AS lossAddressLine1,
	V_ActiveLocationOfLoss.originalAddressLine2 AS lossAddressLine2,
	V_ActiveLocationOfLoss.originalCityName AS lossCityName,
	V_ActiveLocationOfLoss.originalStateCode AS lossStateCode,
	Lookup_States.State_Name AS lossStateName,
	V_ActiveLocationOfLoss.originalZipCode AS lossZipCode,
	V_ActiveLocationOfLoss.latitude AS lossLatitude,
	V_ActiveLocationOfLoss.longitude AS lossLongitude,
	V_ActiveClaim.lossDescription,
	V_ActiveClaim.lossDescriptionExtended,
	V_ActiveClaim.dateOfLoss,
	
	V_ActiveElementalClaim.lossTypeCode,
	V_ActiveElementalClaim.lossTypeDescription,
	V_ActivePolicy.policyTypeCode,
	V_ActivePolicy.policyTypeDescription,
	V_ActiveElementalClaim.coverageTypeCode,
	V_ActiveElementalClaim.coverageTypeDescription,

	V_ActiveElementalClaim.estimatedLossAmount,
	V_ActiveElementalClaim.settlementAmount,
	V_ActiveElementalClaim.policyAmount,
	
	CASE
		WHEN
			ISNULL(FlagDuplicateRemovalPartition.F_INCEND_FIRE,'0') = 'Y'
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS isIncendiaryFire,
	
	V_ActiveElementalClaim.reserveAmount,
	V_ActiveElementalClaim.totalInsuredAmount,
	V_ActiveElementalClaim.replacementAmount,
	
	V_ActiveElementalClaim.actualCashAmount,
	V_ActiveElementalClaim.buildingPolicyAmount,
	V_ActiveElementalClaim.buildingTotalInsuredAmount,
	V_ActiveElementalClaim.buildingReplacementAmount,
	V_ActiveElementalClaim.buildingActualCashAmount,
	V_ActiveElementalClaim.buildingEstimatedLossAmount,
	V_ActiveElementalClaim.contentPolicyAmount,
	V_ActiveElementalClaim.contentTotalInsuredAmount,
	V_ActiveElementalClaim.contentReplacementAmount,
	V_ActiveElementalClaim.contentActualCashAmount,
	V_ActiveElementalClaim.contentEstimatedLossAmount,
	V_ActiveElementalClaim.stockPolicyAmount,
	V_ActiveElementalClaim.stockTotalInsuredAmount,
	V_ActiveElementalClaim.stockReplacementAmount,
	V_ActiveElementalClaim.stockActualCashAmount,
	V_ActiveElementalClaim.stockEstimatedLossAmount,
	V_ActiveElementalClaim.lossOfUsePolicyAmount,
	V_ActiveElementalClaim.lossOfUseTotalInsuredAmount,
	V_ActiveElementalClaim.lossOfUseReplacementAmount,
	V_ActiveElementalClaim.lossOfUseActualCashAmount,
	V_ActiveElementalClaim.lossOfUseEstimatedLossAmount,
	V_ActiveElementalClaim.otherPolicyAmount,
	V_ActiveElementalClaim.otherTotalInsuredAmount,
	V_ActiveElementalClaim.otherReplacementAmount,
	V_ActiveElementalClaim.otherActualCashAmount,
	V_ActiveElementalClaim.otherEstimatedLossAmount,
	V_ActiveElementalClaim.buildingReserveAmount,
	V_ActiveElementalClaim.buildingPaidAmount,
	V_ActiveElementalClaim.contentReserveAmount,
	V_ActiveElementalClaim.contentPaidAmount,
	V_ActiveElementalClaim.stockReserveAmount,
	V_ActiveElementalClaim.stockPaidAmount,
	V_ActiveElementalClaim.lossOfUseReserve,
	V_ActiveElementalClaim.lossOfUsePaid,
	V_ActiveElementalClaim.otherReserveAmount,
	V_ActiveElementalClaim.otherPaidAmount,
	
	V_ActiveClaim.isClaimUnderSIUInvestigation,
	V_ActiveClaim.siuCompanyName,
	V_ActiveClaim.siuRepresentativeFullName,
	V_ActiveClaim.siuWorkPhoneNumber,
	V_ActiveClaim.siuCellPhoneNumber,
	
	V_ActiveElementalClaim.involvedPartyId,
	V_ActiveElementalClaim.adjusterId,
	V_ActiveElementalClaim.involvedPartySequenceId

	/*isActive*/
	/*isCurrent*/
	/*dateInserted*/
	INTO #PendingFMClaimDataToInsert
FROM
	dbo.FireMarshalController
	INNER JOIN [ClaimSearch_Prod].dbo.Lookup_States WITH (NOLOCK)
		ON FireMarshalController.fmStateStatusCode = Lookup_States.State_Abb
	INNER JOIN dbo.V_ActiveLocationOfLoss WITH (NOLOCK)
		ON FireMarshalController.fmStateStatusCode = V_ActiveLocationOfLoss.originalStateCode
	INNER JOIN dbo.V_ActiveClaim WITH (NOLOCK)
		ON V_ActiveLocationOfLoss.addressId = V_ActiveClaim.locationOfLossAddressId
	INNER JOIN dbo.V_ActivePolicy WITH (NOLOCK)
		ON V_ActiveClaim.policyId = V_ActivePolicy.policyId
	INNER JOIN dbo.V_ActiveElementalClaim WITH (NOLOCK)
		ON V_ActiveClaim.claimId = V_ActiveElementalClaim.claimId
	INNER JOIN ClaimSearch_Prod.dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH (NOLOCK)
		ON V_ActivePolicy.insuranceProviderCompanyCode = CompanyHeirarchy.lvl0
	LEFT OUTER JOIN (
		SELECT
			CLT00018.I_ALLCLM,
			CLT00018.F_INCEND_FIRE,
			ROW_NUMBER() OVER(
				PARTITION BY
						CLT00018.I_ALLCLM
				ORDER BY
						CLT00018.Date_Insert DESC
			) AS incendiaryFireUniqueInstanceValue
		FROM
			dbo.FireMarshalDriver
			INNER JOIN [ClaimSearch_Prod].dbo.CLT00018
				ON FireMarshalDriver.isoClaimId = CLT00018.I_ALLCLM
	) FlagDuplicateRemovalPartition
		ON V_ActiveClaim.isoClaimId = FlagDuplicateRemovalPartition.I_ALLCLM
	/*DevNote: The following LO-join against the FireMarshalClaimSendHistory object
		is paired with an IS-NULL filter; this is to ensure that duplicate claim-
		-representation	on the dashboard is prevented*/
	--LEFT OUTER JOIN dbo.FireMarshalClaimSendHistory
	--	ON V_ActiveClaim.claimId = FireMarshalClaimSendHistory.claimId
		/*fire indicator, from 18, join on I_ALLCLM, colName=F_INCEND_FIRE*/
	LEFT OUTER JOIN dbo.V_ActiveCurrentPendingFMClaim AS ExistingPendingClaim
		ON V_ActiveElementalClaim.elementalClaimId = ExistingPendingClaim.elementalClaimId
WHERE
	FlagDuplicateRemovalPartition.incendiaryFireUniqueInstanceValue = 1
	--AND FireMarshalClaimSendHistory.elementalClaimId IS NULL
	