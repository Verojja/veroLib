SET NOEXEC OFF;
--USE ClaimSearch_Dev
BEGIN TRANSACTION


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
	@stepExecutionNotes VARCHAR(1000),
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0;

SELECT
	@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
	(
		COALESCE
		(
			@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
			MAX(FMPendingClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
			CAST('2014-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.FMPendingClaimActivityLog
WHERE
	FMPendingClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND FMPendingClaimActivityLog.isSuccessful = 1;
SET @sourceDateTime = @dateFilterParam;

--UPDATE dbo.FireMarshalController
--	SET
--		FireMarshalController.endDate = @dateInserted	
--	OUTPUT
--		DELETED.fmStateCode,
--		DELETED.fmQualificationRequirmentSetId,
--		DELETED.fmStateStatusCode,
--		DELETED.frequencyCode,
--		DATEADD(
--			MONTH,
--			ProjectedDateToIncrementApply.projectedDateIncrementValue,
--			COALESCE(PreviousDate.mostRecentGenerationDate, DELETED.projectedGenerationDate)
--		) /*new projectedGenerationDate*/,
--		DELETED.receivesPrint,
--		DELETED.receivesFTP,
--		DELETED.receivesEmail,
--		DELETED.fmContactFirstName,
--		DELETED.fmContactMiddleName,
--		DELETED.fmContactLastName,
--		DELETED.fmContactSuffixName,
--		DELETED.fmContactDeptartmentName,
--		DELETED.fmContactDivisionName,
--		DELETED.fmContactDeliveryAddressLine1,
--		DELETED.fmContactDeliveryAddressLine2,
--		DELETED.fmContactDeliveryCity,
--		DELETED.fmContactDeliveryStateCode,
--		DELETED.fmContactZipCode,
--		DELETED.fmContactTitleName,
--		DELETED.fmContactSalutation,
--		@dateInserted,
--		DELETED.endDate
--	INTO dbo.FireMarshalController
--	(
--		fmStateCode,
--		fmQualificationRequirmentSetId,
--		fmStateStatusCode,
--		frequencyCode,
--		projectedGenerationDate,
--		receivesPrint,
--		receivesFTP,
--		receivesEmail,
--		fmContactFirstName,
--		fmContactMiddleName,
--		fmContactLastName,
--		fmContactSuffixName,
--		fmContactDeptartmentName,
--		fmContactDivisionName,
--		fmContactDeliveryAddressLine1,
--		fmContactDeliveryAddressLine2,
--		fmContactDeliveryCity,
--		fmContactDeliveryStateCode,
--		fmContactZipCode,
--		fmContactTitleName,
--		fmContactSalutation,
--		dateInserted,
--		endDate
--	)
--FROM
--	dbo.FireMarshalController
--	CROSS APPLY 
--	(
--		SELECT /*BehaviorLogic for daily\weekly\yearly NOT YET IMPLIMENTED*/
--			CASE
--				WHEN
--					FireMarshalController.frequencyCode = 'Q'
--					AND MONTH(FireMarshalController.projectedGenerationDate) < 4 
--				THEN
--					3
--				WHEN
--					FireMarshalController.frequencyCode = 'Q'
--				THEN
--					4
--				WHEN
--					FireMarshalController.frequencyCode = 'M'
--				THEN
--					1
--				ELSE
--					1
--			END AS projectedDateIncrementValue
--	) AS ProjectedDateToIncrementApply
--	LEFT OUTER JOIN (
--		SELECT
--			FireMarshalClaimSendHistory.lossStateCode AS fmStateCode,
--			MAX(FireMarshalClaimSendHistory.fireMarshallDate) AS mostRecentGenerationDate
--		FROM
--			dbo.FireMarshalClaimSendHistory
--		GROUP BY
--			FireMarshalClaimSendHistory.lossStateCode
--	) PreviousDate
--		ON FireMarshalController.fmStateCode = PreviousDate.fmStateCode
--WHERE
--	FireMarshalController.endDate IS NULL
--	AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< @dateInserted
--	AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('00010101' AS DATE)) <> DATEADD(
--		MONTH,
--		ProjectedDateToIncrementApply.projectedDateIncrementValue,
--		COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
--	);


SELECT

--COUNT(*)
--/*

	V_ActiveElementalClaim.elementalClaimId,
	CAST(ISNULL(ExistingPendingClaim.uniqueInstanceValue,1) AS TINYINT) AS uniqueInstanceValue, 
	V_ActiveClaim.claimId,
	V_ActiveClaim.isoClaimId AS isoFileNumber,
	CAST('Pending' AS VARCHAR(25)) AS reportingStatus,
	CAST('Pending ' + DATENAME(MONTH, FireMarshalController.projectedGenerationDate) AS VARCHAR(255))AS fireMarshallStatus,
	/*fireMarshallDate*/
	FireMarshalController.projectedGenerationDate,
	CASE
		WHEN
			COALESCE(ClaimOpenCloseAggregation.instancesOfClosedClaim,0) = 0
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS claimIsOpen,
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
	CompanyHeirarchy.Customer_lvl3 + ' (' + CompanyHeirarchy.lvl3 + ')' AS groupName,
	
	V_ActiveLocationOfLoss.originalAddressLine1 AS lossAddressLine1,
	V_ActiveLocationOfLoss.originalAddressLine2 AS lossAddressLine2,
	V_ActiveLocationOfLoss.originalCityName AS lossCityName,
	V_ActiveLocationOfLoss.originalStateCode AS lossStateCode,
	Lookup_States.State_Name AS lossStateName,
	V_ActiveLocationOfLoss.originalZipCode AS lossZipCode,
	V_ActiveLocationOfLoss.scrubbedCountyName AS lossGeoCounty,
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
	
	V_ActiveElementalClaim.buildingPaidAmount,
	V_ActiveElementalClaim.contentReserveAmount,
	V_ActiveElementalClaim.contentPaidAmount,
	CASE
		WHEN
			ISNULL(DuplicateRemovalFlagPartition.F_INCEND_FIRE,'0') = 'Y'
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS isIncendiaryFire,
	
	V_ActiveClaim.isClaimUnderSIUInvestigation,
	V_ActiveClaim.siuCompanyName,
	V_ActiveClaim.siuRepresentativeFullName,
	V_ActiveClaim.siuWorkPhoneNumber,
	V_ActiveClaim.siuCellPhoneNumber,
	
	V_ActiveElementalClaim.involvedPartyId,
	V_ActiveElementalClaim.adjusterId,
	V_ActiveElementalClaim.involvedPartySequenceId,
	
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

	V_ActiveElementalClaim.stockReserveAmount,
	V_ActiveElementalClaim.stockPaidAmount,
	V_ActiveElementalClaim.lossOfUseReserve,
	V_ActiveElementalClaim.lossOfUsePaid,
	V_ActiveElementalClaim.otherReserveAmount,
	V_ActiveElementalClaim.otherPaidAmount



	/*isActive*/
	/*isCurrent*/
	/*dateInserted*/
--*/
	INTO #PendingFMClaimDataToInsert
FROM
	dbo.FireMarshalController
	INNER JOIN [ClaimSearch_Prod].dbo.Lookup_States WITH (NOLOCK)
		ON FireMarshalController.fmStateCode = Lookup_States.State_Abb
	INNER JOIN dbo.V_ActiveLocationOfLoss WITH (NOLOCK)
		ON FireMarshalController.fmStateCode = V_ActiveLocationOfLoss.originalStateCode
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
			INNER JOIN [ClaimSearch_Prod].dbo.CLT00018 WITH (NOLOCK)
				ON FireMarshalDriver.isoClaimId = CLT00018.I_ALLCLM
	) DuplicateRemovalFlagPartition
		ON V_ActiveClaim.isoClaimId = DuplicateRemovalFlagPartition.I_ALLCLM
	LEFT OUTER JOIN(
		SELECT
			InnerActiveElementalClaim.claimId,
			SUM
			(
				CASE
					WHEN
						InnerActiveElementalClaim.dateClaimClosed IS NULL
					THEN
						0
					ELSE
						1
				END
			) AS instancesOfClosedClaim
		FROM
			dbo.V_ActiveElementalClaim AS InnerActiveElementalClaim WITH (NOLOCK)
		GROUP BY
			InnerActiveElementalClaim.claimId
	) AS ClaimOpenCloseAggregation
		ON V_ActiveElementalClaim.claimId = ClaimOpenCloseAggregation.claimId 
	/*DevNote: The following LO-join against the FireMarshalClaimSendHistory object
		is paired with an IS-NULL filter; this is to ensure that duplicate claim-
		-representation	on the dashboard is prevented*/
	LEFT OUTER JOIN dbo.FireMarshalClaimSendHistory
		ON V_ActiveElementalClaim.elementalClaimId = FireMarshalClaimSendHistory.claimId
		/*fire indicator, from 18, join on I_ALLCLM, colName=F_INCEND_FIRE*/
	LEFT OUTER JOIN dbo.V_ActiveCurrentPendingFMClaim AS ExistingPendingClaim
		ON V_ActiveElementalClaim.elementalClaimId = ExistingPendingClaim.elementalClaimId
WHERE
	FireMarshalController.endDate IS NULL
	AND FireMarshalController.fmStateStatusCode = 'A'
	AND FireMarshalController.projectedGenerationDate IS NOT NULL
	AND FireMarshalClaimSendHistory.elementalClaimId IS NULL
	AND ISNULL(DuplicateRemovalFlagPartition.incendiaryFireUniqueInstanceValue,1) = 1
	AND V_ActiveElementalClaim.elementalClaimId IN
	(
		--50149,
		--30666,
		--31466,
		
		--222,
		--3601,
		90903,
		90903,
		92103,
		92103,
		93617,
		93617,
		93818,
		93818,
		95560,
		95560,
		96511,
		96511,
		99822,
		99822,
		72295,
		72331
	)

--SELECT TOP 3 * FROM #PendingFMClaimDataToInsert

--SELECT
--	*
--FROM
--	#PendingFMClaimDataToInsert
 
/*Proper logic for FMClaimQualificationRequirementSetBehavior is NOT FULLY IMPLIMENTED;
	currently only works for 'GA' and 'KS'
*/
SELECT
	ValidatedFMStatus.elementalClaimId,
	ValidatedFMStatus.uniqueInstanceValue,
	CASE
		WHEN
			LEFT(ValidatedFMStatus.fireMarshallStatusValue,7) = 'Pending'
		THEN
			'Pending'
		ELSE
			'Exception'
	END  AS reportingStatus,
	ValidatedFMStatus.fireMarshallStatusValue AS fireMarshallStatus
	INTO #PendingClaimWithException
FROM
	(
		SELECT
			InnerPendingFMClaimDataToInsert.elementalClaimid,
			InnerPendingFMClaimDataToInsert.uniqueInstanceValue,
			InnerPendingFMClaimDataToInsert.lossStateCode,
			CASE
				WHEN 
					InnerPendingFMClaimDataToInsert.lossStateCode = 'GA'
				THEN
					CASE
						WHEN
							InnerPendingFMClaimDataToInsert.claimIsOpen = 1
						THEN
							CASE
								WHEN
									ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) = 0
								THEN
									CAST('Settlement amount missing' AS VARCHAR(255))
								ELSE
									InnerPendingFMClaimDataToInsert.fireMarshallStatus	
							END
						ELSE
							InnerPendingFMClaimDataToInsert.fireMarshallStatus
					END
				WHEN
					InnerPendingFMClaimDataToInsert.lossStateCode = 'KS'
				THEN
					CASE
						WHEN
							ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescription)),'Fire') IN (
								'Fire',
								'',
								'blank'
							)
							AND ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescriptionExtended)),'Fire') IN (
								'Fire',
								'',
								'blank'
							)
							AND (
								ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
								AND ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
							)
						THEN
							CAST('Loss description invalid and Estimated and/or Settlement amount missing' AS VARCHAR(255))
						WHEN
							ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescription)),'Fire') IN (
								'Fire',
								'',
								'blank'
							)
							AND ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescriptionExtended)),'Fire') IN (
								'Fire',
								'',
								'blank'
							)
						THEN
							CAST('Loss description invalid' AS VARCHAR(255))
						WHEN
							ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
							AND ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
						THEN
							CAST('Estimated and/or Settlement amount missing' AS VARCHAR(255))
						ELSE
							InnerPendingFMClaimDataToInsert.fireMarshallStatus	
					END
				ELSE
					/*'Status temporarily unavailable, pending Development Update'*/
					InnerPendingFMClaimDataToInsert.fireMarshallStatus
			END AS fireMarshallStatusValue
		FROM
			#PendingFMClaimDataToInsert AS InnerPendingFMClaimDataToInsert
	) AS ValidatedFMStatus
	INNER JOIN dbo.FireMarshalController
		ON ValidatedFMStatus.lossStateCode = FireMarshalController.fmStateCode
WHERE
	FireMarshalController.endDate IS NULL
	AND FireMarshalController.fmStateStatusCode = 'A'
	AND FireMarshalController.projectedGenerationDate IS NOT NULL
	AND FireMarshalController.fmQualificationRequirmentSetId NOT IN
	(
		0,4
	);

SELECT #PendingClaimWithException.elementalClaimId,*
FROM #PendingClaimWithException
WHERE #PendingClaimWithException.reportingStatus = 'Exception'

SELECT #PendingFMClaimDataToInsert.elementalClaimId,*
FROM #PendingFMClaimDataToInsert
WHERE
#PendingFMClaimDataToInsert.elementalClaimId NOT IN
(
	SELECT
		#PendingClaimWithException.elementalClaimId
	FROM #PendingClaimWithException
)

SELECT *
FROM #PendingFMClaimDataToInsert
--/*
UPDATE #PendingFMClaimDataToInsert
	SET
		#PendingFMClaimDataToInsert.reportingStatus = #PendingClaimWithException.reportingStatus,
		#PendingFMClaimDataToInsert.fireMarshallStatus = #PendingClaimWithException.fireMarshallStatus
FROM
	#PendingClaimWithException
WHERE
	#PendingFMClaimDataToInsert.elementalClaimId = #PendingClaimWithException.elementalClaimId
	AND #PendingFMClaimDataToInsert.uniqueInstanceValue = #PendingClaimWithException.uniqueInstanceValue
	AND #PendingFMClaimDataToInsert.fireMarshallStatus <> ISNULL(#PendingClaimWithException.fireMarshallStatus,'~~~~')
	
SELECT *
FROM #PendingFMClaimDataToInsert

SELECT NULL AS [pre ''CRUD'' sanity check.],
*
FROM dbo.FireMarshalPendingClaim	
	WHERE elementalClaimId IN
	(
		--50149,
		--30666,
		--31466,
		
		--222,
		--3601,
		90903,
		90903,
		92103,
		92103,
		93617,
		93617,
		93818,
		93818,
		95560,
		95560,
		96511,
		96511,
		99822,
		99822,
		72295,
		72331
	)

/*Update Existing*/
UPDATE dbo.FireMarshalPendingClaim
SET
	FireMarshalPendingClaim.isCurrent = 0
OUTPUT
	SOURCE.elementalClaimId,
	SOURCE.uniqueInstanceValue+1, /*incriment the uniqueInstanceValue*/
	SOURCE.claimId,
	SOURCE.isoFileNumber,
	SOURCE.reportingStatus,
	SOURCE.fireMarshallStatus,
	@dateInserted AS fireMarshallDate,
	SOURCE.claimIsOpen,
	SOURCE.dateSubmittedToIso,
	SOURCE.originalClaimNumber,
	SOURCE.originalPolicyNumber,
	SOURCE.insuranceProviderOfficeCode,
	SOURCE.insuranceProviderCompanyCode,
	SOURCE.companyName,
	SOURCE.affiliate1Code,
	SOURCE.affiliate1Name,
	SOURCE.affiliate2Code,
	SOURCE.affiliate2Name,
	SOURCE.groupCode,
	SOURCE.groupName,
	SOURCE.lossAddressLine1,
	SOURCE.lossAddressLine2,
	SOURCE.lossCityName,
	SOURCE.lossStateCode,
	SOURCE.lossStateName,
	SOURCE.lossZipCode,
	SOURCE.lossGeoCounty,
	SOURCE.lossLatitude,
	SOURCE.lossLongitude,
	SOURCE.lossDescription,
	SOURCE.lossDescriptionExtended,
	SOURCE.dateOfLoss,
	SOURCE.lossTypeCode,
	SOURCE.lossTypeDescription,
	SOURCE.policyTypeCode,
	SOURCE.policyTypeDescription,
	SOURCE.coverageTypeCode,
	SOURCE.coverageTypeDescription,
	SOURCE.estimatedLossAmount,
	SOURCE.settlementAmount,
	SOURCE.policyAmount,
	SOURCE.buildingPaidAmount,
	SOURCE.contentReserveAmount,
	SOURCE.contentPaidAmount,
	SOURCE.isIncendiaryFire,
	SOURCE.isClaimUnderSIUInvestigation,
	SOURCE.siuCompanyName,
	SOURCE.siuRepresentativeFullName,
	SOURCE.siuWorkPhoneNumber,
	SOURCE.siuCellPhoneNumber,
	SOURCE.involvedPartyId,
	SOURCE.adjusterId,
	SOURCE.involvedPartySequenceId,
	SOURCE.reserveAmount,
	SOURCE.totalInsuredAmount,
	SOURCE.replacementAmount,
	SOURCE.actualCashAmount,
	SOURCE.buildingPolicyAmount,
	SOURCE.buildingTotalInsuredAmount,
	SOURCE.buildingReplacementAmount,
	SOURCE.buildingActualCashAmount,
	SOURCE.buildingEstimatedLossAmount,
	SOURCE.contentPolicyAmount,
	SOURCE.contentTotalInsuredAmount,
	SOURCE.contentReplacementAmount,
	SOURCE.contentActualCashAmount,
	SOURCE.contentEstimatedLossAmount,
	SOURCE.stockPolicyAmount,
	SOURCE.stockTotalInsuredAmount,
	SOURCE.stockReplacementAmount,
	SOURCE.stockActualCashAmount,
	SOURCE.stockEstimatedLossAmount,
	SOURCE.lossOfUsePolicyAmount,
	SOURCE.lossOfUseTotalInsuredAmount,
	SOURCE.lossOfUseReplacementAmount,
	SOURCE.lossOfUseActualCashAmount,
	SOURCE.lossOfUseEstimatedLossAmount,
	SOURCE.otherPolicyAmount,
	SOURCE.otherTotalInsuredAmount,
	SOURCE.otherReplacementAmount,
	SOURCE.otherActualCashAmount,
	SOURCE.otherEstimatedLossAmount,
	SOURCE.buildingReserveAmount,
	SOURCE.stockReserveAmount,
	SOURCE.stockPaidAmount,
	SOURCE.lossOfUseReserve,
	SOURCE.lossOfUsePaid,
	SOURCE.otherReserveAmount,
	SOURCE.otherPaidAmount,
	1 AS isActive,
	1 AS isCurrent,
	@dateInserted AS dateInserted
	INTO dbo.FireMarshalPendingClaim
FROM
	#PendingFMClaimDataToInsert AS SOURCE
WHERE
	FireMarshalPendingClaim.elementalClaimId = SOURCE.elementalClaimId
		AND Source.uniqueInstanceValue = FireMarshalPendingClaim.uniqueInstanceValue
	AND
	(
		FireMarshalPendingClaim.claimId <> SOURCE.claimId
		OR ISNULL(FireMarshalPendingClaim.isoFileNumber,'~~~') <> ISNULL(SOURCE.isoFileNumber,'~~~')
		OR ISNULL(FireMarshalPendingClaim.reportingStatus,'~~~') <> ISNULL(SOURCE.reportingStatus,'~~~')
		OR ISNULL(FireMarshalPendingClaim.fireMarshallStatus,'~~~') <> ISNULL(SOURCE.fireMarshallStatus,'~~~')
		OR  FireMarshalPendingClaim.claimIsOpen <> SOURCE.claimIsOpen
		OR CAST(ISNULL(FireMarshalPendingClaim.dateSubmittedToIso,'99990101')AS DATE) <> CAST(ISNULL(SOURCE.dateSubmittedToIso,'99990101') AS DATE)
		OR ISNULL(FireMarshalPendingClaim.originalClaimNumber,'~~~') <> ISNULL(SOURCE.originalClaimNumber,'~~~')
		OR FireMarshalPendingClaim.originalPolicyNumber <> SOURCE.originalPolicyNumber
		OR FireMarshalPendingClaim.insuranceProviderOfficeCode <> SOURCE.insuranceProviderOfficeCode
		OR FireMarshalPendingClaim.insuranceProviderCompanyCode <> SOURCE.insuranceProviderCompanyCode
		OR FireMarshalPendingClaim.companyName <> SOURCE.companyName
		OR FireMarshalPendingClaim.affiliate1Code <> SOURCE.affiliate1Code
		OR FireMarshalPendingClaim.affiliate1Name <> SOURCE.affiliate1Name
		OR FireMarshalPendingClaim.affiliate2Code <> SOURCE.affiliate2Code
		OR FireMarshalPendingClaim.affiliate2Name <> SOURCE.affiliate2Name
		OR FireMarshalPendingClaim.groupCode <> SOURCE.groupCode
		OR FireMarshalPendingClaim.groupName <> SOURCE.groupName
		OR ISNULL(FireMarshalPendingClaim.lossAddressLine1,'~~~') <> ISNULL(SOURCE.lossAddressLine1,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossAddressLine2,'~~~') <> ISNULL(SOURCE.lossAddressLine2,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossCityName,'~~~') <> ISNULL(SOURCE.lossCityName,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossStateCode,'~~') <> ISNULL(SOURCE.lossStateCode,'~~')
		OR ISNULL(FireMarshalPendingClaim.lossStateName,'~~~') <> ISNULL(SOURCE.lossStateName,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossZipCode,'~~~~~') <> ISNULL(SOURCE.lossZipCode,'~~~~~')
		OR ISNULL(FireMarshalPendingClaim.lossGeoCounty,'~~~') <> ISNULL(SOURCE.lossGeoCounty,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossLatitude,'~~~') <> ISNULL(SOURCE.lossLatitude,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossLongitude,'~~~') <> ISNULL(SOURCE.lossLongitude,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossDescription,'~~~') <> ISNULL(SOURCE.lossDescription,'~~~')
		OR ISNULL(FireMarshalPendingClaim.lossDescriptionExtended,'~~~') <> ISNULL(SOURCE.lossDescriptionExtended,'~~~')
		OR CAST(ISNULL(FireMarshalPendingClaim.dateOfLoss,'99990101') AS DATE) <> CAST(ISNULL(SOURCE.dateOfLoss,'99990101') AS DATE)
		OR ISNULL(FireMarshalPendingClaim.lossTypeCode,'~~~~') <> ISNULL(SOURCE.lossTypeCode,'~~~~')
		OR ISNULL(FireMarshalPendingClaim.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
		OR ISNULL(FireMarshalPendingClaim.policyTypeCode,'~~~~') <> ISNULL(SOURCE.policyTypeCode,'~~~~')
		OR ISNULL(FireMarshalPendingClaim.policyTypeDescription,'~~~') <> ISNULL(SOURCE.policyTypeDescription,'~~~')
		OR ISNULL(FireMarshalPendingClaim.coverageTypeCode,'~~~~') <> ISNULL(SOURCE.coverageTypeCode,'~~~~')
		OR ISNULL(FireMarshalPendingClaim.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
		OR ISNULL(FireMarshalPendingClaim.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
		OR FireMarshalPendingClaim.isIncendiaryFire <> SOURCE.isIncendiaryFire
		OR ISNULL(FireMarshalPendingClaim.isClaimUnderSIUInvestigation,0) <> ISNULL(SOURCE.isClaimUnderSIUInvestigation,0)
		OR ISNULL(FireMarshalPendingClaim.siuCompanyName,'~~~') <> ISNULL(SOURCE.siuCompanyName,'~~~')
		OR ISNULL(FireMarshalPendingClaim.siuRepresentativeFullName,'~~~') <> ISNULL(SOURCE.siuRepresentativeFullName,'~~~')
		OR ISNULL(FireMarshalPendingClaim.siuWorkPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuWorkPhoneNumber,'~~~~~~~~~~')
		OR ISNULL(FireMarshalPendingClaim.siuCellPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuCellPhoneNumber,'~~~~~~~~~~')
		OR FireMarshalPendingClaim.involvedPartyId <> SOURCE.involvedPartyId
		OR ISNULL(FireMarshalPendingClaim.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
		OR ISNULL(FireMarshalPendingClaim.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
		OR ISNULL(FireMarshalPendingClaim.reserveAmount,-1) <> ISNULL(SOURCE.reserveAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.totalInsuredAmount,-1) <> ISNULL(SOURCE.totalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
		OR ISNULL(FireMarshalPendingClaim.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
		OR ISNULL(FireMarshalPendingClaim.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
		OR ISNULL(FireMarshalPendingClaim.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
	);

/*Insert New*/
INSERT INTO dbo.FireMarshalPendingClaim
(
	elementalClaimId,
	uniqueInstanceValue,
	claimId,
	isoFileNumber,
	reportingStatus,
	fireMarshallStatus,
	fireMarshallDate,
	claimIsOpen,
	dateSubmittedToIso,
	originalClaimNumber,
	originalPolicyNumber,
	insuranceProviderOfficeCode,
	insuranceProviderCompanyCode,
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
	estimatedLossAmount,
	settlementAmount,
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
	involvedPartyId,
	adjusterId,
	involvedPartySequenceId,
	reserveAmount,
	totalInsuredAmount,
	replacementAmount,
	actualCashAmount,
	buildingPolicyAmount,
	buildingTotalInsuredAmount,
	buildingReplacementAmount,
	buildingActualCashAmount,
	buildingEstimatedLossAmount,
	contentPolicyAmount,
	contentTotalInsuredAmount,
	contentReplacementAmount,
	contentActualCashAmount,
	contentEstimatedLossAmount,
	stockPolicyAmount,
	stockTotalInsuredAmount,
	stockReplacementAmount,
	stockActualCashAmount,
	stockEstimatedLossAmount,
	lossOfUsePolicyAmount,
	lossOfUseTotalInsuredAmount,
	lossOfUseReplacementAmount,
	lossOfUseActualCashAmount,
	lossOfUseEstimatedLossAmount,
	otherPolicyAmount,
	otherTotalInsuredAmount,
	otherReplacementAmount,
	otherActualCashAmount,
	otherEstimatedLossAmount,
	buildingReserveAmount,
	stockReserveAmount,
	stockPaidAmount,
	lossOfUseReserve,
	lossOfUsePaid,
	otherReserveAmount,
	otherPaidAmount,
	isActive,
	isCurrent,
	dateInserted
)
	SELECT
		#PendingFMClaimDataToInsert.elementalClaimId,
		#PendingFMClaimDataToInsert.uniqueInstanceValue,
		#PendingFMClaimDataToInsert.claimId,
		#PendingFMClaimDataToInsert.isoFileNumber,
		#PendingFMClaimDataToInsert.reportingStatus,
		#PendingFMClaimDataToInsert.fireMarshallStatus,
		@dateInserted AS fireMarshallDate,
		#PendingFMClaimDataToInsert.claimIsOpen,
		#PendingFMClaimDataToInsert.dateSubmittedToIso,
		#PendingFMClaimDataToInsert.originalClaimNumber,
		#PendingFMClaimDataToInsert.originalPolicyNumber,
		#PendingFMClaimDataToInsert.insuranceProviderOfficeCode,
		#PendingFMClaimDataToInsert.insuranceProviderCompanyCode,
		#PendingFMClaimDataToInsert.companyName,
		#PendingFMClaimDataToInsert.affiliate1Code,
		#PendingFMClaimDataToInsert.affiliate1Name,
		#PendingFMClaimDataToInsert.affiliate2Code,
		#PendingFMClaimDataToInsert.affiliate2Name,
		#PendingFMClaimDataToInsert.groupCode,
		#PendingFMClaimDataToInsert.groupName,
		#PendingFMClaimDataToInsert.lossAddressLine1,
		#PendingFMClaimDataToInsert.lossAddressLine2,
		#PendingFMClaimDataToInsert.lossCityName,
		#PendingFMClaimDataToInsert.lossStateCode,
		#PendingFMClaimDataToInsert.lossStateName,
		#PendingFMClaimDataToInsert.lossZipCode,
		#PendingFMClaimDataToInsert.lossGeoCounty,
		#PendingFMClaimDataToInsert.lossLatitude,
		#PendingFMClaimDataToInsert.lossLongitude,
		#PendingFMClaimDataToInsert.lossDescription,
		#PendingFMClaimDataToInsert.lossDescriptionExtended,
		#PendingFMClaimDataToInsert.dateOfLoss,
		#PendingFMClaimDataToInsert.lossTypeCode,
		#PendingFMClaimDataToInsert.lossTypeDescription,
		#PendingFMClaimDataToInsert.policyTypeCode,
		#PendingFMClaimDataToInsert.policyTypeDescription,
		#PendingFMClaimDataToInsert.coverageTypeCode,
		#PendingFMClaimDataToInsert.coverageTypeDescription,
		#PendingFMClaimDataToInsert.estimatedLossAmount,
		#PendingFMClaimDataToInsert.settlementAmount,
		#PendingFMClaimDataToInsert.policyAmount,
		#PendingFMClaimDataToInsert.buildingPaidAmount,
		#PendingFMClaimDataToInsert.contentReserveAmount,
		#PendingFMClaimDataToInsert.contentPaidAmount,
		#PendingFMClaimDataToInsert.isIncendiaryFire,
		#PendingFMClaimDataToInsert.isClaimUnderSIUInvestigation,
		#PendingFMClaimDataToInsert.siuCompanyName,
		#PendingFMClaimDataToInsert.siuRepresentativeFullName,
		#PendingFMClaimDataToInsert.siuWorkPhoneNumber,
		#PendingFMClaimDataToInsert.siuCellPhoneNumber,
		#PendingFMClaimDataToInsert.involvedPartyId,
		#PendingFMClaimDataToInsert.adjusterId,
		#PendingFMClaimDataToInsert.involvedPartySequenceId,
		#PendingFMClaimDataToInsert.reserveAmount,
		#PendingFMClaimDataToInsert.totalInsuredAmount,
		#PendingFMClaimDataToInsert.replacementAmount,
		#PendingFMClaimDataToInsert.actualCashAmount,
		#PendingFMClaimDataToInsert.buildingPolicyAmount,
		#PendingFMClaimDataToInsert.buildingTotalInsuredAmount,
		#PendingFMClaimDataToInsert.buildingReplacementAmount,
		#PendingFMClaimDataToInsert.buildingActualCashAmount,
		#PendingFMClaimDataToInsert.buildingEstimatedLossAmount,
		#PendingFMClaimDataToInsert.contentPolicyAmount,
		#PendingFMClaimDataToInsert.contentTotalInsuredAmount,
		#PendingFMClaimDataToInsert.contentReplacementAmount,
		#PendingFMClaimDataToInsert.contentActualCashAmount,
		#PendingFMClaimDataToInsert.contentEstimatedLossAmount,
		#PendingFMClaimDataToInsert.stockPolicyAmount,
		#PendingFMClaimDataToInsert.stockTotalInsuredAmount,
		#PendingFMClaimDataToInsert.stockReplacementAmount,
		#PendingFMClaimDataToInsert.stockActualCashAmount,
		#PendingFMClaimDataToInsert.stockEstimatedLossAmount,
		#PendingFMClaimDataToInsert.lossOfUsePolicyAmount,
		#PendingFMClaimDataToInsert.lossOfUseTotalInsuredAmount,
		#PendingFMClaimDataToInsert.lossOfUseReplacementAmount,
		#PendingFMClaimDataToInsert.lossOfUseActualCashAmount,
		#PendingFMClaimDataToInsert.lossOfUseEstimatedLossAmount,
		#PendingFMClaimDataToInsert.otherPolicyAmount,
		#PendingFMClaimDataToInsert.otherTotalInsuredAmount,
		#PendingFMClaimDataToInsert.otherReplacementAmount,
		#PendingFMClaimDataToInsert.otherActualCashAmount,
		#PendingFMClaimDataToInsert.otherEstimatedLossAmount,
		#PendingFMClaimDataToInsert.buildingReserveAmount,
		#PendingFMClaimDataToInsert.stockReserveAmount,
		#PendingFMClaimDataToInsert.stockPaidAmount,
		#PendingFMClaimDataToInsert.lossOfUseReserve,
		#PendingFMClaimDataToInsert.lossOfUsePaid,
		#PendingFMClaimDataToInsert.otherReserveAmount,
		#PendingFMClaimDataToInsert.otherPaidAmount,
		1 AS isActive,
		1 AS isCurrent,
		@dateInserted AS dateInserted
	FROM
		#PendingFMClaimDataToInsert
		LEFT OUTER JOIN dbo.FireMarshalPendingClaim
			ON #PendingFMClaimDataToInsert.elementalClaimId = FireMarshalPendingClaim.elementalClaimId
				AND #PendingFMClaimDataToInsert.uniqueInstanceValue = FireMarshalPendingClaim.uniqueInstanceValue
WHERE
	FireMarshalPendingClaim.elementalClaimId IS NULL;
				
SELECT NULL AS [post 'update' sanity check.],
*
FROM dbo.FireMarshalPendingClaim	
	WHERE elementalClaimId IN
	(
		--50149,
		--30666,
		--31466,
		
		--222,
		--3601,
		90903,
		90903,
		92103,
		92103,
		93617,
		93617,
		93818,
		93818,
		95560,
		95560,
		96511,
		96511,
		99822,
		99822,
		72295,
		72331
	)

--*/

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;