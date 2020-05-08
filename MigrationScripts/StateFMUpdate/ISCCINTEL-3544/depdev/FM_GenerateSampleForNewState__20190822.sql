SET NOEXEC OFF;
/*Instructions:
		0.5) <OPTIONAL> Update the ProejctedGenerationDate on line 48
	1.) Update the @StateCode variable on line 50
	2.) Update the TableName-To-Insert-Into on line 415
	3.) Execute 
	

*/
USE ClaimSearch_Prod

BEGIN TRANSACTION

DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(1) = CHAR(13);
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
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*/*GenerateTableName: for line 158*/
	SELECT 'FMExampleSet' + @stateCode + '_' + CAST(YEAR(@executeTimestamp) AS CHAR(4))
--*/
DECLARE @projectedGenerationDate DATE = CAST('20190805' AS DATE),
		@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
		@stateCode CHAR(2) = 'MS'/*!!InitializeMe!!*/;
DECLARE	@fireMarshallDate DATE = DATEADD(DAY, -4, @projectedGenerationDate);
SELECT	/*<OPTIONAL> You can limit the number of rows here, using TOP (#)*/
	V_ActiveElementalClaim.elementalClaimId,
	CAST(ISNULL(ExistingPendingClaim.uniqueInstanceValue,1) AS TINYINT) AS uniqueInstanceValue, 
	V_ActiveClaim.claimId,
	V_ActiveClaim.isoClaimId AS isoFileNumber,
	CAST('Pending' AS VARCHAR(25)) AS reportingStatus,
	(CAST(DATENAME(MONTH, @projectedGenerationDate) AS VARCHAR(255)) + ' ' + CAST(YEAR(@projectedGenerationDate) AS CHAR(4))) AS fireMarshallStatus,
	@fireMarshallDate AS fireMarshallDate,
	@projectedGenerationDate AS projectedGenerationDate,
	/*The following code block supports Propogating "any closed elemental-claim status" to the entire claim.
		Deprecated 20190712, per conversation with business leaders (Stephen Adams).
		Search for a similar description tag or for "ClaimOpenCloseAggregation" to uncomment supporting code- RDW*//*
		CASE
			WHEN
				COALESCE(ClaimOpenCloseAggregation.instancesOfClosedClaim,0) = 0
			THEN
				CAST(1 AS BIT)
			ELSE
				CAST(0 AS BIT)
		END AS claimIsOpen,
	*/
	CASE
		WHEN
			V_ActiveElementalClaim.dateClaimClosed IS NOT NULL
			OR V_ActiveElementalClaim.coverageStatus IN('C', 'CWP')
		THEN
			0
		ELSE
			1
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
	InvolvedParty.fullName AS involvedPartyFullName,
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
	INTO #TempPending
FROM
	dbo.FireMarshalController WITH(NOLOCK)
	INNER JOIN [ClaimSearch_Prod].dbo.Lookup_States WITH (NOLOCK)
		ON FireMarshalController.fmStateCode = Lookup_States.State_Abb
	INNER JOIN (
		SELECT
			DuplicateDataSetPerformanceHackMelissaNameMap.addressId,
			CAST(1 AS BIT) AS isLocationOfLoss,
			DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine1,
			DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine2,
			DuplicateDataSetPerformanceHackMelissaNameMap.originalCityName,
			DuplicateDataSetPerformanceHackMelissaNameMap.originalStateCode,
			DuplicateDataSetPerformanceHackMelissaNameMap.originalZipCode,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine1,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine2,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCityName,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedStateCode,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCode,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCodeExtended,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyName,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyFIPS,
			DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountryCode,
			DuplicateDataSetPerformanceHackMelissaNameMap.latitude,
			DuplicateDataSetPerformanceHackMelissaNameMap.longitude,
			DuplicateDataSetPerformanceHackMelissaNameMap.geoAccuracy,
			DuplicateDataSetPerformanceHackMelissaNameMap.melissaMappingKey,
			DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId,
			DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue
		FROM
			(/*Notes on DuplicateDataSetPerformanceHack: dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 contains duplicate records
				performance of rowNumber/partition is noticeably better than using DISTINCT*/
				SELECT
					ExistingAddress.addressId,
					CAST(LTRIM(RTRIM(CLT00001.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId, 
					ROW_NUMBER() OVER(
						PARTITION BY
							CLT00001.ALLCLMROWID
						ORDER BY
							CLT00001.Date_Insert DESC
							/*CS_Lookup_EntityIDs.[YEAR] DESC*/
					) AS uniqueInstanceValue,
					CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR1)),'') AS VARCHAR(50))AS originalAddressLine1,
					CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR2)),'') AS VARCHAR(50))AS originalAddressLine2,
					CAST(NULLIF(LTRIM(RTRIM(CLT00001.M_LOL_CITY)),'') AS VARCHAR(25))AS originalCityName,
					CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ST_ALPH)),'') AS CHAR(2))AS originalStateCode,
					CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ZIP)),'') AS VARCHAR(9))AS originalZipCode,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine1,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine2,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(25))AS scrubbedCityName,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(2))AS scrubbedStateCode,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(5))AS scrubbedZipCode,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(4))AS scrubbedZipCodeExtended,
					CAST(
						COALESCE
						(
							NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_GeoCounty)),''),
							NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_AddrCountyName)),'')
						) AS VARCHAR(25)
					) AS scrubbedCountyName,
					CAST(
						COALESCE
						(
							NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_GeoCountyFIPS)),''),
							NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_AddrCountyFIPS)),'')
						) AS VARCHAR(25)
					) AS scrubbedCountyFIPS,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(3))AS scrubbedCountryCode,
					CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Latitude)),'') AS VARCHAR(15))AS latitude,
					CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Longitude)),'') AS VARCHAR(15))AS longitude,
					CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS geoAccuracy,
					CAST(NULL AS BIGINT) AS melissaMappingKey
				FROM
					dbo.FireMarshalDriver WITH (NOLOCK)
					INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
						ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
					LEFT OUTER JOIN dbo.Address AS ExistingAddress WITH (NOLOCK)
						ON ExistingAddress.isoClaimId = CLT00001.I_ALLCLM
							AND ExistingAddress.isLocationOfLoss = 1
					LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
						ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
					LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
						ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
				WHERE
					/*Deprecating due to performance costs, and current profile state. RDW 20190328:
						NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
					*/
					CLT00001.C_LOL_ST_ALPH = @stateCode
			) AS DuplicateDataSetPerformanceHackMelissaNameMap
	) AS V_ActiveLocationOfLoss
		ON FireMarshalController.fmStateCode = V_ActiveLocationOfLoss.originalStateCode
		AND V_ActiveLocationOfLoss.uniqueInstanceValue = 1
	INNER JOIN dbo.V_ActiveClaim WITH (NOLOCK)
		ON V_ActiveLocationOfLoss.addressId = V_ActiveClaim.locationOfLossAddressId
	INNER JOIN dbo.V_ActivePolicy WITH (NOLOCK)
		ON V_ActiveClaim.policyId = V_ActivePolicy.policyId
	INNER JOIN dbo.V_ActiveElementalClaim WITH (NOLOCK)
		ON V_ActiveClaim.claimId = V_ActiveElementalClaim.claimId
	INNER JOIN dbo.InvolvedParty WITH(NOLOCK)
		ON V_ActiveElementalClaim.involvedPartyId = InvolvedParty.involvedPartyId
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
			dbo.FireMarshalDriver WITH(NOLOCK)
			INNER JOIN [ClaimSearch_Prod].dbo.CLT00018 WITH (NOLOCK)
				ON FireMarshalDriver.isoClaimId = CLT00018.I_ALLCLM
	) DuplicateRemovalFlagPartition
		ON V_ActiveClaim.isoClaimId = DuplicateRemovalFlagPartition.I_ALLCLM
	/*The following code block supports Propogating "any closed elemental-claim status" to the entire claim.
		Deprecated 20190712, per conversation with business leaders (Stephen Adams). - RDW*//*
		LEFT OUTER JOIN(
			SELECT
				InnerActiveElementalClaim.claimId,
				SUM
				(
					CASE
						WHEN
							InnerActiveElementalClaim.dateClaimClosed IS NOT NULL
							OR InnerActiveElementalClaim.coverageStatus IN('C', 'CWP')
						THEN
							1
						ELSE
							0
					END
				) AS instancesOfClosedClaim
			FROM
				dbo.V_ActiveElementalClaim AS InnerActiveElementalClaim WITH (NOLOCK)
			GROUP BY
				InnerActiveElementalClaim.claimId
		) AS ClaimOpenCloseAggregation
			ON V_ActiveElementalClaim.claimId = ClaimOpenCloseAggregation.claimId
	*/
	/*DevNote: The following LO-join against the FireMarshalClaimSendHistory object
		is paired with an IS-NULL filter; this is to ensure that duplicate claim-
		-representation	on the dashboard is prevented*/
	LEFT OUTER JOIN dbo.V_ActiveFireMarshalClaimSendHistory WITH(NOLOCK)
		ON V_ActiveElementalClaim.claimId = V_ActiveFireMarshalClaimSendHistory.claimId
		AND V_ActiveFireMarshalClaimSendHistory.reportingStatus = 'Sent'
		/*fire indicator, from 18, join on I_ALLCLM, colName=F_INCEND_FIRE*/
	LEFT OUTER JOIN dbo.FireMarshalPendingClaim AS ExistingPendingClaim WITH(NOLOCK)
		ON V_ActiveElementalClaim.elementalClaimId = ExistingPendingClaim.elementalClaimId
WHERE
	FireMarshalController.endDate IS NULL
	--AND FireMarshalController.fmStateStatusCode = 'A'
	--AND FireMarshalController.projectedGenerationDate IS NOT NULL
	AND FireMarshalController.fmStateCode = @stateCode
	AND ISNULL(ExistingPendingClaim.isCurrent,1) = 1
	AND V_ActiveFireMarshalClaimSendHistory.claimId IS NULL
	AND ISNULL(DuplicateRemovalFlagPartition.incendiaryFireUniqueInstanceValue,1) = 1
	AND (
		V_ActiveElementalClaim.lossTypeCode = 'FIRE'
		OR (
			V_ActiveElementalClaim.lossTypeCode = 'EXPL'
			AND V_ActiveLocationOfLoss.originalStateCode IN ('KY', 'FL')
		)
		OR (
			V_ActiveElementalClaim.lossTypeCode = 'LGHT'
			AND V_ActiveLocationOfLoss.originalStateCode = 'KY'
		)
	);

	SELECT
		DashboardAggregationByClaim.elementalClaimId,
		DashboardAggregationByClaim.claimId,
		/*DashboardAggregationByClaim.uniqueInstanceValue,*/
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
		CAST(1 AS BIT) AS isActive,
		CAST(1 AS BIT) AS isCurrent,
		DashboardAggregationByClaim.involvedPartyId,
		DashboardAggregationByClaim.involvedPartyFullName,
		DashboardAggregationByClaim.adjusterId,
		DashboardAggregationByClaim.involvedPartySequenceId,
		@dateInserted AS dateInserted
		INTO ClaimSearch_Dev.dbo.FMExampleSetMS_2019lookatThis				/*<-------------------------- MAKE SURE TO UPDATE THIS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
	FROM
		(
			SELECT
				FireMarshalPendingClaim.elementalClaimId,
				FireMarshalPendingClaim.claimId,
				/*FireMarshalPendingClaim.uniqueInstanceValue,*/
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
				FireMarshalPendingClaim.involvedPartyId,
				FireMarshalPendingClaim.involvedPartyFullName,
				FireMarshalPendingClaim.adjusterId,
				FireMarshalPendingClaim.involvedPartySequenceId
				/*dateInserted*/
		
			FROM
				#TempPending AS FireMarshalPendingClaim
				LEFT OUTER JOIN dbo.V_ActiveAdjuster
					ON FireMarshalPendingClaim.adjusterId = V_ActiveAdjuster.adjusterId
		) AS DashboardAggregationByClaim
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO	

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; ROLLBACK TRANSACTION;


/*
******************************************	Env: JDESQLPRD3.ClaimSearch_Prod	**	User: vrskjdeprd\i24325				**	Time: Aug 26 2019  1:07PM			******************************************

(28897 row(s) affected)

(28897 row(s) affected)
COMMIT

*/
SELECT @@TRANCOUNT