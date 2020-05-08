SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 20190220
Author: Dan Ravaglia and Robert David Warner
Description: Mechanism for data-refresh of Pending FM Claim(s).
			
			Performance: Consider CROSSAPLY vs.
				second TempTable join for Exception-identification

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertFMHistoricClaim
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
				FROM dbo.FMClaimSendHistoryActivityLog
				WHERE
					FMClaimSendHistoryActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FMClaimSendHistoryActivityLog.isSuccessful = 1
					AND FMClaimSendHistoryActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*//*
			SELECT
				@dateFilterParam = CASE
					WHEN
						CAST(CAST(YEAR(GETDATE())-3 AS CHAR(4)) + '0101' AS DATE) < ISNULL(@dateFilterParam,CAST('99990101' AS DATE))
					THEN
						CAST(CAST(YEAR(GETDATE())-3 AS CHAR(4)) + '0101' AS DATE)
					ELSE
						@dateFilterParam
				END
			--*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(FMClaimSendHistoryActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2014-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.FMClaimSendHistoryActivityLog
			WHERE
				FMClaimSendHistoryActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND FMClaimSendHistoryActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureFMPassiveClaimDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				V_ActiveElementalClaim.elementalClaimId,
				CAST(ISNULL(ExistingHistoricClaim.uniqueInstanceValue,1) AS TINYINT) AS uniqueInstanceValue, 
				V_ActiveClaim.claimId,
				V_ActiveClaim.isoClaimId AS isoFileNumber,
				CAST('Passive' AS VARCHAR(25)) AS reportingStatus,
				CAST('Passive' AS VARCHAR(255))AS fireMarshallStatus,
				CAST(NULL AS DATE) AS fireMarshallDate,
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
				INTO #HistoricFMClaimDataToInsert
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
				INNER JOIN dbo.InvolvedParty
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
				LEFT OUTER JOIN dbo.FireMarshalClaimSendHistory AS ExistingHistoricClaim
					ON V_ActiveElementalClaim.elementalClaimId = ExistingHistoricClaim.elementalClaimId
			WHERE
				FireMarshalController.endDate IS NULL
				AND FireMarshalController.fmStateStatusCode = 'P'
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
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FMClaimSendHistoryActivityLog
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
				@stepDescription = 'Update existing FMHistoric Claim',
				@stepStartDateTime = GETDATE();
			
			
			UPDATE dbo.FireMarshalClaimSendHistory
				SET
					FireMarshalClaimSendHistory.uniqueInstanceValue = FireMarshalClaimSendHistory.uniqueInstanceValue +1,
					FireMarshalClaimSendHistory.claimId = SOURCE.claimId,
					FireMarshalClaimSendHistory.isoFileNumber = SOURCE.isoFileNumber,
					FireMarshalClaimSendHistory.reportingStatus = SOURCE.reportingStatus,
					FireMarshalClaimSendHistory.fireMarshallStatus = SOURCE.fireMarshallStatus,
					FireMarshalClaimSendHistory.fireMarshallDate = @dateInserted,
					FireMarshalClaimSendHistory.claimIsOpen = SOURCE.claimIsOpen,
					FireMarshalClaimSendHistory.dateSubmittedToIso = SOURCE.dateSubmittedToIso,
					FireMarshalClaimSendHistory.originalClaimNumber = SOURCE.originalClaimNumber,
					FireMarshalClaimSendHistory.originalPolicyNumber = SOURCE.originalPolicyNumber,
					FireMarshalClaimSendHistory.insuranceProviderOfficeCode = SOURCE.insuranceProviderOfficeCode,
					FireMarshalClaimSendHistory.insuranceProviderCompanyCode = SOURCE.insuranceProviderCompanyCode,
					FireMarshalClaimSendHistory.companyName = SOURCE.companyName,
					FireMarshalClaimSendHistory.affiliate1Code = SOURCE.affiliate1Code,
					FireMarshalClaimSendHistory.affiliate1Name = SOURCE.affiliate1Name,
					FireMarshalClaimSendHistory.affiliate2Code = SOURCE.affiliate2Code,
					FireMarshalClaimSendHistory.affiliate2Name = SOURCE.affiliate2Name,
					FireMarshalClaimSendHistory.groupCode = SOURCE.groupCode,
					FireMarshalClaimSendHistory.groupName = SOURCE.groupName,
					FireMarshalClaimSendHistory.lossAddressLine1 = SOURCE.lossAddressLine1,
					FireMarshalClaimSendHistory.lossAddressLine2 = SOURCE.lossAddressLine2,
					FireMarshalClaimSendHistory.lossCityName = SOURCE.lossCityName,
					FireMarshalClaimSendHistory.lossStateCode = SOURCE.lossStateCode,
					FireMarshalClaimSendHistory.lossStateName = SOURCE.lossStateName,
					FireMarshalClaimSendHistory.lossZipCode = SOURCE.lossZipCode,
					FireMarshalClaimSendHistory.lossGeoCounty = SOURCE.lossGeoCounty,
					FireMarshalClaimSendHistory.lossLatitude = SOURCE.lossLatitude,
					FireMarshalClaimSendHistory.lossLongitude = SOURCE.lossLongitude,
					FireMarshalClaimSendHistory.lossDescription = SOURCE.lossDescription,
					FireMarshalClaimSendHistory.lossDescriptionExtended = SOURCE.lossDescriptionExtended,
					FireMarshalClaimSendHistory.dateOfLoss = SOURCE.dateOfLoss,
					FireMarshalClaimSendHistory.lossTypeCode = SOURCE.lossTypeCode,
					FireMarshalClaimSendHistory.lossTypeDescription = SOURCE.lossTypeDescription,
					FireMarshalClaimSendHistory.policyTypeCode = SOURCE.policyTypeCode,
					FireMarshalClaimSendHistory.policyTypeDescription = SOURCE.policyTypeDescription,
					FireMarshalClaimSendHistory.coverageTypeCode = SOURCE.coverageTypeCode,
					FireMarshalClaimSendHistory.coverageTypeDescription = SOURCE.coverageTypeDescription,
					FireMarshalClaimSendHistory.estimatedLossAmount = SOURCE.estimatedLossAmount,
					FireMarshalClaimSendHistory.settlementAmount = SOURCE.settlementAmount,
					FireMarshalClaimSendHistory.policyAmount = SOURCE.policyAmount,
					FireMarshalClaimSendHistory.buildingPaidAmount = SOURCE.buildingPaidAmount,
					FireMarshalClaimSendHistory.contentReserveAmount = SOURCE.contentReserveAmount,
					FireMarshalClaimSendHistory.contentPaidAmount = SOURCE.contentPaidAmount,
					FireMarshalClaimSendHistory.isIncendiaryFire = SOURCE.isIncendiaryFire,
					FireMarshalClaimSendHistory.isClaimUnderSIUInvestigation = SOURCE.isClaimUnderSIUInvestigation,
					FireMarshalClaimSendHistory.siuCompanyName = SOURCE.siuCompanyName,
					FireMarshalClaimSendHistory.siuRepresentativeFullName = SOURCE.siuRepresentativeFullName,
					FireMarshalClaimSendHistory.siuWorkPhoneNumber = SOURCE.siuWorkPhoneNumber,
					FireMarshalClaimSendHistory.siuCellPhoneNumber = SOURCE.siuCellPhoneNumber,
					FireMarshalClaimSendHistory.involvedPartyId = SOURCE.involvedPartyId,
					FireMarshalClaimSendHistory.involvedPartyFullName = SOURCE.involvedPartyFullName,
					FireMarshalClaimSendHistory.adjusterId = SOURCE.adjusterId,
					FireMarshalClaimSendHistory.involvedPartySequenceId = SOURCE.involvedPartySequenceId,
					FireMarshalClaimSendHistory.reserveAmount = SOURCE.reserveAmount,
					FireMarshalClaimSendHistory.totalInsuredAmount = SOURCE.totalInsuredAmount,
					FireMarshalClaimSendHistory.replacementAmount = SOURCE.replacementAmount,
					FireMarshalClaimSendHistory.actualCashAmount = SOURCE.actualCashAmount,
					FireMarshalClaimSendHistory.buildingPolicyAmount = SOURCE.buildingPolicyAmount,
					FireMarshalClaimSendHistory.buildingTotalInsuredAmount = SOURCE.buildingTotalInsuredAmount,
					FireMarshalClaimSendHistory.buildingReplacementAmount = SOURCE.buildingReplacementAmount,
					FireMarshalClaimSendHistory.buildingActualCashAmount = SOURCE.buildingActualCashAmount,
					FireMarshalClaimSendHistory.buildingEstimatedLossAmount = SOURCE.buildingEstimatedLossAmount,
					FireMarshalClaimSendHistory.contentPolicyAmount = SOURCE.contentPolicyAmount,
					FireMarshalClaimSendHistory.contentTotalInsuredAmount = SOURCE.contentTotalInsuredAmount,
					FireMarshalClaimSendHistory.contentReplacementAmount = SOURCE.contentReplacementAmount,
					FireMarshalClaimSendHistory.contentActualCashAmount = SOURCE.contentActualCashAmount,
					FireMarshalClaimSendHistory.contentEstimatedLossAmount = SOURCE.contentEstimatedLossAmount,
					FireMarshalClaimSendHistory.stockPolicyAmount = SOURCE.stockPolicyAmount,
					FireMarshalClaimSendHistory.stockTotalInsuredAmount = SOURCE.stockTotalInsuredAmount,
					FireMarshalClaimSendHistory.stockReplacementAmount = SOURCE.stockReplacementAmount,
					FireMarshalClaimSendHistory.stockActualCashAmount = SOURCE.stockActualCashAmount,
					FireMarshalClaimSendHistory.stockEstimatedLossAmount = SOURCE.stockEstimatedLossAmount,
					FireMarshalClaimSendHistory.lossOfUsePolicyAmount = SOURCE.lossOfUsePolicyAmount,
					FireMarshalClaimSendHistory.lossOfUseTotalInsuredAmount = SOURCE.lossOfUseTotalInsuredAmount,
					FireMarshalClaimSendHistory.lossOfUseReplacementAmount = SOURCE.lossOfUseReplacementAmount,
					FireMarshalClaimSendHistory.lossOfUseActualCashAmount = SOURCE.lossOfUseActualCashAmount,
					FireMarshalClaimSendHistory.lossOfUseEstimatedLossAmount = SOURCE.lossOfUseEstimatedLossAmount,
					FireMarshalClaimSendHistory.otherPolicyAmount = SOURCE.otherPolicyAmount,
					FireMarshalClaimSendHistory.otherTotalInsuredAmount = SOURCE.otherTotalInsuredAmount,
					FireMarshalClaimSendHistory.otherReplacementAmount = SOURCE.otherReplacementAmount,
					FireMarshalClaimSendHistory.otherActualCashAmount = SOURCE.otherActualCashAmount,
					FireMarshalClaimSendHistory.otherEstimatedLossAmount = SOURCE.otherEstimatedLossAmount,
					FireMarshalClaimSendHistory.buildingReserveAmount = SOURCE.buildingReserveAmount,
					FireMarshalClaimSendHistory.stockReserveAmount = SOURCE.stockReserveAmount,
					FireMarshalClaimSendHistory.stockPaidAmount = SOURCE.stockPaidAmount,
					FireMarshalClaimSendHistory.lossOfUseReserve = SOURCE.lossOfUseReserve,
					FireMarshalClaimSendHistory.lossOfUsePaid = SOURCE.lossOfUsePaid,
					FireMarshalClaimSendHistory.otherReserveAmount = SOURCE.otherReserveAmount,
					FireMarshalClaimSendHistory.otherPaidAmount = SOURCE.otherPaidAmount,
					FireMarshalClaimSendHistory.isActive = 1,
					FireMarshalClaimSendHistory.dateInserted = @dateInserted
			FROM
				#HistoricFMClaimDataToInsert AS SOURCE
			WHERE
				FireMarshalClaimSendHistory.elementalClaimId = SOURCE.elementalClaimId
				AND (
					FireMarshalClaimSendHistory.claimId <> SOURCE.claimId
					OR ISNULL(FireMarshalClaimSendHistory.isoFileNumber,'~~~') <> ISNULL(SOURCE.isoFileNumber,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.reportingStatus,'~~~') <> ISNULL(SOURCE.reportingStatus,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.fireMarshallStatus,'~~~') <> ISNULL(SOURCE.fireMarshallStatus,'~~~')
					OR  FireMarshalClaimSendHistory.claimIsOpen <> SOURCE.claimIsOpen
					OR CAST(ISNULL(FireMarshalClaimSendHistory.dateSubmittedToIso,'99990101') AS DATE) <> CAST(ISNULL(SOURCE.dateSubmittedToIso,'99990101') AS DATE)
					OR ISNULL(FireMarshalClaimSendHistory.originalClaimNumber,'~~~') <> ISNULL(SOURCE.originalClaimNumber,'~~~')
					OR FireMarshalClaimSendHistory.originalPolicyNumber <> SOURCE.originalPolicyNumber
					OR FireMarshalClaimSendHistory.insuranceProviderOfficeCode <> SOURCE.insuranceProviderOfficeCode
					OR FireMarshalClaimSendHistory.insuranceProviderCompanyCode <> SOURCE.insuranceProviderCompanyCode
					OR FireMarshalClaimSendHistory.companyName <> SOURCE.companyName
					OR FireMarshalClaimSendHistory.affiliate1Code <> SOURCE.affiliate1Code
					OR FireMarshalClaimSendHistory.affiliate1Name <> SOURCE.affiliate1Name
					OR FireMarshalClaimSendHistory.affiliate2Code <> SOURCE.affiliate2Code
					OR FireMarshalClaimSendHistory.affiliate2Name <> SOURCE.affiliate2Name
					OR FireMarshalClaimSendHistory.groupCode <> SOURCE.groupCode
					OR FireMarshalClaimSendHistory.groupName <> SOURCE.groupName
					OR ISNULL(FireMarshalClaimSendHistory.lossAddressLine1,'~~~') <> ISNULL(SOURCE.lossAddressLine1,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossAddressLine2,'~~~') <> ISNULL(SOURCE.lossAddressLine2,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossCityName,'~~~') <> ISNULL(SOURCE.lossCityName,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossStateCode,'~~') <> ISNULL(SOURCE.lossStateCode,'~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossStateName,'~~~') <> ISNULL(SOURCE.lossStateName,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossZipCode,'~~~~~') <> ISNULL(SOURCE.lossZipCode,'~~~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossGeoCounty,'~~~') <> ISNULL(SOURCE.lossGeoCounty,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossLatitude,'~~~') <> ISNULL(SOURCE.lossLatitude,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossLongitude,'~~~') <> ISNULL(SOURCE.lossLongitude,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossDescription,'~~~') <> ISNULL(SOURCE.lossDescription,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossDescriptionExtended,'~~~') <> ISNULL(SOURCE.lossDescriptionExtended,'~~~')
					OR CAST(ISNULL(FireMarshalClaimSendHistory.dateOfLoss,'99990101') AS DATE) <> CAST(ISNULL(SOURCE.dateOfLoss,'99990101') AS DATE)
					OR ISNULL(FireMarshalClaimSendHistory.lossTypeCode,'~~~~') <> ISNULL(SOURCE.lossTypeCode,'~~~~')
					OR ISNULL(FireMarshalClaimSendHistory.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.policyTypeCode,'~~~~') <> ISNULL(SOURCE.policyTypeCode,'~~~~')
					OR ISNULL(FireMarshalClaimSendHistory.policyTypeDescription,'~~~') <> ISNULL(SOURCE.policyTypeDescription,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.coverageTypeCode,'~~~~') <> ISNULL(SOURCE.coverageTypeCode,'~~~~')
					OR ISNULL(FireMarshalClaimSendHistory.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR FireMarshalClaimSendHistory.isIncendiaryFire <> SOURCE.isIncendiaryFire
					OR ISNULL(FireMarshalClaimSendHistory.isClaimUnderSIUInvestigation,0) <> ISNULL(SOURCE.isClaimUnderSIUInvestigation,0)
					OR ISNULL(FireMarshalClaimSendHistory.siuCompanyName,'~~~') <> ISNULL(SOURCE.siuCompanyName,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.siuRepresentativeFullName,'~~~') <> ISNULL(SOURCE.siuRepresentativeFullName,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.siuWorkPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuWorkPhoneNumber,'~~~~~~~~~~')
					OR ISNULL(FireMarshalClaimSendHistory.siuCellPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuCellPhoneNumber,'~~~~~~~~~~')
					OR FireMarshalClaimSendHistory.involvedPartyId <> SOURCE.involvedPartyId
					OR ISNULL(FireMarshalClaimSendHistory.involvedPartyFullName,'~~~') <> ISNULL(SOURCE.involvedPartyFullName,'~~~')
					OR ISNULL(FireMarshalClaimSendHistory.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					OR ISNULL(FireMarshalClaimSendHistory.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
					OR ISNULL(FireMarshalClaimSendHistory.reserveAmount,-1) <> ISNULL(SOURCE.reserveAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.totalInsuredAmount,-1) <> ISNULL(SOURCE.totalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(FireMarshalClaimSendHistory.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(FireMarshalClaimSendHistory.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
					OR FireMarshalClaimSendHistory.isActive <> 1
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
			
			/*Log Activity*/
			INSERT INTO dbo.FMClaimSendHistoryActivityLog
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
				@stepId = 3,
				@stepDescription = 'Insert new FMHistoric Claim',
				@stepStartDateTime = GETDATE();	
			
			INSERT INTO dbo.FireMarshalClaimSendHistory
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
				involvedPartyFullName,
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
				dateInserted
			)
			SELECT
				SOURCE.elementalClaimId,
				SOURCE.uniqueInstanceValue,
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
				SOURCE.involvedPartyFullName,
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
				@dateInserted AS dateInserted
			FROM
				#HistoricFMClaimDataToInsert AS SOURCE
				LEFT OUTER JOIN dbo.FireMarshalClaimSendHistory
					ON SOURCE.elementalClaimId = FireMarshalClaimSendHistory.elementalClaimId
			WHERE
				FireMarshalClaimSendHistory.elementalClaimId IS NULL;
			
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMClaimSendHistoryActivityLog
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
				@stepDescription = 'FullHistoryCompare DeActivateClaims',
				@stepStartDateTime = GETDATE();	

			UPDATE dbo.FireMarshalClaimSendHistory
			SET
				FireMarshalClaimSendHistory.isActive = 0,
				FireMarshalClaimSendHistory.dateInserted = @dateInserted
			FROM
				dbo.FireMarshalClaimSendHistory
				LEFT OUTER JOIN #HistoricFMClaimDataToInsert
					ON #HistoricFMClaimDataToInsert.elementalClaimId = FireMarshalClaimSendHistory.elementalClaimId
			WHERE
				#HistoricFMClaimDataToInsert.elementalClaimId IS NULL
				AND FireMarshalClaimSendHistory.reportingStatus = 'Passive'
				AND FireMarshalClaimSendHistory.isActive = 1;

			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMClaimSendHistoryActivityLog
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
		INSERT INTO dbo.FMClaimSendHistoryActivityLog
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
					+ 'of hsp_UpdateInsertFireMarshalDriver; ErrorMsg: '
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