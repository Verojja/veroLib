SET NOEXEC OFF;

--USE ClaimSearch_Dev
USE ClaimSearch_Prod

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
			
			Performance: 

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertFireMarshalExtract
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
				FROM dbo.FireMarshalExtractActivityLog
				WHERE
					FireMarshalExtractActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FireMarshalExtractActivityLog.isSuccessful = 1
					AND FireMarshalExtractActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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

			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(FireMarshalExtractActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2014-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.FireMarshalExtractActivityLog
			WHERE
				FireMarshalExtractActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND FireMarshalExtractActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureFMExtractDataToImport',
				@stepStartDateTime = GETDATE();
			
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
				INTO #PendingFMExtractDataToInsert
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
						dbo.V_ActiveCurrentPendingFMClaim AS FireMarshalPendingClaim
						LEFT OUTER JOIN dbo.V_ActiveAdjuster
							ON FireMarshalPendingClaim.adjusterId = V_ActiveAdjuster.adjusterId
				) AS DashboardAggregationByClaim
				
			UNION

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
			FROM
				(
					SELECT
						V_ActiveFireMarshalClaimSendHistory.elementalClaimId,
						V_ActiveFireMarshalClaimSendHistory.claimId,
						/*V_ActiveFireMarshalClaimSendHistory.uniqueInstanceValue,*/
						ROW_NUMBER() OVER
						(
							PARTITION BY
								V_ActiveFireMarshalClaimSendHistory.claimId,
								V_ActiveFireMarshalClaimSendHistory.uniqueInstanceValue
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
						V_ActiveFireMarshalClaimSendHistory.isoFileNumber,
						V_ActiveFireMarshalClaimSendHistory.reportingStatus,
						V_ActiveFireMarshalClaimSendHistory.fireMarshallStatus AS fMstatus,
						V_ActiveFireMarshalClaimSendHistory.fireMarshallDate AS fMDate,
						V_ActiveFireMarshalClaimSendHistory.claimIsOpen,
						V_ActiveFireMarshalClaimSendHistory.dateSubmittedToIso,
						V_ActiveFireMarshalClaimSendHistory.originalClaimNumber,
						V_ActiveFireMarshalClaimSendHistory.originalPolicyNumber,
						V_ActiveFireMarshalClaimSendHistory.insuranceProviderOfficeCode,
						V_ActiveFireMarshalClaimSendHistory.insuranceProviderCompanyCode,

						V_ActiveAdjuster.adjusterCompanyCode,
						V_ActiveAdjuster.adjusterCompanyName,

						V_ActiveFireMarshalClaimSendHistory.companyName,
						V_ActiveFireMarshalClaimSendHistory.affiliate1Code,
						V_ActiveFireMarshalClaimSendHistory.affiliate1Name,
						V_ActiveFireMarshalClaimSendHistory.affiliate2Code,
						V_ActiveFireMarshalClaimSendHistory.affiliate2Name,
						V_ActiveFireMarshalClaimSendHistory.groupCode,
						V_ActiveFireMarshalClaimSendHistory.groupName,
						V_ActiveFireMarshalClaimSendHistory.lossAddressLine1,
						V_ActiveFireMarshalClaimSendHistory.lossAddressLine2,
						V_ActiveFireMarshalClaimSendHistory.lossCityName,
						V_ActiveFireMarshalClaimSendHistory.lossStateCode,
						V_ActiveFireMarshalClaimSendHistory.lossStateName,
						V_ActiveFireMarshalClaimSendHistory.lossZipCode,
						V_ActiveFireMarshalClaimSendHistory.lossGeoCounty,
						V_ActiveFireMarshalClaimSendHistory.lossLatitude,
						V_ActiveFireMarshalClaimSendHistory.lossLongitude,
						V_ActiveFireMarshalClaimSendHistory.lossDescription,
						V_ActiveFireMarshalClaimSendHistory.lossDescriptionExtended,
						V_ActiveFireMarshalClaimSendHistory.dateOfLoss,
						V_ActiveFireMarshalClaimSendHistory.lossTypeCode,
						V_ActiveFireMarshalClaimSendHistory.lossTypeDescription,
						V_ActiveFireMarshalClaimSendHistory.policyTypeCode,
						V_ActiveFireMarshalClaimSendHistory.policyTypeDescription,
						V_ActiveFireMarshalClaimSendHistory.coverageTypeCode,
						V_ActiveFireMarshalClaimSendHistory.coverageTypeDescription,

						V_ActiveFireMarshalClaimSendHistory.settlementAmount,
						V_ActiveFireMarshalClaimSendHistory.estimatedLossAmount,
						V_ActiveFireMarshalClaimSendHistory.policyAmount,
						V_ActiveFireMarshalClaimSendHistory.buildingPaidAmount,
						V_ActiveFireMarshalClaimSendHistory.contentReserveAmount,
						V_ActiveFireMarshalClaimSendHistory.contentPaidAmount,
						V_ActiveFireMarshalClaimSendHistory.isIncendiaryFire,
						V_ActiveFireMarshalClaimSendHistory.isClaimUnderSIUInvestigation,
						V_ActiveFireMarshalClaimSendHistory.siuCompanyName,
						V_ActiveFireMarshalClaimSendHistory.siuRepresentativeFullName,
						V_ActiveFireMarshalClaimSendHistory.siuWorkPhoneNumber,
						V_ActiveFireMarshalClaimSendHistory.siuCellPhoneNumber,
						/*isActive*/
						/*isCurrent*/
						V_ActiveFireMarshalClaimSendHistory.involvedPartyId,
						V_ActiveFireMarshalClaimSendHistory.involvedPartyFullName,
						V_ActiveFireMarshalClaimSendHistory.adjusterId,
						V_ActiveFireMarshalClaimSendHistory.involvedPartySequenceId
						/*dateInserted*/
					FROM
						dbo.V_ActiveFireMarshalClaimSendHistory
						LEFT OUTER JOIN dbo.V_ActiveAdjuster
							ON V_ActiveFireMarshalClaimSendHistory.adjusterId = V_ActiveAdjuster.adjusterId
				) AS DashboardAggregationByClaim;
	
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
			
			/*Log Activity*/
			INSERT INTO dbo.FireMarshalExtractActivityLog
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
				@stepDescription = 'Update Existing FMExtract record',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.FireMarshalExtract
				SET
					/*elementalClaimId,*/
					/*claimId*/
					FireMarshalExtract.isoFileNumber = SOURCE.isoFileNumber,
					FireMarshalExtract.reportingStatus = SOURCE.reportingStatus,
					FireMarshalExtract.fMstatus = SOURCE.fMstatus,
					FireMarshalExtract.fMDate = SOURCE.fMDate,
					FireMarshalExtract.claimIsOpen = SOURCE.claimIsOpen,
					FireMarshalExtract.dateSubmittedToIso = SOURCE.dateSubmittedToIso,
					FireMarshalExtract.originalClaimNumber = SOURCE.originalClaimNumber,
					FireMarshalExtract.originalPolicyNumber = SOURCE.originalPolicyNumber,
					FireMarshalExtract.insuranceProviderOfficeCode = SOURCE.insuranceProviderOfficeCode,
					FireMarshalExtract.insuranceProviderCompanyCode = SOURCE.insuranceProviderCompanyCode,
					FireMarshalExtract.adjusterCompanyCode = SOURCE.adjusterCompanyCode,
					FireMarshalExtract.adjusterCompanyName = SOURCE.adjusterCompanyName,
					FireMarshalExtract.companyName = SOURCE.companyName,
					FireMarshalExtract.affiliate1Code = SOURCE.affiliate1Code,
					FireMarshalExtract.affiliate1Name = SOURCE.affiliate1Name,
					FireMarshalExtract.affiliate2Code = SOURCE.affiliate2Code,
					FireMarshalExtract.affiliate2Name = SOURCE.affiliate2Name,
					FireMarshalExtract.groupCode = SOURCE.groupCode,
					FireMarshalExtract.groupName = SOURCE.groupName,
					FireMarshalExtract.lossAddressLine1 = SOURCE.lossAddressLine1,
					FireMarshalExtract.lossAddressLine2 = SOURCE.lossAddressLine2,
					FireMarshalExtract.lossCityName = SOURCE.lossCityName,
					FireMarshalExtract.lossStateCode = SOURCE.lossStateCode,
					FireMarshalExtract.lossStateName = SOURCE.lossStateName,
					FireMarshalExtract.lossGeoCounty = SOURCE.lossGeoCounty,
					FireMarshalExtract.lossZipCode = SOURCE.lossZipCode,
					FireMarshalExtract.lossLatitude = SOURCE.lossLatitude,
					FireMarshalExtract.lossLongitude = SOURCE.lossLongitude,
					FireMarshalExtract.lossDescription = SOURCE.lossDescription,
					FireMarshalExtract.lossDescriptionExtended = SOURCE.lossDescriptionExtended,
					FireMarshalExtract.dateOfLoss = SOURCE.dateOfLoss,
					FireMarshalExtract.lossTypeCode = SOURCE.lossTypeCode,
					FireMarshalExtract.lossTypeDescription = SOURCE.lossTypeDescription,
					FireMarshalExtract.policyTypeCode = SOURCE.policyTypeCode,
					FireMarshalExtract.policyTypeDescription = SOURCE.policyTypeDescription,
					FireMarshalExtract.coverageTypeCode = SOURCE.coverageTypeCode,
					FireMarshalExtract.coverageTypeDescription = SOURCE.coverageTypeDescription,
					FireMarshalExtract.settlementAmount = SOURCE.settlementAmount,
					FireMarshalExtract.estimatedLossAmount = SOURCE.estimatedLossAmount,
					FireMarshalExtract.policyAmount = SOURCE.policyAmount,
					FireMarshalExtract.buildingPaidAmount = SOURCE.buildingPaidAmount,
					FireMarshalExtract.contentReserveAmount = SOURCE.contentReserveAmount,
					FireMarshalExtract.contentPaidAmount = SOURCE.contentPaidAmount,
					FireMarshalExtract.isIncendiaryFire = SOURCE.isIncendiaryFire,
					FireMarshalExtract.isClaimUnderSIUInvestigation = SOURCE.isClaimUnderSIUInvestigation,
					FireMarshalExtract.siuCompanyName = SOURCE.siuCompanyName,
					FireMarshalExtract.siuRepresentativeFullName = SOURCE.siuRepresentativeFullName,
					FireMarshalExtract.siuWorkPhoneNumber = SOURCE.siuWorkPhoneNumber,
					FireMarshalExtract.siuCellPhoneNumber = SOURCE.siuCellPhoneNumber,
					FireMarshalExtract.isActive = SOURCE.isActive,
					FireMarshalExtract.isCurrent = SOURCE.isCurrent,
					FireMarshalExtract.involvedPartyId = SOURCE.involvedPartyId,
					FireMarshalExtract.adjusterId = SOURCE.adjusterId,
					FireMarshalExtract.involvedPartySequenceId = SOURCE.involvedPartySequenceId,
					FireMarshalExtract.dateInserted = @dateInserted
			FROM
				#PendingFMExtractDataToInsert AS SOURCE
			WHERE
				FireMarshalExtract.elementalClaimId = SOURCE.elementalClaimId
				AND
				(
					ISNULL(FireMarshalExtract.isoFileNumber,'~~~') <> ISNULL(SOURCE.isoFileNumber,'~~~')
					OR ISNULL(FireMarshalExtract.reportingStatus,'~~~') <> ISNULL(SOURCE.reportingStatus,'~~~')
					OR ISNULL(FireMarshalExtract.fMstatus,'~~~') <> ISNULL(SOURCE.fMstatus,'~~~')
					OR ISNULL(FireMarshalExtract.fMDate,'99990101') <> ISNULL(SOURCE.fMDate,'99990101')
					OR FireMarshalExtract.claimIsOpen <> SOURCE.claimIsOpen
					OR ISNULL(FireMarshalExtract.dateSubmittedToIso,'99990101') <> ISNULL(SOURCE.dateSubmittedToIso,'99990101')
					OR ISNULL(FireMarshalExtract.originalClaimNumber,'~~~') <> ISNULL(SOURCE.originalClaimNumber,'~~~')
					OR FireMarshalExtract.originalPolicyNumber <> SOURCE.originalPolicyNumber
					OR FireMarshalExtract.insuranceProviderOfficeCode <> SOURCE.insuranceProviderOfficeCode
					OR FireMarshalExtract.insuranceProviderCompanyCode <> SOURCE.insuranceProviderCompanyCode
					OR ISNULL(FireMarshalExtract.adjusterCompanyCode,'~~~~') <> ISNULL(SOURCE.adjusterCompanyCode,'~~~~')
					OR ISNULL(FireMarshalExtract.adjusterCompanyName,'~~~') <> ISNULL(SOURCE.adjusterCompanyName,'~~~')
					OR FireMarshalExtract.companyName <> SOURCE.companyName
					OR FireMarshalExtract.affiliate1Code <> SOURCE.affiliate1Code
					OR FireMarshalExtract.affiliate1Name <> SOURCE.affiliate1Name
					OR FireMarshalExtract.affiliate2Code <> SOURCE.affiliate2Code
					OR FireMarshalExtract.affiliate2Name <> SOURCE.affiliate2Name
					OR FireMarshalExtract.groupCode <> SOURCE.groupCode
					OR FireMarshalExtract.groupName <> SOURCE.groupName
					OR ISNULL(FireMarshalExtract.lossAddressLine1,'~~~') <> ISNULL(SOURCE.lossAddressLine1,'~~~')
					OR ISNULL(FireMarshalExtract.lossAddressLine2,'~~~') <> ISNULL(SOURCE.lossAddressLine2,'~~~')
					OR ISNULL(FireMarshalExtract.lossCityName,'~~~') <> ISNULL(SOURCE.lossCityName,'~~~')
					OR ISNULL(FireMarshalExtract.lossStateCode,'~~') <> ISNULL(SOURCE.lossStateCode,'~~')
					OR ISNULL(FireMarshalExtract.lossStateName,'~~~') <> ISNULL(SOURCE.lossStateName,'~~~')
					OR ISNULL(FireMarshalExtract.lossGeoCounty,'~~~') <> ISNULL(SOURCE.lossGeoCounty,'~~~')
					OR ISNULL(FireMarshalExtract.lossZipCode,'~~~') <> ISNULL(SOURCE.lossZipCode,'~~~')
					OR ISNULL(FireMarshalExtract.lossLatitude,'~~~') <> ISNULL(SOURCE.lossLatitude,'~~~')
					OR ISNULL(FireMarshalExtract.lossLongitude,'~~~') <> ISNULL(SOURCE.lossLongitude,'~~~')
					OR ISNULL(FireMarshalExtract.lossDescription,'~~~') <> ISNULL(SOURCE.lossDescription,'~~~')
					OR ISNULL(FireMarshalExtract.lossDescriptionExtended,'~~~') <> ISNULL(SOURCE.lossDescriptionExtended,'~~~')
					OR ISNULL(FireMarshalExtract.dateOfLoss,'99990101') <> ISNULL(SOURCE.dateOfLoss,'99990101')
					OR ISNULL(FireMarshalExtract.lossTypeCode,'~~~~') <> ISNULL(SOURCE.lossTypeCode,'~~~~')
					OR ISNULL(FireMarshalExtract.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					OR ISNULL(FireMarshalExtract.policyTypeCode,'~~~~') <> ISNULL(SOURCE.policyTypeCode,'~~~~')
					OR ISNULL(FireMarshalExtract.policyTypeDescription,'~~~') <> ISNULL(SOURCE.policyTypeDescription,'~~~')
					OR ISNULL(FireMarshalExtract.coverageTypeCode,'~~~~') <> ISNULL(SOURCE.coverageTypeCode,'~~~~')
					OR ISNULL(FireMarshalExtract.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(FireMarshalExtract.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
					OR ISNULL(FireMarshalExtract.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
					OR ISNULL(FireMarshalExtract.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
					OR ISNULL(FireMarshalExtract.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(FireMarshalExtract.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(FireMarshalExtract.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR FireMarshalExtract.isIncendiaryFire <> SOURCE.isIncendiaryFire
					OR FireMarshalExtract.isClaimUnderSIUInvestigation <> SOURCE.isClaimUnderSIUInvestigation
					OR ISNULL(FireMarshalExtract.siuCompanyName,'~~~') <> ISNULL(SOURCE.siuCompanyName,'~~~')
					OR ISNULL(FireMarshalExtract.siuRepresentativeFullName,'~~~') <> ISNULL(SOURCE.siuRepresentativeFullName,'~~~')
					OR ISNULL(FireMarshalExtract.siuWorkPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuWorkPhoneNumber,'~~~~~~~~~~')
					OR ISNULL(FireMarshalExtract.siuCellPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuCellPhoneNumber,'~~~~~~~~~~')
					OR FireMarshalExtract.isActive <> SOURCE.isActive
					OR FireMarshalExtract.isCurrent <> SOURCE.isCurrent
					OR FireMarshalExtract.involvedPartyId <> SOURCE.involvedPartyId
					OR ISNULL(FireMarshalExtract.involvedPartyFullName,'~~~~') <> ISNULL(SOURCE.involvedPartyFullName,'~~~~')
					OR ISNULL(FireMarshalExtract.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					OR ISNULL(FireMarshalExtract.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
				);
				
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalExtractActivityLog
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
				@stepDescription = 'Insert new FMExtract record',
				@stepStartDateTime = GETDATE();
				
			INSERT INTO dbo.FireMarshalExtract
			(
				elementalClaimId,
				claimId,
				/*uniqueInstanceValue,*/
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
				involvedPartyFullName,
				adjusterId,
				involvedPartySequenceId,
				dateInserted
			)
			SELECT
				SOURCE.elementalClaimId,
				SOURCE.claimId,
				/*SOURCE.uniqueInstanceValue,*/
				SOURCE.isoFileNumber,
				SOURCE.reportingStatus,
				SOURCE.fMstatus,
				SOURCE.fMDate,
				SOURCE.claimIsOpen,
				SOURCE.dateSubmittedToIso,
				SOURCE.originalClaimNumber,
				SOURCE.originalPolicyNumber,
				SOURCE.insuranceProviderOfficeCode,
				SOURCE.insuranceProviderCompanyCode,
				SOURCE.adjusterCompanyCode,
				SOURCE.adjusterCompanyName,
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
				SOURCE.lossGeoCounty,
				SOURCE.lossZipCode,
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
				SOURCE.settlementAmount,
				SOURCE.estimatedLossAmount,
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
				SOURCE.isActive,
				SOURCE.isCurrent,
				SOURCE.involvedPartyId,
				SOURCE.involvedPartyFullName,
				SOURCE.adjusterId,
				SOURCE.involvedPartySequenceId,
				@dateInserted AS dateInserted
			FROM
				#PendingFMExtractDataToInsert AS SOURCE
				LEFT OUTER JOIN dbo.FireMarshalExtract
					ON FireMarshalExtract.elementalClaimId = SOURCE.elementalClaimId
			WHERE
				FireMarshalExtract.elementalClaimId IS NULL;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FireMarshalExtractActivityLog
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
				@stepId = 4,
				@stepDescription = 'Insert previously "soft" deleted FMExtract record;', /*workaround for bad delta logic in Delta between vader and claimSearchProd*/
				@stepStartDateTime = GETDATE();
			
			INSERT INTO dbo.FireMarshalExtract
			(
				elementalClaimId,
				claimId,
				/*uniqueInstanceValue,*/
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
				involvedPartyFullName,
				adjusterId,
				involvedPartySequenceId,
				dateInserted
			)
			SELECT
				SOURCE.elementalClaimId,
				SOURCE.claimId,
				/*SOURCE.uniqueInstanceValue,*/
				SOURCE.isoFileNumber,
				SOURCE.reportingStatus,
				SOURCE.fireMarshallStatus AS fMstatus,
				SOURCE.fireMarshallDate AS fMDate,
				SOURCE.claimIsOpen,
				SOURCE.dateSubmittedToIso,
				SOURCE.originalClaimNumber,
				SOURCE.originalPolicyNumber,
				SOURCE.insuranceProviderOfficeCode,
				SOURCE.insuranceProviderCompanyCode,

				V_ActiveAdjuster.adjusterCompanyCode,
				V_ActiveAdjuster.adjusterCompanyName,

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
				SOURCE.lossGeoCounty,
				SOURCE.lossZipCode,
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
				SOURCE.settlementAmount,
				SOURCE.estimatedLossAmount,
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
				SOURCE.isActive,
				SOURCE.isCurrent,
				SOURCE.involvedPartyId,
				SOURCE.involvedPartyFullName,
				SOURCE.adjusterId,
				SOURCE.involvedPartySequenceId,
				@dateInserted AS dateInserted
			FROM
				dbo.FireMarshalPendingClaim AS SOURCE
				LEFT OUTER JOIN dbo.V_ActiveAdjuster
					ON SOURCE.adjusterId = V_ActiveAdjuster.adjusterId
				/*
				!!!!DEV-NOTE: The Check against #PendingFMExtractDataToInsert is CRITICAL.
						It is the first line of defense against adding rows to the FireMarshalExtract table
						for Claims that were "deactivated" as they transitioned from "Pending" to "Sent".
				*/	
				LEFT OUTER JOIN #PendingFMExtractDataToInsert
					ON SOURCE.elementalClaimId = #PendingFMExtractDataToInsert.elementalClaimId
				LEFT OUTER JOIN dbo.FireMarshalExtract
					ON SOURCE.elementalClaimId = FireMarshalExtract.elementalClaimId
			WHERE
				#PendingFMExtractDataToInsert.elementalClaimId IS NULL
				AND FireMarshalExtract.elementalClaimId IS NULL
				AND SOURCE.isCurrent = 1
				AND SOURCE.isActive = 0;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FireMarshalExtractActivityLog
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
				@stepDescription = 'DeActivate Stale FMExtract record',
				@stepStartDateTime = GETDATE();
				
			UPDATE dbo.FireMarshalExtract
			SET
				FireMarshalExtract.isActive = 0,
				FireMarshalExtract.dateInserted = @dateInserted
			WHERE
				FireMarshalExtract.isActive = 1
				AND NOT EXISTS (
					SELECT
						NULL
					FROM
						#PendingFMExtractDataToInsert
					WHERE
						#PendingFMExtractDataToInsert.elementalClaimId = FireMarshalExtract.elementalClaimId
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
				/*Log Activity*/
			INSERT INTO dbo.FireMarshalExtractActivityLog
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
		INSERT INTO dbo.FireMarshalExtractActivityLog
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