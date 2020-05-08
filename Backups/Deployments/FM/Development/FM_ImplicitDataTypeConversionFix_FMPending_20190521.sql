SET NOEXEC OFF;

USE ClaimSearch_Prod

BEGIN TRANSACTION

DROP VIEW dbo.V_ActiveCurrentPendingFMClaim;
DROP TABLE dbo.FireMarshalPendingClaim;

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-04-30
Author: Robert David Warner
Description: System of captureing both current Snapshot of Fire Claim(s)
			 to be sent to FireMarshal(s), as well as their changes over time.

				Updated daily /*20190425*/,
				Records in this object should be mutually exclusive with
				the FireMarshalClaimSendHistory

			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalPendingClaim
(   
	elementalClaimId BIGINT NOT NULL,
	uniqueInstanceValue TINYINT NOT NULL,
	
	claimId BIGINT NOT NULL,
	isoFileNumber VARCHAR(11) NULL, /*isoClaimId*/
	
	reportingStatus VARCHAR(25) NULL, 
	fireMarshallStatus VARCHAR(255) NULL,
	fireMarshallDate DATE NULL,
	claimIsOpen BIT NOT NULL,
	dateSubmittedToIso DATE NULL,
	
	originalClaimNumber VARCHAR(30) NULL,
	originalPolicyNumber VARCHAR(30) NOT NULL,
	
	insuranceProviderOfficeCode CHAR(5) NOT NULL,
	insuranceProviderCompanyCode CHAR(4) NOT NULL,
	companyName VARCHAR(75) NOT NULL,
	affiliate1Code CHAR(4) NOT NULL,
	affiliate1Name VARCHAR(75) NOT NULL,
	affiliate2Code CHAR(4) NOT NULL,
	affiliate2Name VARCHAR(75) NOT NULL,
	groupCode CHAR(4) NOT NULL,
	groupName VARCHAR(75) NOT NULL,
	
	lossAddressLine1 VARCHAR(50) NULL,
	lossAddressLine2 VARCHAR(50) NULL,
	lossCityName VARCHAR(25) NULL,
	
	lossStateCode CHAR(2) NULL,
	lossStateName VARCHAR(50) NULL,
	
	lossZipCode	VARCHAR(9) NULL,
	lossGeoCounty VARCHAR(25) NULL,
	
	lossLatitude VARCHAR(15) NULL,
	lossLongitude VARCHAR(15) NULL,
	
	lossDescription VARCHAR(50) NULL,
	lossDescriptionExtended VARCHAR(200) NULL,
	
	dateOfLoss DATE NULL,
	lossTypeCode CHAR(4) NULL,
	lossTypeDescription VARCHAR(42) NULL,
	policyTypeCode CHAR(4) NULL,
	policyTypeDescription VARCHAR(100) NULL,
	coverageTypeCode CHAR(4) NULL,
	coverageTypeDescription VARCHAR(42) NULL,
	
	estimatedLossAmount MONEY NULL,
	settlementAmount MONEY NULL,
	policyAmount MONEY NULL,
	
	buildingPaidAmount MONEY NULL,
	contentReserveAmount MONEY NULL,
	contentPaidAmount MONEY NULL,
	
	isIncendiaryFire BIT NOT NULL,
	
	isClaimUnderSIUInvestigation BIT NULL,
	siuCompanyName VARCHAR(70) NULL,
	siuRepresentativeFullName VARCHAR(250) NULL,
	siuWorkPhoneNumber CHAR(10) NULL,
	siuCellPhoneNumber CHAR(10) NULL,
	
	involvedPartyId BIGINT NOT NULL,
	adjusterId BIGINT NULL,
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	
	/*dateClaimClosed DATE NULL,*/
	/*coverageStatus VARCHAR(3) NULL,*/
	
	reserveAmount MONEY NULL,
	totalInsuredAmount MONEY NULL,
	replacementAmount MONEY NULL,
	actualCashAmount MONEY NULL,
	buildingPolicyAmount MONEY NULL,
	buildingTotalInsuredAmount MONEY NULL,
	buildingReplacementAmount MONEY NULL,
	buildingActualCashAmount MONEY NULL,
	buildingEstimatedLossAmount MONEY NULL,
	contentPolicyAmount MONEY NULL,
	contentTotalInsuredAmount MONEY NULL,
	contentReplacementAmount MONEY NULL,
	contentActualCashAmount MONEY NULL,
	contentEstimatedLossAmount MONEY NULL,
	stockPolicyAmount MONEY NULL,
	stockTotalInsuredAmount MONEY NULL,
	stockReplacementAmount MONEY NULL,
	stockActualCashAmount MONEY NULL,
	stockEstimatedLossAmount MONEY NULL,
	lossOfUsePolicyAmount MONEY NULL,
	lossOfUseTotalInsuredAmount MONEY NULL,
	lossOfUseReplacementAmount MONEY NULL,
	lossOfUseActualCashAmount MONEY NULL,
	lossOfUseEstimatedLossAmount MONEY NULL,
	otherPolicyAmount MONEY NULL,
	otherTotalInsuredAmount MONEY NULL,
	otherReplacementAmount MONEY NULL,
	otherActualCashAmount MONEY NULL,
	otherEstimatedLossAmount MONEY NULL,
	buildingReserveAmount MONEY NULL,
	
	stockReserveAmount MONEY NULL,
	stockPaidAmount MONEY NULL,
	lossOfUseReserve MONEY NULL,
	lossOfUsePaid MONEY NULL,
	otherReserveAmount MONEY NULL,
	otherPaidAmount MONEY NULL,

	isActive BIT NOT NULL,
	isCurrent BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL
	CONSTRAINT PK_FireMarshalPendingClaim_elementalClaimId_uniqueInstanceValue
		PRIMARY KEY CLUSTERED (elementalClaimId, uniqueInstanceValue)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_FireMarshalPendingClaim_isCurrent_isActive
	ON dbo.FireMarshalPendingClaim (isCurrent, isActive)
	INCLUDE (claimId, isoFileNumber, reportingStatus, fireMarshallStatus, fireMarshallDate, claimIsOpen, dateSubmittedToIso, originalClaimNumber, originalPolicyNumber, insuranceProviderOfficeCode, insuranceProviderCompanyCode, companyName, affiliate1Code, affiliate1Name, affiliate2Code, affiliate2Name, groupCode, groupName, lossAddressLine1, lossAddressLine2, lossCityName, lossStateCode, lossStateName, lossZipCode, lossGeoCounty, lossLatitude, lossLongitude, lossDescription, lossDescriptionExtended, dateOfLoss,
			lossTypeCode, lossTypeDescription, policyTypeCode, policyTypeDescription, coverageTypeCode, coverageTypeDescription, settlementAmount, estimatedLossAmount, policyAmount, isIncendiaryFire, isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName, siuWorkPhoneNumber, siuCellPhoneNumber, involvedPartyId, adjusterId, involvedPartySequenceId, reserveAmount, totalInsuredAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount, buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount,
			contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount, stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount, otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve, lossOfUsePaid, otherReserveAmount, otherPaidAmount, dateInserted);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-04-30
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
************************************************/
CREATE VIEW dbo.V_ActiveCurrentPendingFMClaim
WITH SCHEMABINDING
AS
(
	SELECT
		FireMarshalPendingClaim.elementalClaimId,
		FireMarshalPendingClaim.uniqueInstanceValue,
		FireMarshalPendingClaim.claimId,
		FireMarshalPendingClaim.isoFileNumber,
		FireMarshalPendingClaim.reportingStatus,
		FireMarshalPendingClaim.fireMarshallStatus,
		FireMarshalPendingClaim.fireMarshallDate,
		FireMarshalPendingClaim.claimIsOpen,
		FireMarshalPendingClaim.dateSubmittedToIso,
		FireMarshalPendingClaim.originalClaimNumber,
		FireMarshalPendingClaim.originalPolicyNumber,
		FireMarshalPendingClaim.insuranceProviderOfficeCode,
		FireMarshalPendingClaim.insuranceProviderCompanyCode,
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
		FireMarshalPendingClaim.estimatedLossAmount,
		FireMarshalPendingClaim.settlementAmount,
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
		FireMarshalPendingClaim.involvedPartyId,
		FireMarshalPendingClaim.adjusterId,
		FireMarshalPendingClaim.involvedPartySequenceId,
		FireMarshalPendingClaim.reserveAmount,
		FireMarshalPendingClaim.totalInsuredAmount,
		FireMarshalPendingClaim.replacementAmount,
		FireMarshalPendingClaim.actualCashAmount,
		FireMarshalPendingClaim.buildingPolicyAmount,
		FireMarshalPendingClaim.buildingTotalInsuredAmount,
		FireMarshalPendingClaim.buildingReplacementAmount,
		FireMarshalPendingClaim.buildingActualCashAmount,
		FireMarshalPendingClaim.buildingEstimatedLossAmount,
		FireMarshalPendingClaim.contentPolicyAmount,
		FireMarshalPendingClaim.contentTotalInsuredAmount,
		FireMarshalPendingClaim.contentReplacementAmount,
		FireMarshalPendingClaim.contentActualCashAmount,
		FireMarshalPendingClaim.contentEstimatedLossAmount,
		FireMarshalPendingClaim.stockPolicyAmount,
		FireMarshalPendingClaim.stockTotalInsuredAmount,
		FireMarshalPendingClaim.stockReplacementAmount,
		FireMarshalPendingClaim.stockActualCashAmount,
		FireMarshalPendingClaim.stockEstimatedLossAmount,
		FireMarshalPendingClaim.lossOfUsePolicyAmount,
		FireMarshalPendingClaim.lossOfUseTotalInsuredAmount,
		FireMarshalPendingClaim.lossOfUseReplacementAmount,
		FireMarshalPendingClaim.lossOfUseActualCashAmount,
		FireMarshalPendingClaim.lossOfUseEstimatedLossAmount,
		FireMarshalPendingClaim.otherPolicyAmount,
		FireMarshalPendingClaim.otherTotalInsuredAmount,
		FireMarshalPendingClaim.otherReplacementAmount,
		FireMarshalPendingClaim.otherActualCashAmount,
		FireMarshalPendingClaim.otherEstimatedLossAmount,
		FireMarshalPendingClaim.buildingReserveAmount,
		FireMarshalPendingClaim.stockReserveAmount,
		FireMarshalPendingClaim.stockPaidAmount,
		FireMarshalPendingClaim.lossOfUseReserve,
		FireMarshalPendingClaim.lossOfUsePaid,
		FireMarshalPendingClaim.otherReserveAmount,
		FireMarshalPendingClaim.otherPaidAmount,
		FireMarshalPendingClaim.dateInserted
	FROM
		dbo.FireMarshalPendingClaim
	WHERE
		FireMarshalPendingClaim.isActive = 1
		AND FireMarshalPendingClaim.isCurrent = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN	
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveCurrentPendingFMClaim_elementalClaimId_uniqueInstanceValue
	ON dbo.V_ActiveCurrentPendingFMClaim (elementalClaimId, uniqueInstanceValue)
	WITH (FILLFACTOR = 80);
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
ALTER PROCEDURE dbo.hsp_UpdateInsertFMPendingClaim
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
				FROM dbo.FMPendingClaimActivityLog
				WHERE
					FMPendingClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FMPendingClaimActivityLog.isSuccessful = 1
					AND FMPendingClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
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

			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'UpdateFMControllerProjectionDate',
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
				AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< @dateInserted
				AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('00010101' AS DATE)) <> DATEADD(
					MONTH,
					ProjectedDateToIncrementApply.projectedDateIncrementValue,
					COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
				);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@stepDescription = 'CaptureFMPendingClaimDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
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
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@stepDescription = 'IdentifyClaimQualificationExceptions',
				@stepStartDateTime = GETDATE();
			
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
												ISNULL(InnerPendingFMClaimDataToInsert.lossDescription,'Fire') NOT IN (
													'Fire',
													'',
													'blank'
												)
												AND ISNULL(InnerPendingFMClaimDataToInsert.lossDescriptionExtended,'Fire') NOT IN (
													'Fire',
													'',
													'blank'
												)
											THEN
												CAST('Estimated and/or Settlement amount missing' AS VARCHAR(255))
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
										ISNULL(InnerPendingFMClaimDataToInsert.lossDescription,'Fire') NOT IN (
											'Fire',
											'',
											'blank'
										)
										AND ISNULL(InnerPendingFMClaimDataToInsert.lossDescriptionExtended,'Fire') NOT IN (
											'Fire',
											'',
											'blank'
										)
										AND (
											ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
											OR ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
										)
									THEN
										CAST('Loss description invalid and Estimated and/or Settlement amount missing' AS VARCHAR(255))
									WHEN
										ISNULL(InnerPendingFMClaimDataToInsert.lossDescription,'Fire') NOT IN (
											'Fire',
											'',
											'blank'
										)
										AND ISNULL(InnerPendingFMClaimDataToInsert.lossDescriptionExtended,'Fire') NOT IN (
											'Fire',
											'',
											'blank'
										)
									THEN
										CAST('Loss description invalid' AS VARCHAR(255))
									WHEN
										ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
										OR ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
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

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@stepDescription = 'MergeIntoFMDriverData,Update,Deactivate,Insert',
				@stepStartDateTime = GETDATE();
			
			MERGE INTO dbo.FireMarshalPendingClaim AS TARGET
			USING(
				SELECT
					#PendingFMClaimDataToInsert.elementalClaimId,
					#PendingFMClaimDataToInsert.uniqueInstanceValue,
					#PendingFMClaimDataToInsert.claimId,
					#PendingFMClaimDataToInsert.isoFileNumber,
					#PendingFMClaimDataToInsert.reportingStatus,
					#PendingFMClaimDataToInsert.fireMarshallStatus,
					/*#PendingFMClaimDataToInsert.fireMarshallDate,*/
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
					#PendingFMClaimDataToInsert.otherPaidAmount
					/*#PendingFMClaimDataToInsert.isActive,*/
					/*#PendingFMClaimDataToInsert.isCurrent,*/
					/*#PendingFMClaimDataToInsert.dateInserted*/
				FROM
					#PendingFMClaimDataToInsert
			) AS SOURCE
				ON TARGET.elementalClaimId = SOURCE.elementalClaimId
					AND TARGET.uniqueInstanceValue = SOURCE.uniqueInstanceValue
			WHEN NOT MATCHED BY TARGET
			THEN
				INSERT
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
				VALUES
				(
					SOURCE.elementalClaimId,
					SOURCE.uniqueInstanceValue,
					SOURCE.claimId,
					SOURCE.isoFileNumber,
					SOURCE.reportingStatus,
					SOURCE.fireMarshallStatus,
					@dateInserted /*fireMarshallDate*/,
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
					1 /*isActive*/,
					1 /*isCurrent*/,
					@dateInserted
				)
			WHEN MATCHED
				AND (
					TARGET.claimId <> SOURCE.claimId
					OR ISNULL(TARGET.isoFileNumber,'~~~') <> ISNULL(SOURCE.isoFileNumber,'~~~')
					OR ISNULL(TARGET.reportingStatus,'~~~') <> ISNULL(SOURCE.reportingStatus,'~~~')
					OR ISNULL(TARGET.fireMarshallStatus,'~~~') <> ISNULL(SOURCE.fireMarshallStatus,'~~~')
					OR  TARGET.claimIsOpen <> SOURCE.claimIsOpen
					OR CAST(ISNULL(TARGET.dateSubmittedToIso,'99990101')AS DATE) <> CAST(ISNULL(SOURCE.dateSubmittedToIso,'99990101') AS DATE)
					OR ISNULL(TARGET.originalClaimNumber,'~~~') <> ISNULL(SOURCE.originalClaimNumber,'~~~')
					OR TARGET.originalPolicyNumber <> SOURCE.originalPolicyNumber
					OR TARGET.insuranceProviderOfficeCode <> SOURCE.insuranceProviderOfficeCode
					OR TARGET.insuranceProviderCompanyCode <> SOURCE.insuranceProviderCompanyCode
					OR TARGET.companyName <> SOURCE.companyName
					OR TARGET.affiliate1Code <> SOURCE.affiliate1Code
					OR TARGET.affiliate1Name <> SOURCE.affiliate1Name
					OR TARGET.affiliate2Code <> SOURCE.affiliate2Code
					OR TARGET.affiliate2Name <> SOURCE.affiliate2Name
					OR TARGET.groupCode <> SOURCE.groupCode
					OR TARGET.groupName <> SOURCE.groupName
					OR ISNULL(TARGET.lossAddressLine1,'~~~') <> ISNULL(SOURCE.lossAddressLine1,'~~~')
					OR ISNULL(TARGET.lossAddressLine2,'~~~') <> ISNULL(SOURCE.lossAddressLine2,'~~~')
					OR ISNULL(TARGET.lossCityName,'~~~') <> ISNULL(SOURCE.lossCityName,'~~~')
					OR ISNULL(TARGET.lossStateCode,'~~') <> ISNULL(SOURCE.lossStateCode,'~~')
					OR ISNULL(TARGET.lossStateName,'~~~') <> ISNULL(SOURCE.lossStateName,'~~~')
					OR ISNULL(TARGET.lossZipCode,'~~~~~') <> ISNULL(SOURCE.lossZipCode,'~~~~~')
					OR ISNULL(TARGET.lossGeoCounty,'~~~') <> ISNULL(SOURCE.lossGeoCounty,'~~~')
					OR ISNULL(TARGET.lossLatitude,'~~~') <> ISNULL(SOURCE.lossLatitude,'~~~')
					OR ISNULL(TARGET.lossLongitude,'~~~') <> ISNULL(SOURCE.lossLongitude,'~~~')
					OR ISNULL(TARGET.lossDescription,'~~~') <> ISNULL(SOURCE.lossDescription,'~~~')
					OR ISNULL(TARGET.lossDescriptionExtended,'~~~') <> ISNULL(SOURCE.lossDescriptionExtended,'~~~')
					OR CAST(ISNULL(TARGET.dateOfLoss,'99990101') AS DATE) <> CAST(ISNULL(SOURCE.dateOfLoss,'99990101') AS DATE)
					OR ISNULL(TARGET.lossTypeCode,'~~~~') <> ISNULL(SOURCE.lossTypeCode,'~~~~')
					OR ISNULL(TARGET.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					OR ISNULL(TARGET.policyTypeCode,'~~~~') <> ISNULL(SOURCE.policyTypeCode,'~~~~')
					OR ISNULL(TARGET.policyTypeDescription,'~~~') <> ISNULL(SOURCE.policyTypeDescription,'~~~')
					OR ISNULL(TARGET.coverageTypeCode,'~~~~') <> ISNULL(SOURCE.coverageTypeCode,'~~~~')
					OR ISNULL(TARGET.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(TARGET.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
					OR ISNULL(TARGET.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
					OR ISNULL(TARGET.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
					OR ISNULL(TARGET.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(TARGET.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(TARGET.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR TARGET.isIncendiaryFire <> SOURCE.isIncendiaryFire
					OR ISNULL(TARGET.isClaimUnderSIUInvestigation,0) <> ISNULL(SOURCE.isClaimUnderSIUInvestigation,0)
					OR ISNULL(TARGET.siuCompanyName,'~~~') <> ISNULL(SOURCE.siuCompanyName,'~~~')
					OR ISNULL(TARGET.siuRepresentativeFullName,'~~~') <> ISNULL(SOURCE.siuRepresentativeFullName,'~~~')
					OR ISNULL(TARGET.siuWorkPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuWorkPhoneNumber,'~~~~~~~~~~')
					OR ISNULL(TARGET.siuCellPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuCellPhoneNumber,'~~~~~~~~~~')
					OR TARGET.involvedPartyId <> SOURCE.involvedPartyId
					OR ISNULL(TARGET.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					OR ISNULL(TARGET.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
					OR ISNULL(TARGET.reserveAmount,-1) <> ISNULL(SOURCE.reserveAmount,-1)
					OR ISNULL(TARGET.totalInsuredAmount,-1) <> ISNULL(SOURCE.totalInsuredAmount,-1)
					OR ISNULL(TARGET.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(TARGET.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(TARGET.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(TARGET.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(TARGET.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(TARGET.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(TARGET.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(TARGET.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(TARGET.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(TARGET.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(TARGET.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(TARGET.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(TARGET.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(TARGET.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(TARGET.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(TARGET.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(TARGET.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(TARGET.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(TARGET.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(TARGET.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(TARGET.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(TARGET.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(TARGET.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(TARGET.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(TARGET.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(TARGET.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(TARGET.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(TARGET.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(TARGET.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(TARGET.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(TARGET.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(TARGET.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(TARGET.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(TARGET.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
				)
			THEN UPDATE
				SET Target.isCurrent = 0
			OUTPUT
				SOURCE.elementalClaimId,
				SOURCE.uniqueInstanceValue+1,
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
				INTO dbo.FireMarshalPendingClaim;
			/*
			WHEN NOT MATCHED BY SOURCE
			THEN
				Do nothing. this is not an exhaustive set each execute.
				--UPDATE
				--SET
				--	TARGET.isActive = 0
			*/
			
			
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
		INSERT INTO dbo.FMPendingClaimActivityLog
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