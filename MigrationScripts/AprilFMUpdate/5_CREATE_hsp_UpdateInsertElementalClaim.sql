SET NOEXEC OFF;

--USE ClaimSearch_Dev;
USE ClaimSearch_Prod;

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
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for data-refresh of the ElementalClaim Table.
			
************************************************/
CREATE PROCEDURE dbo.hsp_UpdateInsertElementalClaim
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.ElementalClaimActivityLog
				WHERE
					ElementalClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND ElementalClaimActivityLog.isSuccessful = 1
					AND ElementalClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
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

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(ElementalClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.ElementalClaimActivityLog
			WHERE
				ElementalClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND ElementalClaimActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'Capture Elemental Claim DataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				MAX(ExistingElementalClaimRecord.elementalClaimId) AS elementalClaimId,
				V_ActiveIPAddressMap.claimId,
				V_ActiveIPAddressMap.involvedPartyId,
				MAX(V_ActiveAdjuster.adjusterId) AS adjusterId,
				CAST(CLT00014.C_LOSS_TYP AS CHAR(4)) AS lossType,
				CAST(CLT00014.C_CVG_TYP AS CHAR(4)) AS coverageType,
				MAX(CAST(CLT00014.D_CLM_CLOSE AS DATE)) AS dateClaimClosed, /*D_CLM_CLOSE*/
				CAST(CLT00014.C_CLM_STUS AS VARCHAR(3)) AS coverageStatus, /*C_CLM_STUS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_PD + CLT00017.A_CNNT_PD + CLT00017.A_STK_PD + CLT00017.A_USE_PD + CLT00017.A_OTH_PD AS MONEY),
						CAST(CLT00014.A_STTLMT AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS settlementAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_EST_LOSS + CLT00017.A_CNTT_EST_LOSS + CLT00017.A_STK_EST_LOSS + CLT00017.A_USE_EST_LOSS + CLT00017.A_OTH_EST_LOSS AS MONEY),
						CAST(CLT00014.A_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS estimatedLossAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RSRV + CLT00017.A_CNNT_RSRV + CLT00017.A_STK_RSRV + CLT00017.A_USE_RSRV + CLT00017.A_OTH_RSRV AS MONEY),
						CAST(CLT00014.A_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS reserveAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_TL_INS + CLT00017.A_CNTT_TL_INS + CLT00017.A_STK_TL_INS + CLT00017.A_USE_TL_INS + CLT00017.A_OTH_TL_INS AS MONEY),
						CAST(CLT00014.A_INDM AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS totalInsuredAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_POL + CLT00017.A_CNTT_POL + CLT00017.A_STK_POL + CLT00017.A_USE_POL + CLT00017.A_OTH_POL AS MONEY),
						CAST(CLT00014.A_CVG_TL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS policyAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RPLCMT_VAL + CLT00017.A_CNTT_RPLCMT_VAL + CLT00017.A_STK_RPLCMT_VAL + CLT00017.A_USE_RPLCMT_VAL+ CLT00017.A_OTH_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)	
				) AS replacementAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_ACTL_VAL + CLT00017.A_CNTT_ACTL_VAL + CLT00017.A_STK_ACTL_VAL + CLT00017.A_USE_ACTL_VAL + CLT00017.A_OTH_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS actualCashAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingPolicyAmount, /*A_BLDG_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingTotalInsuredAmount, /*A_BLDG_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingReplacementAmount, /*A_BLDG_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingActualCashAmount, /*A_BLDG_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingEstimatedLossAmount, /*A_BLDG_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentPolicyAmount, /*A_CNTT_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentTotalInsuredAmount, /*A_CNTT_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentReplacementAmount, /*A_CNTT_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentActualCashAmount, /*A_CNTT_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentEstimatedLossAmount, /*A_CNTT_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockPolicyAmount, /*A_STK_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockTotalInsuredAmount, /*A_STK_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockReplacementAmount, /*A_STK_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockActualCashAmount, /*A_STK_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockEstimatedLossAmount, /*A_STK_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUsePolicyAmount, /*A_USE_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseTotalInsuredAmount, /*A_USE_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseReplacementAmount, /*A_USE_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseActualCashAmount, /*A_USE_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseEstimatedLossAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherPolicyAmount, /*A_OTH_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherTotalInsuredAmount, /*A_OTH_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherReplacementAmount, /*A_OTH_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherActualCashAmount, /*A_OTH_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherEstimatedLossAmount, /*A_OTH_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingReserveAmount, /*A_BLDG_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingPaidAmount, /*A_BLDG_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNNT_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentReserveAmount, /*A_CNNT_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNNT_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentPaidAmount, /*A_CNNT_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockReserveAmount, /*A_STK_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockPaidAmount, /*A_STK_PD */
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseReserve, /*A_USE_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUsePaid, /*A_USE_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherReserveAmount, /*A_OTH_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherPaidAmount, /*A_OTH_PD*/
				MAX(V_ActiveIPAddressMap.isoClaimId) AS isoClaimId,
				MAX(V_ActiveIPAddressMap.involvedPartySequenceId) AS involvedPartySequenceId
				INTO #ElementalClaimData
			FROM
				dbo.FireMarshalDriver
				INNER JOIN dbo.V_ActiveIPAddressMap
					ON FireMarshalDriver.isoClaimId = V_ActiveIPAddressMap.isoClaimId
				INNER JOIN dbo.V_ActiveAdjuster
					ON V_ActiveIPAddressMap.isoClaimId = V_ActiveAdjuster.isoClaimId
						AND V_ActiveIPAddressMap.involvedPartySequenceId = V_ActiveAdjuster.involvedPartySequenceId
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00014
					ON V_ActiveIPAddressMap.isoClaimId = CLT00014.I_ALLCLM
						AND V_ActiveIPAddressMap.involvedPartySequenceId = CLT00014.I_NM_ADR
						AND CLT00014.C_LOSS_TYP IS NOT NULL
						AND CLT00014.C_CVG_TYP IS NOT NULL
				LEFT OUTER JOIN (
					/*Notes on DuplicateDataSetPerformanceHack:
						dbo.CLT00017 contains duplicate records (DanR. verified with business that it
						is caused by an error somewhere in source or currentStateProcess 20190409.
							Performance of rowNumber/partition solution is noticeably better than using DISTINCT
					*/
					SELECT
						InnerCLT00017.I_ALLCLM,
						SUM(InnerCLT00017.A_BLDG_POL) AS A_BLDG_POL,
						SUM(InnerCLT00017.A_BLDG_TL_INS) AS A_BLDG_TL_INS,
						SUM(InnerCLT00017.A_BLDG_RPLCMT_VAL) AS A_BLDG_RPLCMT_VAL,
						SUM(InnerCLT00017.A_BLDG_ACTL_VAL) AS A_BLDG_ACTL_VAL,
						SUM(InnerCLT00017.A_BLDG_EST_LOSS) AS A_BLDG_EST_LOSS,
						SUM(InnerCLT00017.A_CNTT_POL) AS A_CNTT_POL,
						SUM(InnerCLT00017.A_CNTT_TL_INS) AS A_CNTT_TL_INS,
						SUM(InnerCLT00017.A_CNTT_RPLCMT_VAL) AS A_CNTT_RPLCMT_VAL,
						SUM(InnerCLT00017.A_CNTT_ACTL_VAL) AS A_CNTT_ACTL_VAL,
						SUM(InnerCLT00017.A_CNTT_EST_LOSS) AS A_CNTT_EST_LOSS,
						SUM(InnerCLT00017.A_STK_POL) AS A_STK_POL,
						SUM(InnerCLT00017.A_STK_TL_INS) AS A_STK_TL_INS,
						SUM(InnerCLT00017.A_STK_RPLCMT_VAL) AS A_STK_RPLCMT_VAL,
						SUM(InnerCLT00017.A_STK_ACTL_VAL) AS A_STK_ACTL_VAL,
						SUM(InnerCLT00017.A_STK_EST_LOSS) AS A_STK_EST_LOSS,
						SUM(InnerCLT00017.A_USE_POL) AS A_USE_POL,
						SUM(InnerCLT00017.A_USE_TL_INS) AS A_USE_TL_INS,
						SUM(InnerCLT00017.A_USE_RPLCMT_VAL) AS A_USE_RPLCMT_VAL,
						SUM(InnerCLT00017.A_USE_ACTL_VAL) AS A_USE_ACTL_VAL,
						SUM(InnerCLT00017.A_USE_EST_LOSS) AS A_USE_EST_LOSS,
						SUM(InnerCLT00017.A_OTH_POL) AS A_OTH_POL,
						SUM(InnerCLT00017.A_OTH_TL_INS) AS A_OTH_TL_INS,
						SUM(InnerCLT00017.A_OTH_RPLCMT_VAL) AS A_OTH_RPLCMT_VAL,
						SUM(InnerCLT00017.A_OTH_ACTL_VAL) AS A_OTH_ACTL_VAL,
						SUM(InnerCLT00017.A_OTH_EST_LOSS) AS A_OTH_EST_LOSS,
						SUM(InnerCLT00017.A_BLDG_RSRV) AS A_BLDG_RSRV,
						SUM(InnerCLT00017.A_BLDG_PD) AS A_BLDG_PD,
						SUM(InnerCLT00017.A_CNNT_RSRV) AS A_CNNT_RSRV,
						SUM(InnerCLT00017.A_CNNT_PD) AS A_CNNT_PD,
						SUM(InnerCLT00017.A_STK_RSRV) AS A_STK_RSRV,
						SUM(InnerCLT00017.A_STK_PD) AS A_STK_PD,
						SUM(InnerCLT00017.A_USE_RSRV) AS A_USE_RSRV,
						SUM(InnerCLT00017.A_USE_PD) AS A_USE_PD,
						SUM(InnerCLT00017.A_OTH_RSRV) AS A_OTH_RSRV,
						SUM(InnerCLT00017.A_OTH_PD) AS A_OTH_PD,
						MAX(InnerCLT00017.Date_Insert) AS Date_Insert
					FROM
						dbo.FireMarshalDriver AS InnerFireMarshalDriver
						INNER JOIN ClaimSearch_Prod.dbo.CLT00017 AS InnerCLT00017
							ON InnerFireMarshalDriver.isoClaimId = InnerCLT00017.I_ALLCLM
					GROUP BY
						InnerCLT00017.I_ALLCLM
				) AS CLT00017
					ON V_ActiveIPAddressMap.isoClaimId = CLT00017.I_ALLCLM
				LEFT OUTER JOIN dbo.ElementalClaim AS ExistingElementalClaimRecord
					ON CLT00014.I_ALLCLM = ExistingElementalClaimRecord.isoClaimId
						AND CLT00014.I_NM_ADR = ExistingElementalClaimRecord.involvedPartySequenceId
						AND CLT00014.C_LOSS_TYP = ExistingElementalClaimRecord.lossType
						AND CLT00014.C_CVG_TYP = ExistingElementalClaimRecord.coverageType
			WHERE
				CLT00014.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				)
				OR CLT00017.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				)
			GROUP BY
				V_ActiveIPAddressMap.claimId,
				V_ActiveIPAddressMap.involvedPartyId,
				CAST(CLT00014.C_LOSS_TYP AS CHAR(4)),
				CAST(CLT00014.C_CVG_TYP AS CHAR(4)),
				CAST(CLT00014.C_CLM_STUS AS VARCHAR(3));
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@stepDescription = 'UpdateElementalClaimData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.ElementalClaim WITH (TABLOCKX)
				SET
					/*ElementalClaim.elementalClaimId = SOURCE.elementalClaimId,*/
					/*ElementalClaim.claimId = SOURCE.claimId,*/
					/*ElementalClaim.involvedPartyId = SOURCE.involvedPartyId,*/
					ElementalClaim.adjusterId = SOURCE.adjusterId,
					/*ElementalClaim.lossType = SOURCE.lossType,*/
					/*ElementalClaim.coverageType = SOURCE.coverageType,*/
					ElementalClaim.dateClaimClosed = SOURCE.dateClaimClosed,
					ElementalClaim.coverageStatus = SOURCE.coverageStatus,
					ElementalClaim.settlementAmount = SOURCE.settlementAmount,
					ElementalClaim.estimatedLossAmount = SOURCE.estimatedLossAmount,
					ElementalClaim.reserveAmount = SOURCE.reserveAmount,
					ElementalClaim.totalInsuredAmount = SOURCE.totalInsuredAmount,
					ElementalClaim.policyAmount = SOURCE.policyAmount,
					ElementalClaim.replacementAmount = SOURCE.replacementAmount,
					ElementalClaim.actualCashAmount = SOURCE.actualCashAmount,
					ElementalClaim.buildingPolicyAmount = SOURCE.buildingPolicyAmount,
					ElementalClaim.buildingTotalInsuredAmount = SOURCE.buildingTotalInsuredAmount,
					ElementalClaim.buildingReplacementAmount = SOURCE.buildingReplacementAmount,
					ElementalClaim.buildingActualCashAmount = SOURCE.buildingActualCashAmount,
					ElementalClaim.buildingEstimatedLossAmount = SOURCE.buildingEstimatedLossAmount,
					ElementalClaim.contentPolicyAmount = SOURCE.contentPolicyAmount,
					ElementalClaim.contentTotalInsuredAmount = SOURCE.contentTotalInsuredAmount,
					ElementalClaim.contentReplacementAmount = SOURCE.contentReplacementAmount,
					ElementalClaim.contentActualCashAmount = SOURCE.contentActualCashAmount,
					ElementalClaim.contentEstimatedLossAmount = SOURCE.contentEstimatedLossAmount,
					ElementalClaim.stockPolicyAmount = SOURCE.stockPolicyAmount,
					ElementalClaim.stockTotalInsuredAmount = SOURCE.stockTotalInsuredAmount,
					ElementalClaim.stockReplacementAmount = SOURCE.stockReplacementAmount,
					ElementalClaim.stockActualCashAmount = SOURCE.stockActualCashAmount,
					ElementalClaim.stockEstimatedLossAmount = SOURCE.stockEstimatedLossAmount,
					ElementalClaim.lossOfUsePolicyAmount = SOURCE.lossOfUsePolicyAmount,
					ElementalClaim.lossOfUseTotalInsuredAmount = SOURCE.lossOfUseTotalInsuredAmount,
					ElementalClaim.lossOfUseReplacementAmount = SOURCE.lossOfUseReplacementAmount,
					ElementalClaim.lossOfUseActualCashAmount = SOURCE.lossOfUseActualCashAmount,
					ElementalClaim.lossOfUseEstimatedLossAmount = SOURCE.lossOfUseEstimatedLossAmount,
					ElementalClaim.otherPolicyAmount = SOURCE.otherPolicyAmount,
					ElementalClaim.otherTotalInsuredAmount = SOURCE.otherTotalInsuredAmount,
					ElementalClaim.otherReplacementAmount = SOURCE.otherReplacementAmount,
					ElementalClaim.otherActualCashAmount = SOURCE.otherActualCashAmount,
					ElementalClaim.otherEstimatedLossAmount = SOURCE.otherEstimatedLossAmount,
					ElementalClaim.buildingReserveAmount = SOURCE.buildingReserveAmount,
					ElementalClaim.buildingPaidAmount = SOURCE.buildingPaidAmount,
					ElementalClaim.contentReserveAmount = SOURCE.contentReserveAmount,
					ElementalClaim.contentPaidAmount = SOURCE.contentPaidAmount,
					ElementalClaim.stockReserveAmount = SOURCE.stockReserveAmount,
					ElementalClaim.stockPaidAmount = SOURCE.stockPaidAmount,
					ElementalClaim.lossOfUseReserve = SOURCE.lossOfUseReserve,
					ElementalClaim.lossOfUsePaid = SOURCE.lossOfUsePaid,
					ElementalClaim.otherReserveAmount = SOURCE.otherReserveAmount,
					ElementalClaim.otherPaidAmount = SOURCE.otherPaidAmount,
					/*ElementalClaim.isActive = SOURCE.isActive,*/
					ElementalClaim.dateInserted = @dateInserted
					/*ElementalClaim.isoClaimId = SOURCE.isoClaimId,*/
					/*ElementalClaim.involvedPartySequenceId = SOURCE.involvedPartySequenceId,*/
			FROM
				#ElementalClaimData AS SOURCE
			WHERE
				SOURCE.elementalClaimId IS NOT NULL
				AND SOURCE.elementalClaimId = ElementalClaim.elementalClaimId
				AND 
				(
					/*elementalClaimId*/
					/*claimId*/
					/*involvedPartyId*/
					ISNULL(ElementalClaim.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					/*lossType*/
					/*coverageType*/
					OR ISNULL(ElementalClaim.dateClaimClosed,'99990101') <> ISNULL(SOURCE.dateClaimClosed,'99990101')
					OR ISNULL(ElementalClaim.coverageStatus,'~~~') <> ISNULL(SOURCE.coverageStatus,'~~~')
					OR ISNULL(ElementalClaim.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
					OR ISNULL(ElementalClaim.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.reserveAmount,-1) <> ISNULL(SOURCE.reserveAmount,-1)
					OR ISNULL(ElementalClaim.totalInsuredAmount,-1) <> ISNULL(SOURCE.totalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
					OR ISNULL(ElementalClaim.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(ElementalClaim.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(ElementalClaim.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(ElementalClaim.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(ElementalClaim.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(ElementalClaim.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(ElementalClaim.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(ElementalClaim.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(ElementalClaim.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(ElementalClaim.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(ElementalClaim.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(ElementalClaim.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(ElementalClaim.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(ElementalClaim.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(ElementalClaim.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(ElementalClaim.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR ISNULL(ElementalClaim.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(ElementalClaim.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(ElementalClaim.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(ElementalClaim.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(ElementalClaim.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
					/*isActive*/
					/*ElementalClaim.dateInserted*/
					/*isoClaimId*/
					/*involvedPartySequenceId*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@stepDescription = 'InsertNewElementalClaimData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.ElementalClaim WITH (TABLOCKX)
			(
				/*elementalClaimId,*/
				claimId,
				involvedPartyId,
				adjusterId,
				lossType,
				coverageType,
				dateClaimClosed,
				coverageStatus,
				settlementAmount,
				estimatedLossAmount,
				reserveAmount,
				totalInsuredAmount,
				policyAmount,
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
				buildingPaidAmount,
				contentReserveAmount,
				contentPaidAmount,
				stockReserveAmount,
				stockPaidAmount,
				lossOfUseReserve,
				lossOfUsePaid,
				otherReserveAmount,
				otherPaidAmount,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				/*SOURCE.elementalClaimId,*/
				SOURCE.claimId,
				SOURCE.involvedPartyId,
				SOURCE.adjusterId,
				SOURCE.lossType,
				SOURCE.coverageType,
				SOURCE.dateClaimClosed,
				SOURCE.coverageStatus,
				SOURCE.settlementAmount,
				SOURCE.estimatedLossAmount,
				SOURCE.reserveAmount,
				SOURCE.totalInsuredAmount,
				SOURCE.policyAmount,
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
				SOURCE.buildingPaidAmount,
				SOURCE.contentReserveAmount,
				SOURCE.contentPaidAmount,
				SOURCE.stockReserveAmount,
				SOURCE.stockPaidAmount,
				SOURCE.lossOfUseReserve,
				SOURCE.lossOfUsePaid,
				SOURCE.otherReserveAmount,
				SOURCE.otherPaidAmount,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#ElementalClaimData AS SOURCE
			WHERE
				SOURCE.elementalClaimId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
		INSERT INTO dbo.ElementalClaimActivityLog
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
					+ 'of hsp_UpdateElementalClaim; ErrorMsg: '
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
COMMIT TRANSACTION
20190114 : 5:27PM
20190122 : 9:52AM
20190122 : 4:50PM
20190130 : 9:54AM
*/