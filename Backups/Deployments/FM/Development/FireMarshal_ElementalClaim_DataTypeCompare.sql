SET NOEXEC OFF;

BEGIN TRANSACTION
DROP TABLE ElementalClaim
DROP TABLE ElementalClaimTwo
CREATE TABLE dbo.ElementalClaim
(   
	elementalClaimId BIGINT IDENTITY(1,1) NOT NULL,
	claimId BIGINT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	adjusterId BIGINT NULL,
	lossType CHAR(4) NULL,
	coverageType CHAR(4) NULL,
	dateClaimClosed DATE NULL,
	coverageStatus VARCHAR(3) NULL,
	settlementAmount DECIMAL(38,17) NULL,
	estimatedLossAmount DECIMAL(38,17) NULL,
	reserveAmount DECIMAL(38,17) NULL,
	totalInsuredAmount DECIMAL(38,17) NULL,
	policyAmount DECIMAL(38,17) NULL,
	replacementValue DECIMAL(38,17) NULL,
	actualCashValue DECIMAL(38,17) NULL,
	buildingPolicyAmount DECIMAL(38,17) NULL,
	buildingTotalInsuredAmount DECIMAL(38,17) NULL,
	buildingReplacementValue DECIMAL(38,17) NULL,
	buildingActualCashValue DECIMAL(38,17) NULL,
	buildingEstimatedLossAmount DECIMAL(38,17) NULL,
	contentPolicyAmount DECIMAL(38,17) NULL,
	contentTotalInsuredAmount DECIMAL(38,17) NULL,
	contentReplacementValue DECIMAL(38,17) NULL,
	contentActualCashValue DECIMAL(38,17) NULL,
	contentEstimatedLossAmount DECIMAL(38,17) NULL,
	stockPolicyAmount DECIMAL(38,17) NULL,
	stockTotalInsuredAmount DECIMAL(38,17) NULL,
	stockReplacementValue DECIMAL(38,17) NULL,
	stockActualCashValue DECIMAL(38,17) NULL,
	stockEstimatedLossAmount DECIMAL(38,17) NULL,
	usePolicyAmount DECIMAL(38,17) NULL,
	useTotalInsuredAmount DECIMAL(38,17) NULL,
	useReplacementValue DECIMAL(38,17) NULL,
	useActualCashValue DECIMAL(38,17) NULL,
	useEstimatedLossAmount DECIMAL(38,17) NULL,
	otherPolicyAmount DECIMAL(38,17) NULL,
	otherTotalInsuredAmount DECIMAL(38,17) NULL,
	otherReplacementValue DECIMAL(38,17) NULL,
	otherActualCashValue DECIMAL(38,17) NULL,
	otherEstimatedLossAmount DECIMAL(38,17) NULL,
	buildingReserveAmount DECIMAL(38,17) NULL,
	buildingPaidAount DECIMAL(38,17) NULL,
	contentReserveAount DECIMAL(38,17) NULL,
	contentPaidAmount DECIMAL(38,17) NULL,
	stockReserveAmoutn DECIMAL(38,17) NULL,
	stockPaidAmount DECIMAL(38,17) NULL,
	useReserve DECIMAL(38,17) NULL,
	usePaid DECIMAL(38,17) NULL,
	otherReserveAmount DECIMAL(38,17) NULL,
	otherPaidAmount DECIMAL(38,17) NULL,

	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,

	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	CONSTRAINT PK_ElementalClaim_elementalClaimId
		PRIMARY KEY CLUSTERED (elementalClaimId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

INSERT INTO [dbo].ElementalClaim
(
         /*elementalClaimId,*/ claimId
			,involvedPartyId
			,adjusterId
           ,[lossType]
           ,[coverageType]
           ,[dateClaimClosed]
           ,[coverageStatus]
           ,[settlementAmount]
           ,[estimatedLossAmount]
           ,[reserveAmount]
           ,[totalInsuredAmount]
           ,[policyAmount]
           ,[replacementValue]
           ,[actualCashValue]
           ,[buildingPolicyAmount]
           ,[buildingTotalInsuredAmount]
           ,[buildingReplacementValue]
           ,[buildingActualCashValue]
           ,[buildingEstimatedLossAmount]
           ,[contentPolicyAmount]
           ,[contentTotalInsuredAmount]
           ,[contentReplacementValue]
           ,[contentActualCashValue]
           ,[contentEstimatedLossAmount]
           ,[stockPolicyAmount]
           ,[stockTotalInsuredAmount]
           ,[stockReplacementValue]
           ,[stockActualCashValue]
           ,[stockEstimatedLossAmount]
           ,[usePolicyAmount]
           ,[useTotalInsuredAmount]
           ,[useReplacementValue]
           ,[useActualCashValue]
           ,[useEstimatedLossAmount]
           ,[otherPolicyAmount]
           ,[otherTotalInsuredAmount]
           ,[otherReplacementValue]
           ,[otherActualCashValue]
           ,[otherEstimatedLossAmount]
           ,[buildingReserveAmount]
           ,[buildingPaidAount]
           ,[contentReserveAount]
           ,[contentPaidAmount]
           ,[stockReserveAmoutn]
           ,[stockPaidAmount]
           ,[useReserve]
           ,[usePaid]
           ,[otherReserveAmount]
           ,[otherPaidAmount],
           isActive,
           dateInserted,
           isoClaimId,
           involvedPartySequenceId
           )
SELECT  TOP 1000
		1,1,1,
      cast(clt14.C_LOSS_TYP as char(4)) as lossType /*C_LOSS_TYP*/
      ,cast(clt14.C_CVG_TYP as char(4)) as coverageType /*C_CVG_TYP*/
      ,cast(clt14.D_CLM_CLOSE as date) as dateClaimClosed /*D_CLM_CLOSE*/
      ,cast(C_CLM_STUS as varchar(3)) as coverageStatus /*C_CLM_STUS*/
      --// PROP OR CAS amounts
      
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_PD	+ CLT17.A_CNNT_PD	+ CLT17.A_STK_PD + CLT17.A_USE_PD + CLT17.A_OTH_PD) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
			THEN cast(CLT14.A_STTLMT AS decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS settlementAmount
   	   
      ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_EST_LOSS	+ CLT17.A_CNTT_EST_LOSS	+ CLT17.A_STK_EST_LOSS + CLT17.A_USE_EST_LOSS + CLT17.A_OTH_EST_LOSS) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
		    THEN cast(CLT14.A_EST_LOSS as decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS estimatedLossAmount
	  
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_RSRV	+ CLT17.A_CNNT_RSRV	+ CLT17.A_STK_RSRV + CLT17.A_USE_RSRV + CLT17.A_OTH_RSRV) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
		    THEN cast(CLT14.A_RSRV as decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS reserveAmount
	   
	   --// CHECK THIS RELATIONSHIP
	   
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
	  		THEN cast((CLT17.A_BLDG_TL_INS + CLT17.A_CNTT_TL_INS	+ CLT17.A_STK_TL_INS + CLT17.A_USE_TL_INS + CLT17.A_OTH_TL_INS) AS decimal(18,0))
	  	WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
	  		THEN cast(CLT14.A_INDM as decimal(18,0))
	  	WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
	  		THEN cast(0 AS decimal(18,0))
	   END) AS totalInsuredAmount
	   
	  --// CHECK THIS RELATIONSHIP
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_POL	+ CLT17.A_CNTT_POL	+ CLT17.A_STK_POL + CLT17.A_USE_POL + CLT17.A_OTH_POL) AS decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
			THEN cast(CLT14.A_CVG_TL as decimal(18,0))
		ELSE cast(0 as decimal(18,0))
	   END) AS policyAmount
	   	  
      --// PROP ONLY (column sum)
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL --AND clt14.I_ALLCLM IS NOT NULL
			THEN (CLT17.A_BLDG_RPLCMT_VAL + CLT17.A_CNTT_RPLCMT_VAL + CLT17.A_STK_RPLCMT_VAL + CLT17.A_USE_RPLCMT_VAL+ CLT17.A_OTH_RPLCMT_VAL) 
	    ELSE 0
	   END )AS replacementValue
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL --AND clt14.I_ALLCLM IS NOT NULL
			THEN cast((CLT17.A_BLDG_ACTL_VAL	+ CLT17.A_CNTT_ACTL_VAL + CLT17.A_STK_ACTL_VAL + CLT17.A_USE_ACTL_VAL + CLT17.A_OTH_ACTL_VAL) as decimal(18,0))
		ELSE cast(0 as decimal(18,0))
	   END )AS actualCashValue
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_POL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingPolicyAmount /*A_BLDG_POL*/
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingTotalInsuredAmount /*A_BLDG_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingReplacementValue /*A_BLDG_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_ACTL_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingActualCashValue /*A_BLDG_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_EST_LOSS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingEstimatedLossAmount /*A_BLDG_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_POL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentPolicyAmount /*A_CNTT_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentTotalInsuredAmount /*A_CNTT_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_RPLCMT_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentReplacementValue /*A_CNTT_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentActualCashValue /*A_CNTT_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentEstimatedLossAmount /*A_CNTT_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockPolicyAmount /*A_STK_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_TL_INS as decimal (18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockTotalInsuredAmount /*A_STK_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_RPLCMT_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS stockReplacementValue /*A_STK_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_ACTL_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS stockActualCashValue /*A_STK_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockEstimatedLossAmount /*A_STK_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS usePolicyAmount /*A_USE_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS useTotalInsuredAmount /*A_USE_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useReplacementValue /*A_USE_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useActualCashValue /*A_USE_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useEstimatedLossAmount /*A_USE_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherPolicyAmount /*A_OTH_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_TL_INS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherTotalInsuredAmount /*A_OTH_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherReplacementValue /*A_OTH_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherActualCashValue /*A_OTH_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherEstimatedLossAmount /*A_OTH_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingReserveAmount /*A_BLDG_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingPaidAount /*A_BLDG_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNNT_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentReserveAount /*A_CNNT_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNNT_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentPaidAmount /*A_CNNT_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockReserveAmoutn /*A_STK_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockPaidAmount /*A_STK_PD */
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useReserve /*A_USE_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_PD as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS usePaid /*A_USE_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherReserveAmount /*A_OTH_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherPaidAmount /*A_OTH_PD*/,
		
		1 AS isActive,
		GETDATE() as dateInserted,
		main.I_ALLCLM as isoClaimId,
		clt14.I_NM_ADR AS involvedPartySequenceId
  --INTO select * from [ClaimSearch_Dev].[dbo].[DIM_FM_Amounts]
  FROM [ClaimSearch_Dev].[dbo].[FM_ExtractFile]main
  
  left join [ClaimSearch_Prod].[dbo].[CLT00017]clt17
  on clt17.I_ALLCLM = main.I_ALLCLM
  
  left join [ClaimSearch_Prod].[dbo].[CLT00014]clt14
  on clt14.I_ALLCLM = main.I_ALLCLM
  
  group by main.I_ALLCLM
          ,clt14.I_NM_ADR
          ,clt14.C_LOSS_TYP
          ,clt14.C_CVG_TYP 
          ,D_CLM_CLOSE
          ,C_CLM_STUS
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.ElementalClaimTwo
(   
	elementalClaimId BIGINT IDENTITY(1,1) NOT NULL,
	claimId BIGINT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	adjusterId BIGINT NULL,
	lossType CHAR(4) NULL,
	coverageType CHAR(4) NULL,
	dateClaimClosed DATE NULL,
	coverageStatus VARCHAR(3) NULL,
	settlementAmount MONEY NULL,
	estimatedLossAmount MONEY NULL,
	reserveAmount MONEY NULL,
	totalInsuredAmount MONEY NULL,
	policyAmount MONEY NULL,
	replacementValue MONEY NULL,
	actualCashValue MONEY NULL,
	buildingPolicyAmount MONEY NULL,
	buildingTotalInsuredAmount MONEY NULL,
	buildingReplacementValue MONEY NULL,
	buildingActualCashValue MONEY NULL,
	buildingEstimatedLossAmount MONEY NULL,
	contentPolicyAmount MONEY NULL,
	contentTotalInsuredAmount MONEY NULL,
	contentReplacementValue MONEY NULL,
	contentActualCashValue MONEY NULL,
	contentEstimatedLossAmount MONEY NULL,
	stockPolicyAmount MONEY NULL,
	stockTotalInsuredAmount MONEY NULL,
	stockReplacementValue MONEY NULL,
	stockActualCashValue MONEY NULL,
	stockEstimatedLossAmount MONEY NULL,
	usePolicyAmount MONEY NULL,
	useTotalInsuredAmount MONEY NULL,
	useReplacementValue MONEY NULL,
	useActualCashValue MONEY NULL,
	useEstimatedLossAmount MONEY NULL,
	otherPolicyAmount MONEY NULL,
	otherTotalInsuredAmount MONEY NULL,
	otherReplacementValue MONEY NULL,
	otherActualCashValue MONEY NULL,
	otherEstimatedLossAmount MONEY NULL,
	buildingReserveAmount MONEY NULL,
	buildingPaidAount MONEY NULL,
	contentReserveAount MONEY NULL,
	contentPaidAmount MONEY NULL,
	stockReserveAmoutn MONEY NULL,
	stockPaidAmount MONEY NULL,
	useReserve MONEY NULL,
	usePaid MONEY NULL,
	otherReserveAmount MONEY NULL,
	otherPaidAmount MONEY NULL,

	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,

	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL /*I_NM_ADR*/,
	CONSTRAINT PK_EasdflementalClaim_elementalClaimId
		PRIMARY KEY CLUSTERED (elementalClaimId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
  INSERT INTO [dbo].ElementalClaimTwo
  (
         /*elementalClaimId,*/ claimId
			,involvedPartyId
			,adjusterId
           ,[lossType]
           ,[coverageType]
           ,[dateClaimClosed]
           ,[coverageStatus]
           ,[settlementAmount]
           ,[estimatedLossAmount]
           ,[reserveAmount]
           ,[totalInsuredAmount]
           ,[policyAmount]
           ,[replacementValue]
           ,[actualCashValue]
           ,[buildingPolicyAmount]
           ,[buildingTotalInsuredAmount]
           ,[buildingReplacementValue]
           ,[buildingActualCashValue]
           ,[buildingEstimatedLossAmount]
           ,[contentPolicyAmount]
           ,[contentTotalInsuredAmount]
           ,[contentReplacementValue]
           ,[contentActualCashValue]
           ,[contentEstimatedLossAmount]
           ,[stockPolicyAmount]
           ,[stockTotalInsuredAmount]
           ,[stockReplacementValue]
           ,[stockActualCashValue]
           ,[stockEstimatedLossAmount]
           ,[usePolicyAmount]
           ,[useTotalInsuredAmount]
           ,[useReplacementValue]
           ,[useActualCashValue]
           ,[useEstimatedLossAmount]
           ,[otherPolicyAmount]
           ,[otherTotalInsuredAmount]
           ,[otherReplacementValue]
           ,[otherActualCashValue]
           ,[otherEstimatedLossAmount]
           ,[buildingReserveAmount]
           ,[buildingPaidAount]
           ,[contentReserveAount]
           ,[contentPaidAmount]
           ,[stockReserveAmoutn]
           ,[stockPaidAmount]
           ,[useReserve]
           ,[usePaid]
           ,[otherReserveAmount]
           ,[otherPaidAmount],
           isActive,
           dateInserted,
           isoClaimId,
           involvedPartySequenceId)
SELECT  TOP 1000
		1,1,1,
      cast(clt14.C_LOSS_TYP as char(4)) as lossType /*C_LOSS_TYP*/
      ,cast(clt14.C_CVG_TYP as char(4)) as coverageType /*C_CVG_TYP*/
      ,cast(clt14.D_CLM_CLOSE as date) as dateClaimClosed /*D_CLM_CLOSE*/
      ,cast(C_CLM_STUS as varchar(3)) as coverageStatus /*C_CLM_STUS*/
      --// PROP OR CAS amounts
      
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_PD	+ CLT17.A_CNNT_PD	+ CLT17.A_STK_PD + CLT17.A_USE_PD + CLT17.A_OTH_PD) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
			THEN cast(CLT14.A_STTLMT AS decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS settlementAmount
   	   
      ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_EST_LOSS	+ CLT17.A_CNTT_EST_LOSS	+ CLT17.A_STK_EST_LOSS + CLT17.A_USE_EST_LOSS + CLT17.A_OTH_EST_LOSS) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
		    THEN cast(CLT14.A_EST_LOSS as decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS estimatedLossAmount
	  
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_RSRV	+ CLT17.A_CNNT_RSRV	+ CLT17.A_STK_RSRV + CLT17.A_USE_RSRV + CLT17.A_OTH_RSRV) as decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
		    THEN cast(CLT14.A_RSRV as decimal(18,0))
		WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
			THEN cast(0 AS decimal(18,0))
	   END) AS reserveAmount
	   
	   --// CHECK THIS RELATIONSHIP
	   
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
	  		THEN cast((CLT17.A_BLDG_TL_INS + CLT17.A_CNTT_TL_INS	+ CLT17.A_STK_TL_INS + CLT17.A_USE_TL_INS + CLT17.A_OTH_TL_INS) AS decimal(18,0))
	  	WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
	  		THEN cast(CLT14.A_INDM as decimal(18,0))
	  	WHEN  (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND (clt14.I_ALLCLM IS NULL OR clt14.I_ALLCLM = '') 
	  		THEN cast(0 AS decimal(18,0))
	   END) AS totalInsuredAmount
	   
	  --// CHECK THIS RELATIONSHIP
	  ,sum(CASE 
		WHEN clt17.I_ALLCLM IS NOT NULL 
			THEN cast((CLT17.A_BLDG_POL	+ CLT17.A_CNTT_POL	+ CLT17.A_STK_POL + CLT17.A_USE_POL + CLT17.A_OTH_POL) AS decimal(18,0)) 	  	
		WHEN (clt17.I_ALLCLM IS NULL OR clt17.I_ALLCLM = '') AND clt14.I_ALLCLM IS NOT NULL
			THEN cast(CLT14.A_CVG_TL as decimal(18,0))
		ELSE cast(0 as decimal(18,0))
	   END) AS policyAmount
	   	  
      --// PROP ONLY (column sum)
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL --AND clt14.I_ALLCLM IS NOT NULL
			THEN (CLT17.A_BLDG_RPLCMT_VAL + CLT17.A_CNTT_RPLCMT_VAL + CLT17.A_STK_RPLCMT_VAL + CLT17.A_USE_RPLCMT_VAL+ CLT17.A_OTH_RPLCMT_VAL) 
	    ELSE 0
	   END )AS replacementValue
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL --AND clt14.I_ALLCLM IS NOT NULL
			THEN cast((CLT17.A_BLDG_ACTL_VAL	+ CLT17.A_CNTT_ACTL_VAL + CLT17.A_STK_ACTL_VAL + CLT17.A_USE_ACTL_VAL + CLT17.A_OTH_ACTL_VAL) as decimal(18,0))
		ELSE cast(0 as decimal(18,0))
	   END )AS actualCashValue
	  
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_POL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingPolicyAmount /*A_BLDG_POL*/
	  ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingTotalInsuredAmount /*A_BLDG_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingReplacementValue /*A_BLDG_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_ACTL_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingActualCashValue /*A_BLDG_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_EST_LOSS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS buildingEstimatedLossAmount /*A_BLDG_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_POL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentPolicyAmount /*A_CNTT_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentTotalInsuredAmount /*A_CNTT_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_RPLCMT_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS contentReplacementValue /*A_CNTT_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentActualCashValue /*A_CNTT_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNTT_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentEstimatedLossAmount /*A_CNTT_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockPolicyAmount /*A_STK_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_TL_INS as decimal (18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockTotalInsuredAmount /*A_STK_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_RPLCMT_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS stockReplacementValue /*A_STK_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_ACTL_VAL as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS stockActualCashValue /*A_STK_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockEstimatedLossAmount /*A_STK_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS usePolicyAmount /*A_USE_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_TL_INS as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS useTotalInsuredAmount /*A_USE_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useReplacementValue /*A_USE_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useActualCashValue /*A_USE_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useEstimatedLossAmount /*A_USE_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_POL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherPolicyAmount /*A_OTH_POL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_TL_INS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherTotalInsuredAmount /*A_OTH_TL_INS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_RPLCMT_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherReplacementValue /*A_OTH_RPLCMT_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_ACTL_VAL as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherActualCashValue /*A_OTH_ACTL_VAL*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_EST_LOSS as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherEstimatedLossAmount /*A_OTH_EST_LOSS*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingReserveAmount /*A_BLDG_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_BLDG_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS buildingPaidAount /*A_BLDG_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNNT_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentReserveAount /*A_CNNT_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_CNNT_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS contentPaidAmount /*A_CNNT_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockReserveAmoutn /*A_STK_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_STK_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS stockPaidAmount /*A_STK_PD */
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS useReserve /*A_USE_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_USE_PD as decimal(18,0)) 
		ELSE cast(0 as decimal(18,0)) END) AS usePaid /*A_USE_PD*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_RSRV as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherReserveAmount /*A_OTH_RSRV*/
      ,sum(CASE WHEN clt17.I_ALLCLM IS NOT NULL THEN cast(clt17.A_OTH_PD as decimal(18,0))
		ELSE cast(0 as decimal(18,0)) END) AS otherPaidAmount /*A_OTH_PD*/,
		
		1 AS isActive,
		GETDATE() as dateInserted,
		main.I_ALLCLM as isoClaimId,
		clt14.I_NM_ADR AS involvedPartySequenceId
  --INTO select * from [ClaimSearch_Dev].[dbo].[DIM_FM_Amounts]
  FROM [ClaimSearch_Dev].[dbo].[FM_ExtractFile]main
  
  left join [ClaimSearch_Prod].[dbo].[CLT00017]clt17
  on clt17.I_ALLCLM = main.I_ALLCLM
  
  left join [ClaimSearch_Prod].[dbo].[CLT00014]clt14
  on clt14.I_ALLCLM = main.I_ALLCLM
  
  group by main.I_ALLCLM
          ,clt14.I_NM_ADR
          ,clt14.C_LOSS_TYP
          ,clt14.C_CVG_TYP 
          ,D_CLM_CLOSE
          ,C_CLM_STUS
COMMIT TRANSACTION