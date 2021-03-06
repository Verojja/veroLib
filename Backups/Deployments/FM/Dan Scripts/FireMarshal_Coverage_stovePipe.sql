  ------------------------------------------
  -- PROGRAM: FM AMOUNT DIMENSION PROC
  -- DEVELOPER: DANIEL RAVAGLIA
  -- DATE: 12/12/2018
  -------------------------------------------
--drop table [ClaimSearch_Dev].[dbo].[DIM_FM_Amounts]


Use ClaimSearch_Dev

UPDATE dbo.DIM_FM_Amounts
SET    dbo.DIM_FM_Amounts.isActive = 0
FROM   dbo.DIM_FM_Amounts A
   
INNER JOIN dbo.FM_ExtractFile B
on a.isoClaimId = b.I_ALLCLM
  

INSERT INTO [ClaimSearch_Dev].[dbo].[DIM_FM_Amounts]
           ([isoClaimId]
           ,[I_NM_ADR]
           ,[lossType]
           ,[coverageType]
           ,[dateClaimClosed]
           ,[coverageStatus]
           ,[isActive]
           ,[dateInserted]
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
           ,[otherPaidAmount])
SELECT 
       cast(main.I_ALLCLM as char(11)) as isoClaimId      
      ,cast(clt14.I_NM_ADR as smallint) as I_NM_ADR
      ,cast(clt14.C_LOSS_TYP as char(4)) as lossType /*C_LOSS_TYP*/
      ,cast(clt14.C_CVG_TYP as char(4)) as coverageType /*C_CVG_TYP*/
      ,cast(clt14.D_CLM_CLOSE as date) as dateClaimClosed /*D_CLM_CLOSE*/
      ,cast(C_CLM_STUS as varchar(3)) as coverageStatus /*C_CLM_STUS*/
      ,cast (1 as bit) AS isActive
      ,getdate() AS dateInserted
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
		ELSE cast(0 as decimal(18,0)) END) AS otherPaidAmount /*A_OTH_PD*/

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
          

  

