  ----------------------------
  -- PROGRAM: FM EXTRACT PROC
  -- DEVELOPER: DANIEL RAVAGLIA
  -- DATE: 12/12/2018
  ----------------------------

  drop table [ClaimSearch_Dev].[dbo].[Proc_FM_Tags]
  select 
    CLT1.I_ALLCLM
   ,CLT1.C_LOL_ST_ALPH  -- // REMOVED AFTER TESTING
   ,CLT1.I_CUST         -- // REMOVED AFTER TESTING
   ,CLT1.T_LOSS_DSC     -- // REMOVED AFTER TESTING
   ,CLT1.Date_Insert    -- // REMOVED AFTER TESTING
   ,CAST (SUBSTRING(CLT1.D_RCV,1,10) AS DATE) AS D_RCV_10  -- // REMOVED AFTER TESTING
   ,SUM(case																								-- // REMOVED AFTER TESTING
	 WHEN SUBSTRING(CLT1.I_CUST,1,2) in ('Z9','X5','X7','X8','X9') THEN 1 ELSE 0 END) AS FLAG_TEST_COMP		-- // REMOVED AFTER TESTING
   ,CLT1.F_APD			-- // REMOVED AFTER TESTING
   ,CLT1.F_AUTO			-- // REMOVED AFTER TESTING
   ,CLT1.F_PROP			-- // REMOVED AFTER TESTING
   ,CLT1.F_CSLTY		-- // REMOVED AFTER TESTING
   ,CLT1.C_POL_TYP		-- // REMOVED AFTER TESTING
   ,sum(CASE 
	 WHEN CLT1.C_CLM_SRCE = 'P' 
	 AND CLT1.C_LOSS_TYP = 'FIRE' 
	 THEN 1 else 0 END) AS FLAG_LEG_FIRE 
   ,sum(CASE 
	 WHEN CLT1.C_CLM_SRCE = 'U' 
	 AND (clt1.F_AUTO = 'N' AND clt1.F_APD = 'N')	  
	 AND clt14.C_LOSS_TYP in ('FIRE') 
	 THEN 1 else 0 end) AS FLAG_UF_FIRE
  ,sum(CASE 
	 WHEN CLT1.C_CLM_SRCE = 'U' 
	 AND clt14.C_LOSS_TYP in ('LGHT','EXPL') 
	 THEN 1 else 0 end) AS FLAG_UF_LightExpl
   ,sum(CASE 
	 WHEN CLT1.C_CLM_SRCE = 'U' 
	  AND (clt1.F_AUTO = 'Y' OR clt1.F_APD = 'Y')
	  and (clt1.F_PROP = 'N' OR clt1.F_CSLTY = 'N')	  
	  and clt14.C_LOSS_TYP = 'FIRE'
	  OR 
	  (
	  CLT1.C_CLM_SRCE = 'U' 
	  AND clt1.C_POL_TYP in ('PAPP','CAPP','PPMH') 
	  and clt14.C_CVG_TYP in ('COMP','OTAU') 
	  and clt14.C_LOSS_TYP = 'FIRE'
	  )
	  then 1 else 0 end) AS FLAG_UF_AUTO
   ,sum(CASE 
     WHEN CLT1.T_LOSS_DSC not in ('Fire','','blank') 
	 then 1 else 0 END) AS FLAG_VALID_LOSS_DESC
   ,sum(CASE								-- // ANALYSIS CODE TO BE REMOVED
	 WHEN len(CLT1.T_LOSS_DSC) >6			-- // ANALYSIS CODE TO BE REMOVED
	 then 1 else 0							-- // ANALYSIS CODE TO BE REMOVED
	END) AS FLAG_VALID_LOSS_DESC_LENGTH		-- // ANALYSIS CODE TO BE REMOVED
   ,sum(CASE
	 WHEN CLT14.A_EST_LOSS > 0 THEN 1
	 else 0 END) AS FLAG_VALID_LOSS_ESTIMATE
   ,sum(CASE
	 WHEN CLT14.A_STTLMT > 0 THEN 1
	 else 0 END) AS FLAG_VALID_LOSS_SETTLEMENT
   ,SUM (CLT14.A_EST_LOSS) AS A_EST_LOSS   -- // REMOVED AFTER TESTING
   ,SUM (CLT14.A_STTLMT) AS A_STTLMT       -- // REMOVED AFTER TESTING
   
   
  into [ClaimSearch_Dev].[dbo].[Proc_FM_Tags]  
  from [ClaimSearch_Prod].[dbo].[CLT00001]clt1   
  
  inner join [ClaimSearch_Prod].[dbo].[CLT00014]clt14 
  on clt1.I_ALLCLM = clt14.I_ALLCLM
  and clt1.Date_Insert = clt14.Date_Insert
  

 -- to be replaced with current day extraction when running daily   
  where clt1.D_RCV between '2008-01-01-00.00.00.000000'
                       and '2019-01-31-23.59.59.999999'
  and
  (
  (CLT1.C_CLM_SRCE = 'P' AND CLT1.C_LOSS_TYP = 'FIRE') or 
  (CLT1.C_CLM_SRCE = 'U' AND clt14.C_LOSS_TYP in ('FIRE','LGHT','EXPL')) or
  (CLT1.C_CLM_SRCE = 'U' AND (clt1.F_AUTO = 'Y' OR clt1.F_APD = 'Y') and clt14.C_LOSS_TYP = 'FIRE') or 
  (CLT1.C_CLM_SRCE = 'U' AND clt1.C_POL_TYP in ('PAPP','CAPP','PPMH') and clt14.C_CVG_TYP in ('COMP','OTAU') and clt14.C_LOSS_TYP = 'FIRE')
  )
  
  group by 
	CLT1.I_ALLCLM
   ,CLT1.C_LOL_ST_ALPH
   ,CLT1.I_CUST
   ,CLT1.F_APD
   ,CLT1.F_AUTO
   ,CLT1.F_PROP
   ,CLT1.F_CSLTY  
   ,CLT1.C_POL_TYP
   ,CLT1.T_LOSS_DSC
   ,CLT1.Date_Insert
   ,CAST (SUBSTRING(CLT1.D_RCV,1,10) AS DATE)
   
   CREATE UNIQUE INDEX I_ALLCLM_IDX ON [ClaimSearch_Dev].[dbo].[Proc_FM_Tags] (I_ALLCLM)

 -------------------------------------------------
 -- Rule Evaluation
 -------------------------------------------------
  Drop table [ClaimSearch_Dev].[dbo].[FM_ExtractFile]       
  SELECT distinct
       MAIN.[I_ALLCLM]  
      --,MAIN.F_APD
      --,MAIN.F_AUTO
      --,MAIN.F_PROP
      --,MAIN.F_CSLTY
      --,MAIN.T_LOSS_DSC
      --,MAIN.C_POL_TYP  
      --,pol.C_POL_TYP_DESC_FULL as  C_POL_TYP
	  ,CASE 
		WHEN ([FLAG_LEG_FIRE] > 0 OR [FLAG_UF_FIRE] > 0) AND [FLAG_UF_AUTO] = 0 and [FLAG_UF_LightExpl] = 0 then 'Basic' 
		WHEN ([FLAG_LEG_FIRE] > 0 OR [FLAG_UF_FIRE] > 0) AND [FLAG_UF_LightExpl] > 0 and [FLAG_UF_AUTO] = 0 THEN 'Basic + Lighting/Fire'
		WHEN ([FLAG_LEG_FIRE] > 0 OR [FLAG_UF_FIRE] > 0) AND [FLAG_UF_AUTO] > 0 and [FLAG_UF_LightExpl] = 0 THEN 'Basic + Auto'		
		WHEN ([FLAG_LEG_FIRE] > 0 OR [FLAG_UF_FIRE] > 0) AND [FLAG_UF_AUTO] > 0 and [FLAG_UF_LightExpl] > 0 THEN 'Basic + Auto + Lighting/Fire'
		WHEN ([FLAG_LEG_FIRE] > 0 OR [FLAG_UF_FIRE] = 0) AND [FLAG_UF_AUTO] = 0 and [FLAG_UF_LightExpl] > 0 THEN 'Lighting/Fire only'
		WHEN ([FLAG_LEG_FIRE] = 0 OR [FLAG_UF_FIRE] = 0) AND [FLAG_UF_AUTO] > 0 and [FLAG_UF_LightExpl] = 0 THEN 'Auto Fire only'
	   end as FM_Perspective	
	  ,CASE 
		when ([FLAG_VALID_LOSS_ESTIMATE] > 0 OR [FLAG_VALID_LOSS_SETTLEMENT] > 0) THEN 'Y' ELSE 'N' 
	   end as FLAG_Amount_Qualifier_Provided
      --,MAIN.A_EST_LOSS  -- // REMOVED AFTER TESTING
      --,MAIN.A_STTLMT    -- // REMOVED AFTER TESTING   
      ,case when [FLAG_LEG_FIRE] > 0		then 'Y' ELSE 'N' END AS [FLAG_LEG_FIRE]		-- // REMOVED AFTER TESTING
      ,case when [FLAG_UF_FIRE] > 0			then 'Y' ELSE 'N' END AS [FLAG_UF_FIRE]			-- // REMOVED AFTER TESTING
      ,case when [FLAG_UF_LightExpl] > 0	then 'Y' ELSE 'N' END AS [FLAG_UF_LightExpl]	-- // REMOVED AFTER TESTING
      ,case when [FLAG_UF_AUTO] > 0			then 'Y' ELSE 'N' END AS [FLAG_UF_AUTO]			-- // REMOVED AFTER TESTING
      ,case when [FLAG_VALID_LOSS_DESC] > 0 then 'Y' ELSE 'N' END AS [FLAG_VALID_LOSS_DESC] -- // REMOVED AFTER TESTING
      ,case when fm.I_ALLCLM is null		then 'N' else 'Y' end as Included_in_FM_Report	-- // REMOVED AFTER TESTING
      ,MAIN.Date_Insert		
      ,MAIN.C_LOL_ST_ALPH	-- // REMOVED AFTER TESTING
      --,MAIN.I_CUST          -- // REMOVED AFTER TESTING      
      ,CASE 
		WHEN sttab.F_FM = 'A' THEN 'Active'
		WHEN sttab.F_FM = 'I' THEN 'Inactive'
		WHEN sttab.F_FM = 'P' THEN 'Passive'
		else 'Non FireMarshal State'		
	   End As 'State_Status'
      
  INTO [ClaimSearch_Dev].[dbo].[FM_ExtractFile]       
  FROM [ClaimSearch_Dev].[dbo].[Proc_FM_Tags]main   
  
  left join [ClaimSearch_Prod].[dbo].[CLT000FM]FM
  on fm.I_ALLCLM = MAIN.I_ALLCLM 
  
  left join [ClaimSearch_Prod].[dbo].[Dashboard_COM_State]sttab
  on sttab.state = MAIN.C_LOL_ST_ALPH
  
  left join [ClaimSearch_Prod].[dbo].[Dim_Policy_Type]pol
  on pol.C_POL_TYP = main.C_POL_TYP
  
  left join [ClaimSearch_Prod].[dbo].[V_MM_Hierarchy]cust
  on cust.lvl0 = main.I_CUST
  
  where FLAG_TEST_COMP = 0  -- // REMOVED AFTER TESTING
  and MAIN.C_LOL_ST_ALPH is not null
  
  create index i_allclm_idx on [ClaimSearch_Dev].[dbo].[FM_ExtractFile] (I_ALLCLM)
  
  
  --select * from [ClaimSearch_Dev].[dbo].[FM_ExtractFile]       
  

  
