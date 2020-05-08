SET NOEXEC OFF;

USE CSDataScience;

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--SELECT @@TRANCOUNT
--ROLLBACK
/*!!!DEV CODE!!! REMOVE THE SELECT TOP!!!!*/

DECLARE @dateInserted DATETIME2(0) = GETDATE();
DECLARE @DateFilter3Year INT = CAST(
	REPLACE(
		CAST(
			CAST(
				DATEADD(
					YEAR,
					-3,
					GETDATE()
				)
			AS DATE)
		AS CHAR(10)),
		'-',
		''
	)
AS INT);

INSERT INTO CDP.ReasonsExportSample
SELECT TOP 16000
--COUNT(*)
	UnpivotedReasonCodes.I_ALLCLM AS [ClaimID],
	UnpivotedReasonCodes.I_ALLCLM AS [ISO File Number],
	--UnpivotedReasonCodes.featureName,
	--UnpivotedReasonCodes.number_RSN,
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS [ReasonIndex],
	RuleDescriptionApply.ruleDescriptionValue AS [RC_Description],
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UPPER(SUBSTRING(CLT00004.M_FUL_NM,1,1)) + 'xxxxx' + UPPER(UPPER(SUBSTRING(CLT00004.M_FUL_NM,1,1))) + 'xxxxx' AS [Matching Party],
	--UnpivotedReasonCodes.Date_Insert AS [Date of Match],
	
	--[Drivers License Number],
	--[Issuing State],
	
	--INTO dbo.CDPReasonExtract
	@dateInserted AS dateInserted
FROM
	(
	/*!!!DEV CODE!!! REMOVE THE SELECT TOP!!!!*/
		SELECT --TOP 10
		--SELECT
			TempReasonCodes.I_ALLCLM,
			TempReasonCodes.Date_Insert,
			TempReasonCodes.final_score,
			TempReasonCodes.number_RSN,
			TempReasonCodes.RSN_F_SSN_multiENT,
			TempReasonCodes.RSN_X_NLIC_LICEnt_avg,
			TempReasonCodes.RSN_X_NLIC_LICEnt_min_fp,
			TempReasonCodes.RSN_X_NENT_NLIC_avg,
			TempReasonCodes.RSN_X_NSSN_SSNEnt_max_tp,
			TempReasonCodes.RSN_N_EFFDT_TO_LOSS,
			TempReasonCodes.RSN_NIGHT_FLAG,
			TempReasonCodes.RSN_X_NENT_AAA_avg,
			
			TempReasonCodes.RSN_X_NENT_APA_max,
			TempReasonCodes.RSN_X_NENT_BODI_max,
			TempReasonCodes.RSN_X_NENT_Clmtag_max,
			TempReasonCodes.RSN_X_NENT_Clmtag_max_fp,
			TempReasonCodes.RSN_X_NENT_CUSPA_max,
			TempReasonCodes.RSN_X_NENT_IAA_min_fp,
			TempReasonCodes.RSN_X_NENT_SPtag_max,
			TempReasonCodes.RSN_NVIN_FRD_fp_max,
			TempReasonCodes.RSN_NVIN_FPA_fp_max,
			TempReasonCodes.RSN_NVIN_CVGCOLL_fp_max,
			TempReasonCodes.RSN_Ent_Count_fp_max,
			TempReasonCodes.RSN_X_NENT_D2LSAA_min_fp,
			TempReasonCodes.RSN_X_NENT_D2LSPL_min_tp,
			TempReasonCodes.RSN_Intelli_Fraud,
			TempReasonCodes.RSN_N_CRMN_glty,
			TempReasonCodes.RSN_N_CRMN_RT_C
		FROM
			CDP.ReasonCodeOutput_Filtered_20190726 AS TempReasonCodes
		/*!!!DEV CODE!!! REMOVE THE FOLLOWING FILTER*/
		--WHERE
			--TempReasonCodes.I_ALLCLM = '0C004606922' /*RSN_F_SSN_multiENT  //  F_SSN_multiENT3y  - 3 min 30sec*/
			--AND UnpivotedReasonCodes.I_ALLCLM = '0A004891949' /*RSN_X_NLIC_LICEnt_min_fp // X_NLIC_LICEnt3y_min_fp */
	) AS PivotTarget
	UNPIVOT (
		isRuleObserved FOR featureName IN (
			PivotTarget.RSN_F_SSN_multiENT,
			PivotTarget.RSN_X_NLIC_LICEnt_avg,
			PivotTarget.RSN_X_NLIC_LICEnt_min_fp,
			PivotTarget.RSN_X_NENT_NLIC_avg,
			PivotTarget.RSN_X_NSSN_SSNEnt_max_tp,
			PivotTarget.RSN_N_EFFDT_TO_LOSS,
			PivotTarget.RSN_NIGHT_FLAG,
			PivotTarget.RSN_X_NENT_AAA_avg,
			PivotTarget.RSN_X_NENT_APA_max,
			PivotTarget.RSN_X_NENT_BODI_max,
			PivotTarget.RSN_X_NENT_Clmtag_max,
			PivotTarget.RSN_X_NENT_Clmtag_max_fp,
			PivotTarget.RSN_X_NENT_CUSPA_max,
			PivotTarget.RSN_X_NENT_IAA_min_fp,
			PivotTarget.RSN_X_NENT_SPtag_max,
			PivotTarget.RSN_NVIN_FRD_fp_max,
			PivotTarget.RSN_NVIN_FPA_fp_max,
			PivotTarget.RSN_NVIN_CVGCOLL_fp_max,
			PivotTarget.RSN_Ent_Count_fp_max,
			PivotTarget.RSN_X_NENT_D2LSAA_min_fp,
			PivotTarget.RSN_X_NENT_D2LSPL_min_tp,
			PivotTarget.RSN_Intelli_Fraud,
			PivotTarget.RSN_N_CRMN_glty,
			PivotTarget.RSN_N_CRMN_RT_C
		)
	) AS UnpivotedReasonCodes
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON UnpivotedReasonCodes.I_ALLCLM = CLT0001A.I_ALLCLM
	--INNER JOIN CDP.FS_PA_Matching_All
	--	ON FS_PA_Matching_All.I_ALLCLM = UnpivotedReasonCodes.I_ALLCLM
	CROSS APPLY (
		SELECT
			CASE
--				/*These two feature names are currently not included in the RuleFile (Li, Shiu-tang 20190729)*//*
--				WHEN
--					UnpivotedReasonCodes.featureName =('LSS_Cov_numeric_top3_avg','This loss type and coverage combination are correlated with questionable claims.'), /*LSS_Cov_numeric_top3_avg*/
--				WHEN
--					UnpivotedReasonCodes.featureName =('LSS_Cov_numeric_top2_avg','This loss type and coverage combination are correlated with questionable claims.'), /*LSS_Cov_numeric_top2_avg*/
--				*/
--/*1*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_F_SSN_multiENT'  /*F_SSN_multiENT3y*/
--				THEN
--					(
--						SELECT
--							CAST(
--								('Including this loss, this involved party''s SSN, {' 
--									--+ CAST(LTRIM(RTRIM(InnerCLT00007.N_SSN)) AS VARCHAR(36)) /*socialSecurityNumberObfuscated*/
--									+ 'XXXXX' + CAST(LTRIM(RTRIM(InnerCLT0007A.SSN_4)) AS VARCHAR(4)) /*socialSecurityNumberLastFour*/
--									+ '}, is linked to '
--									+ CAST(FS_PA_Matching_All.F_SSN_multiENT AS VARCHAR(50))
--									+ ' or more involved parties in the ClaimSearch database')
--							AS VARCHAR(8000)) AS ssnRuleDescription
--						FROM
--							ClaimSearch_Prod.dbo.CLT00007 AS InnerCLT00007 WITH (NOLOCK)
--							INNER JOIN ClaimSearch_Prod.dbo.CLT0007A AS InnerCLT0007A WITH(NOLOCK)
--								ON InnerCLT00007.I_ALLCLM = InnerCLT0007A.I_ALLCLM
--								AND InnerCLT00007.I_NM_ADR = InnerCLT0007A.I_NM_ADR
--						WHERE
--							InnerCLT00007.N_SSN = CLT00007.N_SSN
--					)
--/*4*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_X_NLIC_LICEnt_avg' /*X_NLIC_LICEnt3y_avg*/
--				THEN
--					'This Drivier''s license is associated to multiple parties in the ClaimSearch Database.'
--/*5*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_X_NLIC_LICEnt_min_fp' /*X_NLIC_LICEnt3y_min_fp*/
--				THEN
--					(
--						SELECT
--							CAST(
--								''
--								+ CAST(COUNT(*) AS VARCHAR(50))
--								+ ' Number of people have used Drivers License '
--								--+ CAST(LTRIM(RTRIM(InnerCLT00008.N_DRV_LIC)) AS VARCHAR(52)) /*driversLicenseNumberObfuscated*/
--								+ CAST(LTRIM(RTRIM(InnerCLT0008A.N_DRV_LIC)) AS VARCHAR(3)) /*driversLicenseNumberLast3*/
--								+' in the last 3 years in the ClaimSearch database.'
--							AS VARCHAR(8000)) AS drLicenseMinRuleDescription
--						FROM
--							ClaimSearch_Prod.dbo.CLT00008 AS InnerCLT00008 WITH (NOLOCK)
--							INNER JOIN ClaimSearch_Prod.dbo.CLT0008A AS InnerCLT0008A WITH(NOLOCK)
--								ON InnerCLT00008.I_ALLCLM = InnerCLT0008A.I_ALLCLM
--								AND InnerCLT00008.I_NM_ADR = InnerCLT0008A.I_NM_ADR
--						WHERE
--							InnerCLT00008.N_DRV_LIC = CLT00008.N_DRV_LIC
--							AND InnerCLT00008.Date_Insert >= @DateFilter3Year
--						GROUP BY
--							InnerCLT00008.N_DRV_LIC,
--							InnerCLT0008A.N_DRV_LIC
--					)
--/*6*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_NLIC_avg' /*X_NENT_NLIC3y_avg*/
--				THEN
--					'Bxx Sxxxx has used X driver''s licenses in the past 3 years in the ClaimSearch database.'
--/*3*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_X_NSSN_SSNEnt_max_tp' /*X_NSSN_SSNEnt3y_max_tp*/
--				THEN
--					'This SSN is linked to X different people in the last 3 years.'
--/*7*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_N_EFFDT_TO_LOSS' /*N_EFFDT_TO_LOSS*/
--				THEN
--					'This loss occurred within X days of the original policy inception date'
--/*2*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_NIGHT_FLAG' /*NIGHT_FLAG*/
--				THEN
--					'This loss occurred between 10PM and Midnight.'
--/*8*/			WHEN
--					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_AAA_avg' /*X_NENT_AAA3y_avg*/
--				THEN
--					'Bxx Sxxxx has had X claims in the last 3 years'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_APA_max' /*X_NENT_APA3y_max*/
				THEN
					'Bxx Sxxxx has had X auto claims in the last 3 years (Excluding Glass and Towing?)'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_BODI_max' /*X_NENT_BODI3y_max*/
				THEN
					'In this occurrence, the [Insured/Claimant actual name](s) reported a bodily injury claim and previously reported X or more bodily injury claims.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_Clmtag_max' /*X_NENT_Clmtag3y_max*/
				THEN
					'The [Insured/Claimant actual name](s) in this occurrence,  has a previous questionable claim within the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_Clmtag_max_fp' /*X_NENT_Clmtag3y_max_fp*/
				THEN
					'The [Insured actual name](s) in this occurrence,  has a previous questionable claim within the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_CUSPA_max' /*X_NENT_CUSPA3y_max*/
				THEN
					'Bxx Sxxxx has been associated with X different auto insurers in the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_IAA_min_fp' /*X_NENT_IAA3y_min_fp*/
				THEN
					'This Insured has has X claims in the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_SPtag_max' /*X_NENT_SPtag3y_max*/
				THEN
					'Bxx Sxxxx has been involved in X questionable claims as a Service Provider within the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_NVIN_FRD_fp_max' /*NVIN_FRD3y_fp_max*/
				THEN
					'VIN XYZ involved in this loss has been involved in X  questionable losses in the past 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_NVIN_FPA_fp_max' /*NVIN_FPA3y_fp_max*/
				THEN
					'VIN XYZ involved in this loss has been involved in X  first party losses in the past 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_NVIN_CVGCOLL_fp_max' /*NVIN_CVGCOLL3y_fp_max*/
				THEN
					'VIN XYZ involved in this loss has been involved in X  first party collision losses in the past 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_Ent_Count_fp_max' /*Ent_Count_3y_fp_max*/
				THEN
					'Including this loss, an involved party''s VIN is linked to X or more different insureds or claimants in the ClaimSearch database in the last 3 years.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_D2LSAA_min_fp' /*X_NENT_D2LSAA_min_fp*/
				THEN
					'The insured had a prior loss X days before this loss.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_X_NENT_D2LSPL_min_tp' /*X_NENT_D2LSPL_min_tp*/
				THEN
					'The claimant had a prior loss X days before this loss.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_Intelli_Fraud' /*Intelli_Fraud_fp*/
				THEN
					'Bxx Sxxxx has a civil or criminal history.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_N_CRMN_glty' /*N_CRMN_glty*/
				THEN
					'Bxx Sxxxx has a civil history and/or a criminal history with guilty conviction.'
/*!!!*/			WHEN
					UnpivotedReasonCodes.featureName = 'RSN_N_CRMN_RT_C' /*N_CRMN_RT_C*/
				THEN
					'Bxx Sxxxx has a criminal history'
			END AS ruleDescriptionValue
	) AS RuleDescriptionApply
WHERE
	UnpivotedReasonCodes.isRuleObserved =1
	AND
	(
		UnpivotedReasonCodes.featureName = 'RSN_X_NENT_APA_max' /*X_NENT_APA3y_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_BODI_max' /*X_NENT_BODI3y_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_Clmtag_max' /*X_NENT_Clmtag3y_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_Clmtag_max_fp' /*X_NENT_Clmtag3y_max_fp*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_CUSPA_max' /*X_NENT_CUSPA3y_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_IAA_min_fp' /*X_NENT_IAA3y_min_fp*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_SPtag_max' /*X_NENT_SPtag3y_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_NVIN_FRD_fp_max' /*NVIN_FRD3y_fp_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_NVIN_FPA_fp_max' /*NVIN_FPA3y_fp_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_NVIN_CVGCOLL_fp_max' /*NVIN_CVGCOLL3y_fp_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_Ent_Count_fp_max' /*Ent_Count_3y_fp_max*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_D2LSAA_min_fp' /*X_NENT_D2LSAA_min_fp*/
		OR UnpivotedReasonCodes.featureName = 'RSN_X_NENT_D2LSPL_min_tp' /*X_NENT_D2LSPL_min_tp*/
		OR UnpivotedReasonCodes.featureName = 'RSN_Intelli_Fraud' /*Intelli_Fraud_fp*/
		OR UnpivotedReasonCodes.featureName = 'RSN_N_CRMN_glty' /*N_CRMN_glty*/
		OR UnpivotedReasonCodes.featureName = 'RSN_N_CRMN_RT_C' /*N_CRMN_RT_C*/
	)
ORDER BY NEWID()	
	
	
	
	--AND UnpivotedReasonCodes.I_ALLCLM = '' /*RSN_X_NLIC_LICEnt_min_fp // X_NLIC_LICEnt3y_min_fp */
	--AND UnpivotedReasonCodes.featureName = 'RSN_X_NLIC_LICEnt_min_fp' /*X_NLIC_LICEnt3y_min_fp*/

--SELECT * FROM dbo.CDPReasonExtract
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
----8,245,722

--(8,245,722 row(s) affected)
--ROLLBACK
--SELECT COUNT(*) FROM CDP.ReasonsExport WITH (NOLOCK)
--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;