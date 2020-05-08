SELECT  
	FS_PA_Matching_All.I_ALLCLM,
	X_NENT_NSSN_max,
	X_NENT_NSSN_max_tp,

	X_NENT_by_SSN_max,
	X_NENT_by_SSN_max_tp,

	F_SSN_multiENT,
	X_NSSN_SSNEnt_max_tp,
	*
FROM CDP.FS_PA_Matching_All
WHERE
	FS_PA_Matching_All.I_ALLCLM IN
	(
		
		'0A004598446',
		'0A004632010',
		'0A004883521',
		'0A004875906',
		'0B004605703',
		'0B004814887',
		'0G004730565',
		'0R004605675',
		'1G004869260',
		'1D004749045'
	)
	
ORDER BY
	FS_PA_Matching_All.I_ALLCLM
	
	
SELECT 
RSN_F_SSN_multiENT,
RSN_X_NSSN_SSNEnt_max_tp,
* FROM CDP.ReasonCodeOutput_Filtered_20190726
WHERE
ReasonCodeOutput_Filtered_20190726.I_ALLCLM IN
(
	'0A004598446',
	'0A004632010',
	'0A004883521',
	'0A004875906',
	'0B004605703',
	'0B004814887',
	'0G004730565',
	'0R004605675',
	'1G004869260',
	'1D004749045'
)
ORDER BY
	I_ALLCLM

SELECT
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM,
	UniquePersonSSNSet.I_NM_ADR,
	UniquePersonSSNSet.name,
	UniquePersonSSNSet.matched_SSN,
	RSN_F_SSN_multiENT,
	RSN_X_NSSN_SSNEnt_max_tp,
	ISNULL(MyCodeEx.ssnRuleDescription,'Including this loss, this involved party''s SSN, {XXXXX__NA}, is linked to NA or more involved parties in the ClaimSearch database') AS ssnRuleDescription,
	MyCodeEx.countValue
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	LEFT OUTER JOIN
	(
		SELECT
			FS_PA_Matching_SSN.I_ALLCLM,
			FS_PA_Matching_SSN.I_NM_ADR,
			FS_PA_Matching_SSN.name,
			FS_PA_Matching_SSN.matched_SSN,
			ROW_NUMBER() OVER(
				PARTITION BY
					FS_PA_Matching_SSN.I_ALLCLM,
					FS_PA_Matching_SSN.I_NM_ADR
				ORDER BY
					Date_Insert DESC
			) AS uniqueInstanceValue
		FROM
			CDP.FS_PA_Matching_SSN
	) AS UniquePersonSSNSet
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = UniquePersonSSNSet.I_ALLCLM
	OUTER APPLY (
			SELECT
				CAST(
					('Including this loss, this involved party''s SSN, {' 
						--+ CAST(LTRIM(RTRIM(InnerCLT00007.N_SSN)) AS VARCHAR(36)) /*socialSecurityNumberObfuscated*/
						+ 'xxxxx' + CAST(LTRIM(RTRIM(ISNULL(InnerCLT0007A.SSN_4,'__NA'))) AS VARCHAR(4)) /*socialSecurityNumberLastFour*/
						+ '}, is linked to '
						--+ CAST(FS_PA_Matching_All.F_SSN_multiENT AS VARCHAR(50))
						+ CAST(COUNT(*) AS VARCHAR(10))
						+ ' or more involved parties in the ClaimSearch database'
					)
				AS VARCHAR(8000)) AS ssnRuleDescription,
				COUNT(*) AS countValue
			FROM
				CDP.FS_PA_Matching_SSN AS InnerCLT00007 WITH (NOLOCK)
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0007A AS InnerCLT0007A WITH(NOLOCK)
					ON InnerCLT00007.I_ALLCLM = InnerCLT0007A.I_ALLCLM
					AND InnerCLT00007.I_NM_ADR = InnerCLT0007A.I_NM_ADR
			WHERE
				InnerCLT00007.matched_SSN = UniquePersonSSNSet.matched_SSN
			GROUP BY
				InnerCLT00007.matched_SSN,
				InnerCLT0007A.SSN_4
		) AS MyCodeEx
WHERE
	ISNULL(UniquePersonSSNSet.uniqueInstanceValue,1) = 1
	AND ISNULL(MyCodeEx.countValue,2) > 1
	AND ReasonCodeOutput_Filtered_20190726.I_ALLCLM IN
		(
			'0A004598446',
			'0A004632010',
			'0A004883521',
			'0A004875906',
			'0B004605703',
			'0B004814887',
			'0G004730565',
			'0R004605675',
			'1G004869260',
			'1D004749045'
		)
ORDER BY
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM
