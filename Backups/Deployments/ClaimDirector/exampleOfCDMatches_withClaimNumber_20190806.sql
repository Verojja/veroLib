
--'CDP.FS_PA_Claim'.I_ALLCLM'
SELECT DISTINCT
	ListThree.displayOrder,
	ReasonCodeOutput_Filtered.I_ALLCLM,
	CLT0001A.N_CLM AS nonDataScienceData_ClaimNumber,
	ReasonCodeOutput_Filtered.featureName,
	CASE
		WHEN
			FS_PA_Matching_Entity.C_MTCH_RSN IS NULL
		THEN
			0
		ELSE
			1
	END AS isDataScienceDataPresent
	----ReasonCodeOutput_Filtered.displayOrder,
	--FS_PA_Matching_Entity.C_MTCH_RSN,
	--FS_PA_Matching_Entity.*
FROM
	(
			VALUES
				(2, '5T005005208', 'RSN_F_SSN_multiENT'),
				(3, '6J004877903', 'RSN_X_NLIC_LICEnt_avg'),
				(4, '7N005014357', 'RSN_X_NLIC_LICEnt_min_fp'),
				(5, '4L004831473', 'RSN_X_NENT_NLIC_avg'),
				(6, '3G004882041', 'RSN_X_NSSN_SSNEnt_max_tp'),
				(7, '3G004835207', 'RSN_N_EFFDT_TO_LOSS'),
				(8, '9F005002959', 'RSN_NIGHT_FLAG'),
				(9, '1Z004996326', 'RSN_X_NENT_AAA_avg'),
				(10, '8N005007747', 'RSN_X_NENT_APA_max'),
				(11, '8Q004896817', 'RSN_X_NENT_BODI_max'),
				(12, '4I004995117', 'RSN_X_NENT_Clmtag_max'),
				(13, '4I004995117', 'RSN_X_NENT_Clmtag_max_fp'),
				(14, '2Z004831053', 'RSN_X_NENT_CUSPA_max'),
				(15, '6P005022389', 'RSN_X_NENT_IAA_min_fp'),
				(16, '9D004742042', 'RSN_X_NENT_SPtag_max'),
				(17, '1J004764222', 'RSN_NVIN_FRD_fp_max'),
				(18, '5D004875867', 'RSN_NVIN_FPA_fp_max'),
				(19, '4S004897977', 'RSN_NVIN_CVGCOLL_fp_max'),
				(20, '2B004905710', 'RSN_Ent_Count_fp_max'),
				(21, '2W005001069', 'RSN_X_NENT_D2LSAA_min_fp'),
				(22, '4J004905202', 'RSN_X_NENT_D2LSPL_min_tp'),
				(23, '9L004798084', 'RSN_Intelli_Fraud'),
				(24, '6Y005013153', 'RSN_N_CRMN_glty'),
				(25, '5C005006111', 'RSN_N_CRMN_RT_C'),
				(NULL, '0Z004906038', 'RSN_N_CRMN_RT_C'),
				(NULL, '0Z004906163', 'RSN_NIGHT_FLAG'),
				(NULL, '0Z004909181', 'RSN_NIGHT_FLAG'),
				(NULL, '0Z004909181', 'RSN_N_CRMN_glty'),
				(NULL, '0Z004909181', 'RSN_N_CRMN_RT_C'),
				(NULL, '0Z004911373', 'RSN_NIGHT_FLAG'),
				(NULL, '0Z004913217', 'RSN_NIGHT_FLAG'),
				(NULL, '0Z004914526', 'RSN_NIGHT_FLAG'),
				--(NULL, '0Z004914786', 'RSN_NIGHT_FLAG'),
				--(NULL, '3J004719157', 'RSN_NIGHT_FLAG'),
				--(NULL, '3J004719944', 'RSN_NIGHT_FLAG'),
				(NULL, '3J004720571', 'RSN_N_CRMN_glty'),
				(NULL, '3J004720571', 'RSN_N_CRMN_RT_C')
		) AS ReasonCodeOutput_Filtered (displayOrder, I_ALLCLM, featureName)
		INNER JOIN (
			VALUES
				('RSN_F_SSN_multiENT',  'F_SSN_multiENT3y'),
				('RSN_X_NLIC_LICEnt_avg', 'X_NLIC_LICEnt3y_avg'),
				('RSN_X_NLIC_LICEnt_min_fp', 'X_NLIC_LICEnt3y_min_fp'),
				('RSN_X_NENT_NLIC_avg', 'X_NENT_NLIC3y_avg'),
				('RSN_X_NSSN_SSNEnt_max_tp', 'X_NSSN_SSNEnt3y_max_tp'),
				('RSN_N_EFFDT_TO_LOSS', 'N_EFFDT_TO_LOSS'),
				('RSN_NIGHT_FLAG', 'NIGHT_FLAG'),
				('RSN_X_NENT_AAA_avg', 'X_NENT_AAA3y_avg'),
				('RSN_X_NENT_APA_max', 'X_NENT_APA3y_max'),
				('RSN_X_NENT_BODI_max', 'X_NENT_BODI3y_max'),
				('RSN_X_NENT_Clmtag_max', 'X_NENT_Clmtag3y_max'),
				('RSN_X_NENT_Clmtag_max_fp', 'X_NENT_Clmtag3y_max_fp'),
				('RSN_X_NENT_CUSPA_max', 'X_NENT_CUSPA3y_max'),
				('RSN_X_NENT_IAA_min_fp', 'X_NENT_IAA3y_min_fp'),
				('RSN_X_NENT_SPtag_max', 'X_NENT_SPtag3y_max'),
				('RSN_NVIN_FRD_fp_max', 'NVIN_FRD3y_fp_max'),
				('RSN_NVIN_FPA_fp_max', 'NVIN_FPA3y_fp_max'),
				('RSN_NVIN_CVGCOLL_fp_max', 'NVIN_CVGCOLL3y_fp_max'),
				('RSN_Ent_Count_fp_max', 'Ent_Count_3y_fp_max'),
				('RSN_X_NENT_D2LSAA_min_fp', 'X_NENT_D2LSAA_min_fp'),
				('RSN_X_NENT_D2LSPL_min_tp', 'X_NENT_D2LSPL_min_tp'),
				('RSN_Intelli_Fraud', 'Intelli_Fraud_fp'),
				('RSN_N_CRMN_glty', 'N_CRMN_glty'),
				('RSN_N_CRMN_RT_C', 'N_CRMN_RT_C')
		) AS ListTwo (reasonfileFeatureName, excelFeatureName)
			ON ReasonCodeOutput_Filtered.featureName = ListTwo.reasonfileFeatureName
		INNER JOIN (
			VALUES
				(0, 'LSS_Cov_numeric_top3_avg'),
				(1,  'LSS_Cov_numeric_top2_avg'),
				(2,  'F_SSN_multiENT3y'),
				(3,  'X_NLIC_LICEnt3y_avg'),
				(4,  'X_NLIC_LICEnt3y_min_fp'),
				(5,  'X_NENT_NLIC3y_avg'),
				(6,  'X_NSSN_SSNEnt3y_max_tp'),
				(7,  'N_EFFDT_TO_LOSS'),
				(8,  'NIGHT_FLAG'),
				(9,  'X_NENT_AAA3y_avg'),
				(10, 'X_NENT_APA3y_max'),
				(11, 'X_NENT_BODI3y_max'),
				(12, 'X_NENT_Clmtag3y_max'),
				(13, 'X_NENT_Clmtag3y_max_fp'),
				(14, 'X_NENT_CUSPA3y_max'),
				(15, 'X_NENT_IAA3y_min_fp'),
				(16, 'X_NENT_SPtag3y_max'),
				(17, 'NVIN_FRD3y_fp_max'),
				(18, 'NVIN_FPA3y_fp_max'),
				(19, 'NVIN_CVGCOLL3y_fp_max'),
				(20, 'Ent_Count_3y_fp_max'),
				(21, 'X_NENT_D2LSAA_min_fp'),
				(22, 'X_NENT_D2LSPL_min_tp'),
				(23, 'Intelli_Fraud_fp'),
				(24, 'N_CRMN_glty'),
				(25, 'N_CRMN_RT_C')
		) AS ListThree (displayOrder, excelFeatureName)
			ON ListTwo.excelFeatureName = ListThree.excelFeatureName
		LEFT OUTER JOIN CDP.FS_PA_Matching_Entity
			ON FS_PA_Matching_Entity.I_ALLCLM = ReasonCodeOutput_Filtered.I_ALLCLM
		LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
			ON CLT0001A.I_ALLCLM = ReasonCodeOutput_Filtered.I_ALLCLM
			
ORDER BY 
	ListThree.displayOrder,
	ReasonCodeOutput_Filtered.I_ALLCLM
	--FS_PA_Matching_Entity.C_MTCH_RSN
	--FS_PA_Matching_Entity.name
	
/*		
	('2B004905710', 'RSN_Ent_Count_fp_max'),
	('9L004798084', 'RSN_Intelli_Fraud'),
	('5T005005208', 'RSN_F_SSN_multiENT'),
	('6Y005013153', 'RSN_N_CRMN_glty'),
	('5C005006111', 'RSN_N_CRMN_RT_C'),
	('3G004835207', 'RSN_N_EFFDT_TO_LOSS'),
	('9F005002959', 'RSN_NIGHT_FLAG'),
	('4S004897977', 'RSN_NVIN_CVGCOLL_fp_max'),
	('5D004875867', 'RSN_NVIN_FPA_fp_max'),
	('1J004764222', 'RSN_NVIN_FRD_fp_max'),
	('1Z004996326', 'RSN_X_NENT_AAA_avg'),
	('8N005007747', 'RSN_X_NENT_APA_max'),
	('8Q004896817', 'RSN_X_NENT_BODI_max'),
	('4I004995117', 'RSN_X_NENT_Clmtag_max'),
	('4I004995117', 'RSN_X_NENT_Clmtag_max_fp'),
	('2Z004831053', 'RSN_X_NENT_CUSPA_max'),
	('2W005001069', 'RSN_X_NENT_D2LSAA_min_fp'),
	('4J004905202', 'RSN_X_NENT_D2LSPL_min_tp'),
	('6P005022389', 'RSN_X_NENT_IAA_min_fp'),
	('4L004831473', 'RSN_X_NENT_NLIC_avg'),
	('9D004742042', 'RSN_X_NENT_SPtag_max'),
	('6J004877903', 'RSN_X_NLIC_LICEnt_avg'),
	('7N005014357', 'RSN_X_NLIC_LICEnt_min_fp'),
	('3G004882041', 'RSN_X_NSSN_SSNEnt_max_tp'),
	
	SELECT
		PartitionedUnpivotedReasonCodes.I_ALLCLM,
		PartitionedUnpivotedReasonCodes.featureName,
		PartitionedUnpivotedReasonCodes.uniqueInstanceValue
	FROM
		(
			SELECT
				UnpivotedReasonCodes.I_ALLCLM,
				UnpivotedReasonCodes.featureName,
				ROW_NUMBER() OVER(
					PARTITION BY	
						UnpivotedReasonCodes.featureName
					ORDER BY
						UnpivotedReasonCodes.Date_Insert DESC
				) AS uniqueInstanceValue
			FROM
				(
				/*!!!DEV CODE!!! REMOVE THE SELECT TOP!!!!*/
					--SELECT TOP 10
					SELECT
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
			WHERE
				UnpivotedReasonCodes.isRuleObserved =1
		) AS PartitionedUnpivotedReasonCodes
	WHERE
		PartitionedUnpivotedReasonCodes.uniqueInstanceValue = 1
*/


/*
SELECT TOP 10
	SetComparisonSubQuery.I_ALLCLM
FROM
(
	SELECT
		ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	FROM
		CDP.ReasonCodeOutput_Filtered_20190726 WITH(NOLOCK)
	EXCEPT
	
	SELECT
		FS_PA_Matching_Entity.I_ALLCLM
	FROM
		CDP.FS_PA_Matching_Entity WITH(NOLOCK)
) AS SetComparisonSubQuery


SELECT
	UnpivotedReasonCodes.I_ALLCLM,
	UnpivotedReasonCodes.featureName
FROM
	(
		SELECT
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
		WHERE
			TempReasonCodes.I_ALLCLM IN (
				'0Z004906038',
				'0Z004906163',
				'0Z004909181',
				'0Z004911373',
				'0Z004913217',
				'0Z004914526',
				'0Z004914786',
				'3J004719157',
				'3J004719944',
				'3J004720571'
			)
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
WHERE
	UnpivotedReasonCodes.isRuleObserved =1


*/