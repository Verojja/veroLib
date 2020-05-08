BEGIN TRANSACTION

DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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
/*
	DROP TABLE CDP.ReasonsExportSample
*/

/*******SSN RULE 1 RSN_F_SSN_multiENT *******/
--/*
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(MyCodeEx.ssnRuleDescription,'Including this loss, this involved party''s SSN, {XXXXX__NA}, is linked to NA or more involved parties in the ClaimSearch database') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
	INTO CDP.ReasonsExportSample
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
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
						+ ' or more involved parties in the ClaimSearch database.'
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
	ReasonCodeOutput_Filtered_20190726.RSN_F_SSN_multiENT = 1
	AND ISNULL(UniquePersonSSNSet.uniqueInstanceValue,1) = 1
	AND ISNULL(MyCodeEx.countValue,2) > 1
	
--*/
GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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
/*******SSN RULE 2 RSN_NIGHT_FLAG *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	'This loss occurred between 10PM and Midnight.' AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_NIGHT_FLAG = 1
	
--*/

--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE RC_Description LIKE 'This loss occurred between 10PM%'
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_NIGHT_FLAG = 1
GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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


/*******SSN RULE 3 RSN_X_NSSN_SSNEnt_max_tp *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(MyCodeEx.ssnRuleDescription,'This SSN is linked to X different people in the last 3 years.') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
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
					('This SSN is linked to ' 
						+ CAST(COUNT(*) AS VARCHAR(10))
						+ ' different people in the last 3 years.'
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
				AND InnerCLT00007.Date_Insert >= @dateFilter3Year
			GROUP BY
				InnerCLT00007.matched_SSN,
				InnerCLT0007A.SSN_4
		) AS MyCodeEx
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_X_NSSN_SSNEnt_max_tp = 1
	AND ISNULL(UniquePersonSSNSet.uniqueInstanceValue,1) = 1
	AND ISNULL(MyCodeEx.countValue,2) > 1
	
--*/

--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE RC_Description LIKE 'This SSN is linked to %'
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_X_NSSN_SSNEnt_max_tp = 1
GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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

/*******SSN RULE 4 RSN_NIGHT_FLAG *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	'This Drivier''s license is associated to multiple parties in the ClaimSearch Database.' AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_X_NLIC_LICEnt_avg = 1
	
--*/

--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE RC_Description LIKE 'This Drivier''s license is associated to multiple parties in the ClaimSearch Database.%'
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_X_NLIC_LICEnt_avg = 1
--SELECT COUNT(DISTINCT dateInserted) FROM CDP.ReasonsExportSample
GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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
/*******SSN RULE 5  RSN_X_NLIC_LICEnt_min_fp*******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(
		(
			''
			+ CAST(FS_PA_Matching_All.X_NLIC_LICEnt_min_fp AS VARCHAR(50))
			+ ' Number of people have used Drivers License '
			+ ISNULL(
				OuterApplyDL.dlLastThree,
				'xyz'
			)
			+ ' in the last 3 years in the ClaimSearch database.'
		)
		,'X Number of people have used Drivers License xyz in the last 3 years in the ClaimSearch database.') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_Matching_All 
		ON FS_PA_Matching_All.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	LEFT OUTER JOIN (
		SELECT
			FS_PA_Matching_DL.I_ALLCLM,
			FS_PA_Matching_DL.matched_DL_number,
			ROW_NUMBER() OVER(
			PARTITION BY
				FS_PA_Matching_DL.I_ALLCLM
			ORDER BY
				FS_PA_Matching_DL.Date_Insert DESC
		) AS uniqueInstanceValue
		FROM
			CDP.FS_PA_Matching_DL
		WHERE
			FS_PA_Matching_DL.matched_DL_number IS NOT NULL
			AND FS_PA_Matching_DL.Date_Insert >= @dateFilter3Year
	) AS FS_PA_Matching_DL
		ON FS_PA_Matching_DL.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	OUTER APPLY (
		SELECT
			CAST(LTRIM(RTRIM(InnerCLT0008A.N_DRV_LIC)) AS VARCHAR(3)) /*driversLicenseNumberLast3*/ AS dlLastThree
		FROM
			ClaimSearch_Prod.dbo.CLT00008 AS InnerCLT00008 WITH (NOLOCK)
			INNER JOIN ClaimSearch_Prod.dbo.CLT0008A AS InnerCLT0008A WITH(NOLOCK)
				ON InnerCLT00008.I_ALLCLM = InnerCLT0008A.I_ALLCLM
				AND InnerCLT00008.I_NM_ADR = InnerCLT0008A.I_NM_ADR
		WHERE
			InnerCLT00008.N_DRV_LIC = FS_PA_Matching_DL.matched_DL_number
		GROUP BY
			InnerCLT00008.N_DRV_LIC,
			InnerCLT0008A.N_DRV_LIC
	) AS OuterApplyDL
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_X_NLIC_LICEnt_min_fp = 1
	AND ISNULL(FS_PA_Matching_DL.uniqueInstanceValue,1) =1

--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE dateInserted = @dateInserted
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_X_NLIC_LICEnt_min_fp = 1
--*/
GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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
/*******SSN RULE 6 RSN_X_NENT_NLIC_avg *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(
		(
			+ ISNULL(FS_PA_Matching_DL.name, 'Bxx Sxxxx')
			+ ' has used '
			+ ISNULL(
				CAST(FS_PA_Matching_All.X_NENT_NLIC_avg AS VARCHAR(50)),
				'x'
			)
			+ ' driver''s licenses in the past 3 years in the ClaimSearch database.'
		)
		,'Bxx Sxxxx has used X driver''s licenses in the past 3 years in the ClaimSearch database.') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_Matching_All 
		ON FS_PA_Matching_All.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	LEFT OUTER JOIN (
		SELECT
			FS_PA_Matching_DL.I_ALLCLM,
			FS_PA_Matching_DL.name,
			ROW_NUMBER() OVER(
			PARTITION BY
				FS_PA_Matching_DL.I_ALLCLM
			ORDER BY
				FS_PA_Matching_DL.Date_Insert DESC
		) AS uniqueInstanceValue
		FROM
			CDP.FS_PA_Matching_DL
		WHERE
			FS_PA_Matching_DL.matched_DL_number IS NOT NULL
			AND FS_PA_Matching_DL.Date_Insert >= @dateFilter3Year
	) AS FS_PA_Matching_DL
		ON FS_PA_Matching_DL.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	--OUTER APPLY (
	--	SELECT
	--		CAST(LTRIM(RTRIM(InnerCLT0008A.N_DRV_LIC)) AS VARCHAR(3)) /*driversLicenseNumberLast3*/ AS dlLastThree
	--	FROM
	--		ClaimSearch_Prod.dbo.CLT00008 AS InnerCLT00008 WITH (NOLOCK)
	--		INNER JOIN ClaimSearch_Prod.dbo.CLT0008A AS InnerCLT0008A WITH(NOLOCK)
	--			ON InnerCLT00008.I_ALLCLM = InnerCLT0008A.I_ALLCLM
	--			AND InnerCLT00008.I_NM_ADR = InnerCLT0008A.I_NM_ADR
	--	WHERE
	--		InnerCLT00008.N_DRV_LIC = FS_PA_Matching_DL.matched_DL_number
	--	GROUP BY
	--		InnerCLT00008.N_DRV_LIC,
	--		InnerCLT0008A.N_DRV_LIC
	--) AS OuterApplyDL
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_X_NENT_NLIC_avg = 1
	AND ISNULL(FS_PA_Matching_DL.uniqueInstanceValue,1) =1

--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE dateInserted = @dateInserted
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_X_NENT_NLIC_avg = 1
--*/

GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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
/*******SSN RULE 7 RSN_N_EFFDT_TO_LOSS *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(
		(
			+ 'This loss occurred within '
			+ ISNULL(
				CAST(FS_PA_NonMatching_All.N_EFFDT_TO_LOSS AS VARCHAR(50)),
				'x'
			)
			+ ' days of the original policy inception date.'
		)
		,'This loss occurred within X days of the original policy inception date.') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_NonMatching_All 
		ON FS_PA_NonMatching_All.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_N_EFFDT_TO_LOSS = 1

SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE dateInserted = @dateInserted
SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_N_EFFDT_TO_LOSS = 1
--*/

GO
DECLARE @dateInserted DATETIME2(0) = GETDATE();

DECLARE @dateFilter3Year INT = CAST(
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

/*******SSN RULE 8 RSN_X_NENT_AAA_avg *******/
--/*
INSERT INTO CDP.ReasonsExportSample
SELECT TOP 1000
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS ClaimID,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	NULL AS ReasonIndex,
	ISNULL(
		(
			+ ISNULL(OuterApply.name,'Bxx Sxxxx')
			+' has had '
			+ ISNULL(
				CAST(FS_PA_Matching_All.X_NENT_AAA_avg AS VARCHAR(50)),
				'x'
			)
			+ ' claims in the last 3 years.'
		)
		,'Bxx Sxxxx has had X claims in the last 3 years.') AS RC_Description,
	NULL AS [Index],
	NULL AS [Reason Description],
	
	--UniquePersonSSNSet.I_NM_ADR,
	--UniquePersonSSNSet.name,
	--UniquePersonSSNSet.matched_SSN,
	--RSN_F_SSN_multiENT,
	--RSN_X_NSSN_SSNEnt_max_tp,
	--MyCodeEx.countValue,
	
	@dateInserted AS dateInserted
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_Matching_All 
		ON FS_PA_Matching_All.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
	OUTER APPLY (
		SELECT
			FS_PA_Entity.name,
			ROW_NUMBER() OVER(
				PARTITION BY
					FS_PA_Entity.I_ALLCLM,
					FS_PA_Entity.name
				ORDER BY
					FS_PA_Entity.Date_Insert
			) AS uniqueInstanceValue
		FROM
			CDP.FS_PA_Entity
		WHERE
			FS_PA_Entity.I_ALLCLM = ReasonCodeOutput_Filtered_20190726.I_ALLCLM
			AND FS_PA_Entity.Date_Insert >=@dateFilter3Year
	) AS OuterApply
WHERE
	ReasonCodeOutput_Filtered_20190726.RSN_X_NENT_AAA_avg = 1
	AND ISNULL(OuterApply.uniqueInstanceValue,1) = 1
	
--SELECT COUNT(*) FROM CDP.ReasonsExportSample WHERE dateInserted = @dateInserted
--SELECT COUNT(*) FROM CDP.ReasonCodeOutput_Filtered_20190726 WHERE ReasonCodeOutput_Filtered_20190726.RSN_X_NENT_AAA_avg = 1
--*/
--4,668,782

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;