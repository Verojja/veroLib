BEGIN TRANSACTION

DECLARE @dateInserted DATETIME2(0) = GETDATE();
--DROP TABLE CDP.InvolvedPartyExtract
/*
				CLAIM DATA
				
ISO Claim ID
I_CUST
ISO File Number
Claim Number
Policy Number
Loss Date
Loss Type Code
T_LOSS_TYP_GRP
Loss Type
Policy Type Code
C_POL_TYP_grp
Policy Type
Scored Date
Score
Policy Inception Date
Policy Expiration Date
First Report Date
Address
Suite
City
Loss State
Zip
LossAddr_Lat
LossAddr_Long

					IP DATA


ClaimID
ISO File Number
Claim Number
Policy Number
Loss Date
LossType
Policy Type
Scored Date
Score
Involved Party Index
Role Code
Role
SSN
Diver License
Involved Party ID
MD_FirstName1
MD_MiddleName1
MD_LastName1
Address
Suite
City
State
Zip
ResidAddr_Lat
ResidAddr_Long

*/
--8,245,722
	
--8,245,722

/************************	CLM		************************/
/*
SELECT --TOP 10
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO Claim ID],
	CLT0001A.I_CUST AS I_CUST,
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	ISNULL(LTRIM(RTRIM(CLT0001A.N_POL)),'NA') AS [Policy Number],
	CLT0001A.D_OCUR AS [Loss Date],
	CLT0001A.C_LOSS_TYP AS [Loss Type Code],
	NULL AS T_LOSS_TYP_GRP,
	CAST(Dim_Loss_Type.T_LOSS_TYP AS VARCHAR(42)) AS [Loss Type],
	ISNULL(NULLIF(LTRIM(RTRIM(CLT0001A.C_POL_TYP)),''),'NA') AS [Policy Type Code],
	NULL AS C_POL_TYP_grp,
	NULLIF(LTRIM(RTRIM(UniquePolicyTypeInstance.policyTypeDescription)),'') AS [Policy Type],
	ReasonCodeOutput_Filtered_20190726.Date_Insert AS [Scored Date],
	ReasonCodeOutput_Filtered_20190726.final_score AS Score,
	CLT0001A.D_POL_INCP AS [Policy Inception Date],
	CLT0001A.D_POL_EXPIR AS [Policy Expiration Date],
	CASE
		WHEN
			LEN(CLT0001A.D_RCV) = 26
		THEN
			CAST(
				SUBSTRING(CLT0001A.D_RCV,1,10)
				+ ' '
				+ REPLACE((SUBSTRING(CLT0001A.D_RCV,12,8)),'.',':')
				+ (SUBSTRING(CLT0001A.D_RCV,20,8))
				AS DATETIME2(0)
			)
		ELSE
			CAST(NULL AS DATETIME2(0))
	END AS [First Report Date],
	T_LOL_STR1 AS Address,
	NULL AS Suite,
	M_LOL_CITY AS City,
	C_LOL_ST_ALPH AS [Loss State],
	C_LOL_ZIP AS Zip,
	NULL AS LossAddr_Lat,
	NULL AS LossAddr_Long,
	@dateInserted AS dateInserted 
	INTO CDP.ClaimExtract
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	LEFT OUTER JOIN
	(
		SELECT
			COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP) AS policyTypeCode,
			COALESCE(Dim_Policy_Type.T_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP_DESC) AS policyTypeDescription,
			ROW_NUMBER() OVER(
				PARTITION BY
					COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP)
				ORDER BY
					COALESCE(Dim_Policy_Type.Date_Insert,'20000101') DESC
			) AS uniqueInstanceValue
		FROM
			ClaimSearch_Prod.dbo.Dim_Policy_Type WITH (NOLOCK)
			FULL OUTER JOIN ClaimSearch_Prod.dbo.Lookup_Pol_Type_Code WITH (NOLOCK)
				ON Dim_Policy_Type.C_POL_TYP = Lookup_Pol_Type_Code.C_POL_TYP
	) AS UniquePolicyTypeInstance
		ON CLT0001A.C_POL_TYP = UniquePolicyTypeInstance.policyTypeCode
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.Dim_Loss_Type
		ON CLT0001A.C_LOSS_TYP = Dim_Loss_Type.C_LOSS_TYP
WHERE
	UniquePolicyTypeInstance.uniqueInstanceValue = 1
--*/
--SELECT * FROM CDP.InvolvedPartyExtract

/************************	IP		************************/
--/*
SELECT --TOP 10

	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ClaimID],
	ReasonCodeOutput_Filtered_20190726.I_ALLCLM AS [ISO File Number],
	CLT0001A.N_CLM AS [Claim Number] /*Match Claim Number*/,
	ISNULL(LTRIM(RTRIM(CLT0001A.N_POL)),'NA') AS [Policy Number],
	CLT0001A.D_OCUR AS [Loss Date],
	CAST(Dim_Loss_Type.T_LOSS_TYP AS VARCHAR(42)) AS [Loss Type],
	NULLIF(LTRIM(RTRIM(UniquePolicyTypeInstance.policyTypeDescription)),'') AS [Policy Type],
	ReasonCodeOutput_Filtered_20190726.Date_Insert AS [Scored Date],
	ReasonCodeOutput_Filtered_20190726.final_score AS Score,
	
	FS_PA_Matching_Entity.I_NM_ADR AS [Involved Party Index],
	FS_PA_Matching_Entity.C_ROLE AS [Role Code],
	Lookup_Rolecode.T_ROLE AS [Role],
	
	FS_PA_Matching_SSN.matched_SSN AS SSN,
	FS_PA_Matching_DL.matched_DL_number AS [Diver License],
	
	NULL AS [Involved Party ID],
	
	RTRIM(LTRIM(CASE
		WHEN
			NULLIF(LTRIM(RTRIM(FS_PA_Matching_Entity.name)),'') IS NULL
		THEN
			'DsRecNotFnd'
		WHEN
			FS_PA_Matching_Entity.name LIKE '%,%'
			--AND (LEN(FS_PA_Matching_Entity.name)) > (CHARINDEX(',',FS_PA_Matching_Entity.name))
		THEN
			SUBSTRING(
				FS_PA_Matching_Entity.name,
				1,
				CHARINDEX(',',FS_PA_Matching_Entity.name) -1
			)
		WHEN
			FS_PA_Matching_Entity.name LIKE '% %'
			--AND (LEN(FS_PA_Matching_Entity.name)) > (CHARINDEX(',',FS_PA_Matching_Entity.name))
		THEN
			SUBSTRING(
				FS_PA_Matching_Entity.name,
				1,
				CHARINDEX(' ',FS_PA_Matching_Entity.name) -1
			)
		WHEN
			LEN(LTRIM(RTRIM(FS_PA_Matching_Entity.name))) > 0
		THEN
			FS_PA_Matching_Entity.name
		ELSE
			'DsRecNotFnd'
	END)) AS MD_FirstName1,
	NULL AS MD_MiddleName1,
	LTRIM(RTRIM(CASE
		WHEN
			NULLIF(LTRIM(RTRIM(FS_PA_Matching_Entity.name)),'') IS NULL
		THEN
			'DsRecNotFnd'
		WHEN
			FS_PA_Matching_Entity.name LIKE '%,%'
			AND (LEN(FS_PA_Matching_Entity.name)) > (CHARINDEX(',',FS_PA_Matching_Entity.name))
		THEN
			SUBSTRING(
				FS_PA_Matching_Entity.name,
				(CHARINDEX(',',FS_PA_Matching_Entity.name) + 1),
				(LEN(FS_PA_Matching_Entity.name) - CHARINDEX(',',FS_PA_Matching_Entity.name))
			)
		WHEN
			RTRIM(FS_PA_Matching_Entity.name) LIKE '% %'
		THEN
			SUBSTRING(
				FS_PA_Matching_Entity.name,
				(CHARINDEX(' ',FS_PA_Matching_Entity.name) + 1),
				(LEN(FS_PA_Matching_Entity.name) - CHARINDEX(' ',FS_PA_Matching_Entity.name))
			)
		ELSE
			'DsRecNotFnd'
	END)) AS MD_LastName1,
	CLT00004.T_ADR_LN1 Address,
	NULL AS Suite,
	CLT00004.M_CITY AS City,
	CLT00004.C_ST_ALPH AS State,
	CLT00004.C_ZIP AS Zip,
	NULL AS ResidAddr_Lat,
	NULL AS ResidAddr_Long,
	@dateInserted AS dateInserted 
	INTO CDP.InvolvedPartyExtract
FROM
	CDP.ReasonCodeOutput_Filtered_20190726
	INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT0001A.I_ALLCLM
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_Matching_Entity
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = FS_PA_Matching_Entity.I_ALLCLM
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.Lookup_Rolecode
		ON Lookup_Rolecode.C_ROLE = FS_PA_Matching_Entity.C_ROLE
	LEFT OUTER JOIN CDP.FS_PA_Matching_DL_UAT AS FS_PA_Matching_DL
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = FS_PA_Matching_DL.I_ALLCLM
	LEFT OUTER JOIN CDP.FS_PA_Matching_SSN_UAT AS FS_PA_Matching_SSN
		ON ReasonCodeOutput_Filtered_20190726.I_ALLCLM = FS_PA_Matching_SSN.I_ALLCLM
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.Dim_Loss_Type
		ON CLT0001A.C_LOSS_TYP = Dim_Loss_Type.C_LOSS_TYP
	LEFT OUTER JOIN
	(
		SELECT
			COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP) AS policyTypeCode,
			COALESCE(Dim_Policy_Type.T_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP_DESC) AS policyTypeDescription,
			ROW_NUMBER() OVER(
				PARTITION BY
					COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP)
				ORDER BY
					COALESCE(Dim_Policy_Type.Date_Insert,'20000101') DESC
			) AS uniqueInstanceValue
		FROM
			ClaimSearch_Prod.dbo.Dim_Policy_Type WITH (NOLOCK)
			FULL OUTER JOIN ClaimSearch_Prod.dbo.Lookup_Pol_Type_Code WITH (NOLOCK)
				ON Dim_Policy_Type.C_POL_TYP = Lookup_Pol_Type_Code.C_POL_TYP
	) AS UniquePolicyTypeInstance
		ON CLT0001A.C_POL_TYP = UniquePolicyTypeInstance.policyTypeCode

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

SELECT @@TRANCOUNT