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
	@stepExecutionNotes VARCHAR(1000),
	@dateFilterParam DATETIME2(0);
	
/*Set Logging Variables for execution*/
SELECT
	@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
	(
		COALESCE
		(
			@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
			MAX(InvolvedPartyActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
			CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.InvolvedPartyActivityLog WITH (NOLOCK)
WHERE
	InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND InvolvedPartyActivityLog.isSuccessful = 1;
SET @sourceDateTime = @dateFilterParam;

/*Set Logging Variables for Current Step_Start*/
SELECT
	@stepId = 200,
	@stepDescription = 'CreateSupportDataTempTable_PerformanceTest',
	@stepStartDateTime = GETDATE();
	
SELECT
	CASE
		WHEN
			DuplicateDataSetPerformanceHackMelissaNameMap.SubjectType = 'B'
		THEN
			1
		ELSE
			0
	END AS isBusiness,
	CAST(NULLIF(LTRIM(RTRIM(CLT00035.N_TIN)),'') AS VARCHAR(30)) AS taxIdentificationNumberObfuscated,
	RIGHT(CAST(NULLIF(LTRIM(RTRIM(CLT00035.N_TIN)),'') AS VARCHAR(30)),4) AS taxIdentificationNumberLastFour,
	CAST(NULLIF(LTRIM(RTRIM(CLT00007.N_SSN)),'') AS VARCHAR(30)) AS socialSecurityNumberObfuscated,
	RIGHT(CAST(NULLIF(LTRIM(RTRIM(CLT00007.N_SSN)),'') AS VARCHAR(30)),4) AS socialSecurityNumberLastFour,
	CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_HICN_MEDCR)),'') AS VARCHAR(36)) AS hICNObfuscated,
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.N_DRV_LIC)),'') AS VARCHAR(15)) AS driversLicenseNumberObfuscated,
	RIGHT(CAST(NULLIF(LTRIM(RTRIM(CLT00008.N_DRV_LIC)),'') AS VARCHAR(15)),3) AS driversLicenseNumberLast3,
	CAST(NULLIF(LTRIM(RTRIM(/*CLT00008.C_DRV_LIC_CLASS*/NULL)),'') AS VARCHAR(3)) AS driversLicenseClass,
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_ST_ALPH)),'') AS VARCHAR(2)) AS driversLicenseState,
	CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_GEND)),'') AS CHAR(2)) AS genderCode,
	CAST(NULLIF(LTRIM(RTRIM(CLT00004.N_PSPRT)),'') AS VARCHAR(9)) passportID,
	CAST(NULLIF(LTRIM(RTRIM(CLT00010.N_PROF_MED_LIC)),'') AS VARCHAR(20)) professionalMedicalLicense,
	CASE
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_SIU_INVST
					)
				),
				''
			) = 'Y'
		THEN
			CAST(1 AS BIT)
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_SIU_INVST
					)
				),
				''
			) = 'N'
		THEN
			CAST(0 AS BIT)
		ELSE
			CAST(NULL AS BIT)
	END AS isUnderSiuInvestigation,
	CASE
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_ENF_ACTN
					)
				),
				''
			) = 'Y'
		THEN
			CAST(1 AS BIT)
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_ENF_ACTN
					)
				),
				''
			) = 'N'
		THEN
			CAST(0 AS BIT)
		ELSE
			CAST(NULL AS BIT)
	END AS isLawEnforcementAction,
	CASE
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_FRAUD_BUR_RPT
					)
				),
				''
			) = 'Y'
		THEN
			CAST(1 AS BIT)
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_FRAUD_BUR_RPT
					)
				),
				''
			) = 'N'
		THEN
			CAST(0 AS BIT)
		ELSE
			CAST(NULL AS BIT)
	END AS isReportedToFraudBureau,
	CASE
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_FRAUD_OCUR
					)
				),
				''
			) = 'Y'
		THEN
			CAST(1 AS BIT)
		WHEN
			NULLIF(
				LTRIM(
					RTRIM(
						CLT00004.F_FRAUD_OCUR
					)
				),
				''
			) = 'N'
		THEN
			CAST(0 AS BIT)
		ELSE
			CAST(NULL AS BIT)
	END AS isFraudReported,
	CLT00004.D_BRTH AS dateOfBirth,
	CAST(NULLIF(LTRIM(RTRIM(CLT00004.M_FUL_NM)),'') AS VARCHAR(70)) AS fullName,
	CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Names_Melissa_Output.MD_FirstName1)),'') AS VARCHAR(100)) AS firstName,
	CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Names_Melissa_Output.MD_MiddleName1)),'') AS VARCHAR(100)) AS middleName,
	CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Names_Melissa_Output.MD_LastName1)),'') AS VARCHAR(100)) AS lastName,
	CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Names_Melissa_Output.MD_Suffix1)),'') AS VARCHAR(50)) suffix,
	CLT00004.I_ALLCLM AS isoClaimId,
	CLT00004.I_NM_ADR AS involvedPartySequenceId
	INTO #ScrubbedNameData
FROM
	dbo.FM_ExtractFile WITH (NOLOCK)
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON FM_ExtractFile.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN
	(
		SELECT
			CS_Lookup_EntityIDs.CLMNMROWID,
			CS_Lookup_EntityIDs.SubjectKey,
			ROW_NUMBER() OVER(
				PARTITION BY
					CS_Lookup_EntityIDs.CLMNMROWID
				ORDER BY
					CS_Lookup_EntityIDs.[YEAR] DESC
			) AS uniqueInstanceValue,
			CS_Lookup_EntityIDs.SubjectType
		FROM
			[ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)		
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
		ON CLT00004.CLMNMROWID = DuplicateDataSetPerformanceHackMelissaNameMap.CLMNMROWID
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
		ON DuplicateDataSetPerformanceHackMelissaNameMap.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007 WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008 WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010 WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035 WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1
	AND NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
	AND CLT00004.I_NM_ADR IS NOT NULL
	AND CASE
			WHEN
				CAST(
					ISNULL(
						NULLIF(
							LTRIM(
								RTRIM(
									CAST(
										CLT00004.Date_Insert
										AS CHAR(8)
									)
								)
							),
							''
						),
						'00010101'
					)
					AS DATE
				) >
				ISNULL(
					CS_Lookup_Unique_Names_Melissa_Output.Date_Insert,
					CAST('00010101' AS DATE)
				)
			THEN
				CAST(
					CAST(
						CLT00004.Date_Insert
						AS CHAR(8)
					)
					AS DATE
				)
			ELSE
				CS_Lookup_Unique_Names_Melissa_Output.Date_Insert
		END >= @dateFilterParam;

/*Set Logging Variables for Current Step_End_Success*/
SELECT
	@stepEndDateTime = GETDATE(),
	@recordsAffected = ROWCOUNT_BIG(),
	@isSuccessful = 1,
	@stepExecutionNotes = NULL;

/*Log Activity*/
INSERT INTO dbo.InvolvedPartyActivityLog
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
	@stepId = 201,
	@stepDescription = 'CreateIndexOnTempTable_PerformanceTest',
	@stepStartDateTime = GETDATE();
			
CREATE UNIQUE CLUSTERED INDEX PK_ScrubbedNameData_isoClaimId_involvedPartySequenceId
	ON #ScrubbedNameData (isoClaimId, involvedPartySequenceId)

/*Set Logging Variables for Current Step_End_Success*/
SELECT
	@stepEndDateTime = GETDATE(),
	@recordsAffected = ROWCOUNT_BIG(),
	@isSuccessful = 1,
	@stepExecutionNotes = NULL;

/*Log Activity*/
INSERT INTO dbo.InvolvedPartyActivityLog
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