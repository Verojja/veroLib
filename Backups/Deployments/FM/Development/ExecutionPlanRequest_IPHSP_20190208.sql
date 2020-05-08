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
			CAST('2013-08-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.InvolvedPartyActivityLog
WHERE
	InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND InvolvedPartyActivityLog.isSuccessful = 1;
SET @sourceDateTime = @dateFilterParam;

/*Set Logging Variables for Current Step_Start*/
SELECT
	@stepId = 1,
	@stepDescription = 'CaptureNonAliasedInvolvedPartyDataToImport',
	@stepStartDateTime = GETDATE();
	
SELECT
	ExistingInvolvedParty.involvedPartyId,
	/*Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,*/
	/*ServicesProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,*/
	CASE
		WHEN
			CS_Lookup_EntityIDs.SubjectType = 'B'
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
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
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
	/*isActive,*/
	/*dateInserted,*/
	CLT00004.I_ALLCLM AS isoClaimId,
	CLT00004.I_NM_ADR AS involvedPartySequenceId
	INTO #FMNonAliasedInvolvedPartyData
FROM
	dbo.FM_ExtractFile WITH (NOLOCK)
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON FM_ExtractFile.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN dbo.V_ActiveFMNonAliasedInvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
			AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = Aliases.I_ALLCLM
			AND CLT00004.I_NM_ADR = Aliases.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ServicesProviders.I_ALLCLM
			AND CLT00004.I_NM_ADR = ServicesProviders.I_NM_ADR
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
		ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
		ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010
		ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007
		ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008
		ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035
		ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
WHERE
	NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
	AND CLT00004.I_NM_ADR IS NOT NULL
	AND Aliases.I_NM_ADR_AKA IS NULL
	AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NULL
	AND CAST(
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
	) >= @dateFilterParam;
	
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
	@stepId = 2,
	@stepDescription = 'UpdateFMNonAliasedInvolvedPartyData',
	@stepStartDateTime = GETDATE();

UPDATE dbo.InvolvedParty WITH (TABLOCKX)
	SET
		InvolvedParty.isAliasOfInvolvedPartyId = NULL,
		InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
		InvolvedParty.isBusiness = SOURCE.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
		InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
		InvolvedParty.genderCode = SOURCE.genderCode,
		InvolvedParty.passportID = SOURCE.passportID,
		InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
		InvolvedParty.isFraudReported = SOURCE.isFraudReported,
		InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
		InvolvedParty.fullName = SOURCE.fullName,
		InvolvedParty.firstName = SOURCE.firstName,
		InvolvedParty.middleName = SOURCE.middleName,
		InvolvedParty.lastName = SOURCE.lastName,
		InvolvedParty.suffix = SOURCE.suffix,
		/*InvolvedParty.isActive = SOURCE.isActive,*/
		InvolvedParty.dateInserted = @dateInserted
		/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
		InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
FROM
	#FMNonAliasedInvolvedPartyData AS SOURCE
	INNER JOIN V_ActiveFMNonAliasedInvolvedParty ON
		SOURCE.involvedPartyId = V_ActiveFMNonAliasedInvolvedParty.involvedPartyId
WHERE
	SOURCE.involvedPartyId IS NOT NULL
	AND SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
	AND 
	(
		/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
		ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
		OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
		OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
		OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
		OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
		OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
		OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
		OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
		OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
		OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
		OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
		OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
		OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
		OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
		OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
		OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
		OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
		OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
		OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
		/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
		OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
		OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
		OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
	);
--OPTION (RECOMPILE);

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
	@stepId = 3,
	@stepDescription = 'InsertNewFMNonAliasedInvolvedPartyData',
	@stepStartDateTime = GETDATE();

INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
(
	/*involvedPartyId,*/
	isAliasOfInvolvedPartyId,
	isServiceProviderOfInvolvedPartyId,
	isBusiness,
	taxIdentificationNumberObfuscated,
	taxIdentificationNumberLastFour,
	socialSecurityNumberObfuscated,
	socialSecurityNumberLastFour,
	hICNObfuscated,
	driversLicenseNumberObfuscated,
	driversLicenseNumberLast3,
	driversLicenseClass,
	driversLicenseState,
	genderCode,
	passportID,
	professionalMedicalLicense,
	isUnderSiuInvestigation,
	isLawEnforcementAction,
	isReportedToFraudBureau,
	isFraudReported,
	dateOfBirth,
	fullName,
	firstName,
	middleName,
	lastName,
	suffix,
	isActive,
	dateInserted,
	isoClaimId,
	involvedPartySequenceId
)
SELECT
	NULL AS isAliasOfInvolvedPartyId,
	NULL AS isServiceProviderOfInvolvedPartyId,
	SOURCE.isBusiness,
	SOURCE.taxIdentificationNumberObfuscated,
	SOURCE.taxIdentificationNumberLastFour,
	SOURCE.socialSecurityNumberObfuscated,
	SOURCE.socialSecurityNumberLastFour,
	SOURCE.hICNObfuscated,
	SOURCE.driversLicenseNumberObfuscated,
	SOURCE.driversLicenseNumberLast3,
	SOURCE.driversLicenseClass,
	SOURCE.driversLicenseState,
	SOURCE.genderCode,
	SOURCE.passportID,
	SOURCE.professionalMedicalLicense,
	SOURCE.isUnderSiuInvestigation,
	SOURCE.isLawEnforcementAction,
	SOURCE.isReportedToFraudBureau,
	SOURCE.isFraudReported,
	SOURCE.dateOfBirth,
	SOURCE.fullName,
	SOURCE.firstName,
	SOURCE.middleName,
	SOURCE.lastName,
	SOURCE.suffix,
	1 AS isActive,
	@dateInserted AS dateInserted,
	SOURCE.isoClaimId,
	SOURCE.involvedPartySequenceId
FROM
	#FMNonAliasedInvolvedPartyData AS SOURCE
WHERE
	SOURCE.involvedPartyId IS NULL;
--OPTION (RECOMPILE);

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
	@stepId = 4,
	@stepDescription = 'CaptureAliasedInvolvedPartyToImport',
	@stepStartDateTime = GETDATE();

SELECT
	ExistingInvolvedParty.involvedPartyId,
	Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,
	/*ServicesProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,*/
	CASE
		WHEN
			CS_Lookup_EntityIDs.SubjectType = 'B'
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
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
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
	/*isActive,*/
	/*dateInserted,*/
	CLT00004.I_ALLCLM AS isoClaimId,
	CLT00004.I_NM_ADR AS involvedPartySequenceId
	INTO #FMAliasedInvolvedPartyData
FROM
	dbo.FM_ExtractFile WITH (NOLOCK)
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON FM_ExtractFile.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN dbo.V_ActiveFMNonAliasedInvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
			AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = Aliases.I_ALLCLM
			AND CLT00004.I_NM_ADR = Aliases.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ServicesProviders.I_ALLCLM
			AND CLT00004.I_NM_ADR = ServicesProviders.I_NM_ADR
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
		ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
		ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010
		ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007
		ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008
		ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035
		ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
WHERE
	NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
	AND CLT00004.I_NM_ADR IS NOT NULL
	AND Aliases.I_NM_ADR_AKA IS NOT NULL
	AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NULL
	AND CAST(
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
	) >= @dateFilterParam;
	
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
	@stepId = 5,
	@stepDescription = 'UpdateFMAliasedInvolvedPartyData',
	@stepStartDateTime = GETDATE();

UPDATE dbo.InvolvedParty WITH (TABLOCKX)
	SET
		InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
		InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
		InvolvedParty.isBusiness = SOURCE.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
		InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
		InvolvedParty.genderCode = SOURCE.genderCode,
		InvolvedParty.passportID = SOURCE.passportID,
		InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
		InvolvedParty.isFraudReported = SOURCE.isFraudReported,
		InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
		InvolvedParty.fullName = SOURCE.fullName,
		InvolvedParty.firstName = SOURCE.firstName,
		InvolvedParty.middleName = SOURCE.middleName,
		InvolvedParty.lastName = SOURCE.lastName,
		InvolvedParty.suffix = SOURCE.suffix,
		/*InvolvedParty.isActive = SOURCE.isActive,*/
		InvolvedParty.dateInserted = @dateInserted
		/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
		InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
FROM
	#FMAliasedInvolvedPartyData AS SOURCE
	INNER JOIN dbo.V_ActiveFMAliasedInvolvedParty
		ON SOURCE.involvedPartyId = V_ActiveFMAliasedInvolvedParty.involvedPartyId
WHERE
	SOURCE.involvedPartyId IS NOT NULL
	AND SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
	AND 
	(
		/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
		ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
		OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
		OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
		OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
		OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
		OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
		OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
		OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
		OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
		OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
		OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
		OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
		OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
		OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
		OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
		OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
		OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
		OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
		OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
		/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
		OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
		OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
		OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
	);
--OPTION (RECOMPILE);

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
	@stepId = 6,
	@stepDescription = 'InsertNewFMAliasedInvolvedPartyData',
	@stepStartDateTime = GETDATE();

INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
(
	/*involvedPartyId,*/
	isAliasOfInvolvedPartyId,
	isServiceProviderOfInvolvedPartyId,
	isBusiness,
	taxIdentificationNumberObfuscated,
	taxIdentificationNumberLastFour,
	socialSecurityNumberObfuscated,
	socialSecurityNumberLastFour,
	hICNObfuscated,
	driversLicenseNumberObfuscated,
	driversLicenseNumberLast3,
	driversLicenseClass,
	driversLicenseState,
	genderCode,
	passportID,
	professionalMedicalLicense,
	isUnderSiuInvestigation,
	isLawEnforcementAction,
	isReportedToFraudBureau,
	isFraudReported,
	dateOfBirth,
	fullName,
	firstName,
	middleName,
	lastName,
	suffix,
	isActive,
	dateInserted,
	isoClaimId,
	involvedPartySequenceId
)
SELECT
	SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
	NULL AS isServiceProviderOfInvolvedPartyId,
	SOURCE.isBusiness,
	SOURCE.taxIdentificationNumberObfuscated,
	SOURCE.taxIdentificationNumberLastFour,
	SOURCE.socialSecurityNumberObfuscated,
	SOURCE.socialSecurityNumberLastFour,
	SOURCE.hICNObfuscated,
	SOURCE.driversLicenseNumberObfuscated,
	SOURCE.driversLicenseNumberLast3,
	SOURCE.driversLicenseClass,
	SOURCE.driversLicenseState,
	SOURCE.genderCode,
	SOURCE.passportID,
	SOURCE.professionalMedicalLicense,
	SOURCE.isUnderSiuInvestigation,
	SOURCE.isLawEnforcementAction,
	SOURCE.isReportedToFraudBureau,
	SOURCE.isFraudReported,
	SOURCE.dateOfBirth,
	SOURCE.fullName,
	SOURCE.firstName,
	SOURCE.middleName,
	SOURCE.lastName,
	SOURCE.suffix,
	1 AS isActive,
	@dateInserted AS dateInserted,
	SOURCE.isoClaimId,
	SOURCE.involvedPartySequenceId
FROM
	#FMAliasedInvolvedPartyData AS SOURCE
WHERE
	SOURCE.involvedPartyId IS NULL;
--OPTION (RECOMPILE);

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
	@stepId = 7,
	@stepDescription = 'CaptureNonAliasedServiceProviderDataToImport',
	@stepStartDateTime = GETDATE();
	
SELECT
	ExistingInvolvedParty.involvedPartyId,
	/*Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,*/
	ServicesProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,
	CASE
		WHEN
			CS_Lookup_EntityIDs.SubjectType = 'B'
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
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
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
	/*isActive,*/
	/*dateInserted,*/
	CLT00004.I_ALLCLM AS isoClaimId,
	CLT00004.I_NM_ADR AS involvedPartySequenceId
	INTO #FMNonAliasedServiceProviderData
FROM
	dbo.FM_ExtractFile WITH (NOLOCK)
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON FM_ExtractFile.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN dbo.V_ActiveFMNonAliasedInvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
			AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = Aliases.I_ALLCLM
			AND CLT00004.I_NM_ADR = Aliases.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ServicesProviders.I_ALLCLM
			AND CLT00004.I_NM_ADR = ServicesProviders.I_NM_ADR
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
		ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
		ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010
		ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007
		ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008
		ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035
		ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
WHERE
	NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
	AND CLT00004.I_NM_ADR IS NOT NULL
	AND Aliases.I_NM_ADR_AKA IS NULL
	AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NOT NULL
	AND CAST(
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
	) >= @dateFilterParam;
	
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
	@stepId = 8,
	@stepDescription = 'UpdateFMNonAliasedServiceProviderData',
	@stepStartDateTime = GETDATE();

UPDATE dbo.InvolvedParty WITH (TABLOCKX)
	SET
		/*InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,*/
		InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
		InvolvedParty.isBusiness = SOURCE.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
		InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
		InvolvedParty.genderCode = SOURCE.genderCode,
		InvolvedParty.passportID = SOURCE.passportID,
		InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
		InvolvedParty.isFraudReported = SOURCE.isFraudReported,
		InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
		InvolvedParty.fullName = SOURCE.fullName,
		InvolvedParty.firstName = SOURCE.firstName,
		InvolvedParty.middleName = SOURCE.middleName,
		InvolvedParty.lastName = SOURCE.lastName,
		InvolvedParty.suffix = SOURCE.suffix,
		/*InvolvedParty.isActive = SOURCE.isActive,*/
		InvolvedParty.dateInserted = @dateInserted
		/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
		InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
FROM
	#FMNonAliasedServiceProviderData AS SOURCE
	INNER JOIN dbo.V_ActiveFMNonAliasedServiceProvider
		ON SOURCE.involvedPartyId = V_ActiveFMNonAliasedServiceProvider.involvedPartyId
WHERE
	SOURCE.involvedPartyId IS NOT NULL
	AND SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
	AND 
	(
		/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
		/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
		OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
		OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
		OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
		OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
		OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
		OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
		OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
		OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
		OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
		OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
		OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
		OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
		OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
		OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
		OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
		OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
		OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
		OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
		/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
		OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
		OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
		OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
	);
--OPTION (RECOMPILE);

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
	@stepId = 9,
	@stepDescription = 'InsertNewFMNonAliasedServiceProviderData',
	@stepStartDateTime = GETDATE();

INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
(
	/*involvedPartyId,*/
	isAliasOfInvolvedPartyId,
	isServiceProviderOfInvolvedPartyId,
	isBusiness,
	taxIdentificationNumberObfuscated,
	taxIdentificationNumberLastFour,
	socialSecurityNumberObfuscated,
	socialSecurityNumberLastFour,
	hICNObfuscated,
	driversLicenseNumberObfuscated,
	driversLicenseNumberLast3,
	driversLicenseClass,
	driversLicenseState,
	genderCode,
	passportID,
	professionalMedicalLicense,
	isUnderSiuInvestigation,
	isLawEnforcementAction,
	isReportedToFraudBureau,
	isFraudReported,
	dateOfBirth,
	fullName,
	firstName,
	middleName,
	lastName,
	suffix,
	isActive,
	dateInserted,
	isoClaimId,
	involvedPartySequenceId
)
SELECT
	NULL AS isAliasOfInvolvedPartyId,
	SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
	SOURCE.isBusiness,
	SOURCE.taxIdentificationNumberObfuscated,
	SOURCE.taxIdentificationNumberLastFour,
	SOURCE.socialSecurityNumberObfuscated,
	SOURCE.socialSecurityNumberLastFour,
	SOURCE.hICNObfuscated,
	SOURCE.driversLicenseNumberObfuscated,
	SOURCE.driversLicenseNumberLast3,
	SOURCE.driversLicenseClass,
	SOURCE.driversLicenseState,
	SOURCE.genderCode,
	SOURCE.passportID,
	SOURCE.professionalMedicalLicense,
	SOURCE.isUnderSiuInvestigation,
	SOURCE.isLawEnforcementAction,
	SOURCE.isReportedToFraudBureau,
	SOURCE.isFraudReported,
	SOURCE.dateOfBirth,
	SOURCE.fullName,
	SOURCE.firstName,
	SOURCE.middleName,
	SOURCE.lastName,
	SOURCE.suffix,
	1 AS isActive,
	@dateInserted AS dateInserted,
	SOURCE.isoClaimId,
	SOURCE.involvedPartySequenceId
FROM
	#FMNonAliasedServiceProviderData AS SOURCE
WHERE
	SOURCE.involvedPartyId IS NULL;
--OPTION (RECOMPILE);

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
	@stepId = 10,
	@stepDescription = 'CaptureFMAliasedServiceProviderPartyDataToImport',
	@stepStartDateTime = GETDATE();
	
SELECT
	ExistingInvolvedParty.involvedPartyId,
	Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,
	ServicesProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,
	CASE
		WHEN
			CS_Lookup_EntityIDs.SubjectType = 'B'
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
	CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
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
	/*isActive,*/
	/*dateInserted,*/
	CLT00004.I_ALLCLM AS isoClaimId,
	CLT00004.I_NM_ADR AS involvedPartySequenceId
	INTO #FMAliasedServiceProviderPartyData
FROM
	dbo.FM_ExtractFile WITH (NOLOCK)
	INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
		ON FM_ExtractFile.I_ALLCLM = CLT00004.I_ALLCLM
	LEFT OUTER JOIN dbo.V_ActiveFMNonAliasedInvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
			AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
		ON CLT00004.I_ALLCLM = CLT00006.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00006.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
		ON CLT00006.I_ALLCLM = CLT00021.I_ALLCLM
			AND CLT00006.I_NM_ADR = CLT00021.I_NM_ADR
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
		ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
	LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
		ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010
		ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007
		ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008
		ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
	LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035
		ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
			AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
WHERE
	NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
	AND CLT00004.I_NM_ADR IS NOT NULL
	AND Aliases.I_NM_ADR_AKA IS NOT NULL
	AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NOT NULL
	AND CAST(
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
	) >= @dateFilterParam;
	
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
	@stepId = 11,
	@stepDescription = 'UpdateFMAliasedServiceProviderData',
	@stepStartDateTime = GETDATE();

UPDATE dbo.InvolvedParty WITH (TABLOCKX)
	SET
		InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
		InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
		InvolvedParty.isBusiness = SOURCE.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
		InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
		InvolvedParty.genderCode = SOURCE.genderCode,
		InvolvedParty.passportID = SOURCE.passportID,
		InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
		InvolvedParty.isFraudReported = SOURCE.isFraudReported,
		InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
		InvolvedParty.fullName = SOURCE.fullName,
		InvolvedParty.firstName = SOURCE.firstName,
		InvolvedParty.middleName = SOURCE.middleName,
		InvolvedParty.lastName = SOURCE.lastName,
		InvolvedParty.suffix = SOURCE.suffix,
		/*InvolvedParty.isActive = SOURCE.isActive,*/
		InvolvedParty.dateInserted = @dateInserted
		/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
		InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
FROM
	#FMNonAliasedServiceProviderData AS SOURCE
	INNER JOIN dbo.V_ActiveFMAliasedServiceProvider
		ON SOURCE.involvedPartyId = V_ActiveFMAliasedServiceProvider.involvedPartyId
WHERE
	SOURCE.involvedPartyId IS NOT NULL
	AND SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
	AND 
	(
		/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
		/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
		OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
		OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
		OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
		OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
		OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
		OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
		OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
		OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
		OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
		OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
		OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
		OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
		OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
		OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
		OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
		OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
		OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
		OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
		OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
		OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
		OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
		/*OR ISNULL(InvolvedParty.isActive,'') <> ISNULL(SOURCE.isActive,'')
		OR ISNULL(InvolvedParty.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
		OR ISNULL(InvolvedParty.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
		OR ISNULL(InvolvedParty.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
	);
--OPTION (RECOMPILE);

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
	@stepId = 9,
	@stepDescription = 'InsertNewFMNonAliasedServiceProviderData',
	@stepStartDateTime = GETDATE();

INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
(
	/*involvedPartyId,*/
	isAliasOfInvolvedPartyId,
	isServiceProviderOfInvolvedPartyId,
	isBusiness,
	taxIdentificationNumberObfuscated,
	taxIdentificationNumberLastFour,
	socialSecurityNumberObfuscated,
	socialSecurityNumberLastFour,
	hICNObfuscated,
	driversLicenseNumberObfuscated,
	driversLicenseNumberLast3,
	driversLicenseClass,
	driversLicenseState,
	genderCode,
	passportID,
	professionalMedicalLicense,
	isUnderSiuInvestigation,
	isLawEnforcementAction,
	isReportedToFraudBureau,
	isFraudReported,
	dateOfBirth,
	fullName,
	firstName,
	middleName,
	lastName,
	suffix,
	isActive,
	dateInserted,
	isoClaimId,
	involvedPartySequenceId
)
SELECT
	SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
	SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
	SOURCE.isBusiness,
	SOURCE.taxIdentificationNumberObfuscated,
	SOURCE.taxIdentificationNumberLastFour,
	SOURCE.socialSecurityNumberObfuscated,
	SOURCE.socialSecurityNumberLastFour,
	SOURCE.hICNObfuscated,
	SOURCE.driversLicenseNumberObfuscated,
	SOURCE.driversLicenseNumberLast3,
	SOURCE.driversLicenseClass,
	SOURCE.driversLicenseState,
	SOURCE.genderCode,
	SOURCE.passportID,
	SOURCE.professionalMedicalLicense,
	SOURCE.isUnderSiuInvestigation,
	SOURCE.isLawEnforcementAction,
	SOURCE.isReportedToFraudBureau,
	SOURCE.isFraudReported,
	SOURCE.dateOfBirth,
	SOURCE.fullName,
	SOURCE.firstName,
	SOURCE.middleName,
	SOURCE.lastName,
	SOURCE.suffix,
	1 AS isActive,
	@dateInserted AS dateInserted,
	SOURCE.isoClaimId,
	SOURCE.involvedPartySequenceId
FROM
	#FMNonAliasedServiceProviderData AS SOURCE
WHERE
	SOURCE.involvedPartyId IS NULL;
--OPTION (RECOMPILE);

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
