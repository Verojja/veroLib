SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for data-refresh of the InvolvedParty Table.
				Also inserts new associations into the IPACRMap.

			Performance:
				Use of TempTable index (adds 6 seconds, but significantly reduces runtime of subsequent steps
				Use of WindowfunctionPartition for Duplicate removal (35% performance gain over Distinct).
				Explore LOCK_ESCALATION at the partition level 
					set the LOCK_ESCALATION option of the ALTER TABLE statement to AUTO.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertInvolvedParty
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.InvolvedPartyActivityLog
				WHERE
					InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND InvolvedPartyActivityLog.isSuccessful = 1
					AND InvolvedPartyActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
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
				@stepExecutionNotes VARCHAR(1000);

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
				@stepId = 1,
				@stepDescription = 'CreateSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			/*NOTE: potentially update this step if/when IP Claim-Role changes source*/	
			SELECT
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyId,
				DuplicateDataSetPerformanceHackMelissaNameMap.isBusiness,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyRoleCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.hICNObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberLast3,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseClass,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseState,
				DuplicateDataSetPerformanceHackMelissaNameMap.genderCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.passportID,
				DuplicateDataSetPerformanceHackMelissaNameMap.professionalMedicalLicense,
				DuplicateDataSetPerformanceHackMelissaNameMap.isUnderSiuInvestigation,
				DuplicateDataSetPerformanceHackMelissaNameMap.isLawEnforcementAction,
				DuplicateDataSetPerformanceHackMelissaNameMap.isReportedToFraudBureau,
				DuplicateDataSetPerformanceHackMelissaNameMap.isFraudReported,
				DuplicateDataSetPerformanceHackMelissaNameMap.dateOfBirth,
				DuplicateDataSetPerformanceHackMelissaNameMap.fullName,
				DuplicateDataSetPerformanceHackMelissaNameMap.firstName,
				DuplicateDataSetPerformanceHackMelissaNameMap.middleName,
				DuplicateDataSetPerformanceHackMelissaNameMap.lastName,
				DuplicateDataSetPerformanceHackMelissaNameMap.suffix,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartySequenceId
				INTO #ScrubbedNameData
			FROM
				(
					SELECT
						ExistingInvolvedParty.involvedPartyId,
						NULLIF(CAST(LTRIM(RTRIM(CLT00004.I_ALLCLM)) AS VARCHAR(11)),'') AS isoClaimId,
						CAST(CLT00004.I_NM_ADR AS INT) AS involvedPartySequenceId,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00004.I_ALLCLM,
								CLT00004.I_NM_ADR
							ORDER BY
								CLT00004.Date_Insert DESC
						) AS uniqueInstanceValue,
						CASE
							WHEN
								CLT00004.C_NM_TYP = 'B'
							THEN
								CAST(1 AS BIT)
							ELSE
								CAST(0 AS BIT)
						END AS isBusiness,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_ROLE)),'') AS VARCHAR(2)) AS involvedPartyRoleCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00035.N_TIN)),'') AS VARCHAR(36)) AS taxIdentificationNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0035A.TIN_4)),'') AS CHAR(4)) AS taxIdentificationNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00007.N_SSN)),'') AS VARCHAR(36)) AS socialSecurityNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0007A.SSN_4)),'') AS VARCHAR(4)) AS socialSecurityNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_HICN_MEDCR)),'') AS VARCHAR(36)) AS hICNObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.N_DRV_LIC)),'') AS VARCHAR(52)) AS driversLicenseNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.N_DRV_LIC)),'') AS VARCHAR(3)) AS driversLicenseNumberLast3,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_ST_ALPH)),'') AS VARCHAR(2)) AS driversLicenseState,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_GEND)),'') AS CHAR(2)) AS genderCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.N_PSPRT)),'') AS VARCHAR(9)) passportID,
						CAST(NULLIF(LTRIM(RTRIM(CLT00010.N_PROF_MED_LIC)),'') AS VARCHAR(20)) professionalMedicalLicense,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isUnderSiuInvestigation,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isLawEnforcementAction,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isReportedToFraudBureau,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)*/
							ELSE
								CAST(0 AS BIT)
						END AS isFraudReported,
						CLT00004.D_BRTH AS dateOfBirth,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.M_FUL_NM)),'') AS VARCHAR(70)) AS fullName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_FirstName1*/NULL)),'') AS VARCHAR(100)) AS firstName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_MiddleName1*/NULL)),'') AS VARCHAR(100)) AS middleName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_LastName1*/NULL)),'') AS VARCHAR(100)) AS lastName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_Suffix1*/NULL)),'') AS VARCHAR(50)) suffix,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[B])-6,
								7,
								''
							),
						3) AS businessArea,
						RIGHT(CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),7) AS businessTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[C])-6,
								7,
								''
							),
						3) AS cellArea,
						RIGHT(CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),7) AS cellTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[F])-6,
								7,
								''
							),
						3) AS faxArea,
						RIGHT(CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),7) AS faxTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[H])-6,
								7,
								''
							),
						3) AS homeArea,
						RIGHT(CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),7) AS homeTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[P])-6,
								7,
								''
							),
						3) AS pagerArea,
						RIGHT(CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),7) AS pagerTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[*])-6,
								7,
								''
							),
						3) AS otherArea,
						RIGHT(CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),7) AS otherTel
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
						/*DEVNOTE: Deprecated; not using scrubed value at present and significant performance hit*//*
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
							ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
							ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey*/
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0007A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0007A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0007A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0008A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0008A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0008A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0035A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0035A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0035A.I_NM_ADR
						LEFT OUTER JOIN (
							SELECT
								CLT00009.I_ALLCLM,
								CLT00009.I_NM_ADR,
								CAST(
									CAST(
										RIGHT('00000' + CAST(CLT00009.N_AREA AS VARCHAR(3)),3)
										+ RIGHT('00000' + CAST(CLT00009.N_TEL AS VARCHAR(7)),7)
									AS CHAR(10))
								 AS BIGINT) AS phoneNumberValue,
								ISNULL(CLT00009.C_TEL_TYP, '*') AS phoneNumberType
							FROM
								dbo.FireMarshalDriver AS INNERFM_ExtractFile WITH (NOLOCK)
								INNER JOIN ClaimSearch_Prod.dbo.CLT00009 WITH (NOLOCK)
									ON INNERFM_ExtractFile.isoClaimId = CLT00009.I_ALLCLM
							) PhoneNumbers PIVOT
							(
								SUM(phoneNumberValue)
								FOR phoneNumberType IN
								(
									[B],
									[C],
									[F],
									[H],
									[P],
									[*]
								)
						) AS PhoneNumbersPivoted
							ON CLT00004.I_ALLCLM = PhoneNumbersPivoted.I_ALLCLM
								AND CLT00004.I_NM_ADR = PhoneNumbersPivoted.I_NM_ADR
						LEFT OUTER JOIN dbo.InvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
								AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
					WHERE
						/*Deprecating due to performance costs, and current profile state. RDW 20190306:
							NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
							AND CLT00004.I_NM_ADR IS NOT NULL
						*/
						CLT00004.Date_Insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
				) AS DuplicateDataSetPerformanceHackMelissaNameMap
			WHERE
				DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;

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
				@stepDescription = 'AddIndexToSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			CREATE UNIQUE CLUSTERED INDEX PK_ScrubbedNameData_isoClaimId_IPSequenceId
				ON #ScrubbedNameData (isoClaimId, involvedPartySequenceId);
			CREATE NONCLUSTERED INDEX NIX_ScrubbedNameData_involvedPartyId
				ON #ScrubbedNameData (involvedPartyId);
				
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
				@stepDescription = 'CaptureNonAliasedInvolvedPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				/*NULL AS isAliasOfInvolvedPartyId,*/
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMNonAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
			
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
				@stepDescription = 'UpdateFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
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
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
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
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
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
				@stepId = 5,
				@stepDescription = 'InsertNewFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
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
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
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
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
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
				@stepId = 6,
				@stepDescription = 'CaptureAliasedInvolvedPartyToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				#ScrubbedNameData.involvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
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
				@stepDescription = 'UpdateFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
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
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
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
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
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
				@stepId = 8,
				@stepDescription = 'InsertNewFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
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
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
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
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
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
				@stepId = 9,
				@stepDescription = 'CaptureNonAliasedServiceProviderDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				/*NULL isAliasOfInvolvedPartyId,*/
				COALESCE(ServiceProviderInvolvedParty.involvedPartyId, DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId) AS isServiceProviderOfInvolvedPartyId, /*COALESECE to protect validity of IP*/
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMNonAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty WITH (NOLOCK)
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
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
				@stepId = 10,
				@stepDescription = 'UpdateFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
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
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
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
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
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
				@stepId = 11,
				@stepDescription = 'InsertNewFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
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
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
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
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
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
				@stepId = 12,
				@stepDescription = 'CaptureFMAliasedServiceProviderPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				ServiceProviderInvolvedParty.involvedPartyId AS isServiceProviderOfInvolvedPartyId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel,
				/*isActive,*/
				/*dateInserted,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId
				INTO #FMAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
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
				@stepId = 13,
				@stepDescription = 'UpdateFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					/*InvolvedParty.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,*/
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
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive = SOURCE.isActive,*/
					InvolvedParty.dateInserted = @dateInserted
					/*InvolvedParty.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty
					ON SOURCE.involvedPartyId = InvolvedParty.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					/*OR ISNULL(InvolvedParty.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')*/
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
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
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
				@stepId = 14,
				@stepDescription = 'InsertNewFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				/*involvedPartyRoleCode,*/
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
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isBusiness,
				/*SOURCE.involvedPartyRoleCode,*/
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
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				1 AS isActive,
				@dateInserted AS dateInserted,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId
			FROM
				#FMAliasedServiceProviderData AS SOURCE
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
				@stepId = 15,
				@stepDescription = 'CreateIPAMDataTempTable',
				@stepStartDateTime = GETDATE();

			SELECT
				InvolvedParty.involvedPartyId,
				V_ActiveClaim.claimId,
				V_ActiveNonLocationOfLoss.addressId AS nonLocationOfLossAddressId,
				ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK') AS claimRoleCode,
				1 AS isActive,
				/*@dateInserted AS dateInserted,*/
				InvolvedParty.isoClaimId,
				InvolvedParty.involvedPartySequenceId,
				InvolvedPartyAddressMap.claimId as existingIPAMClaimId
				INTO #IPAMData
			FROM
				#ScrubbedNameData
				INNER JOIN dbo.V_ActiveClaim
					ON #ScrubbedNameData.isoClaimId = V_ActiveClaim.isoClaimId
				INNER JOIN dbo.InvolvedParty
					ON #ScrubbedNameData.isoClaimId = InvolvedParty.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedParty.involvedPartySequenceId
				INNER JOIN dbo.V_ActiveNonLocationOfLoss
					ON #ScrubbedNameData.isoClaimId = V_ActiveNonLocationOfLoss.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = V_ActiveNonLocationOfLoss.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedPartyAddressMap
					ON #ScrubbedNameData.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId

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
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@stepId = 16,
				@stepDescription = 'UpdateIPAMData',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.InvolvedPartyAddressMap
				SET
					InvolvedPartyAddressMap.involvedPartyId = SOURCE.involvedPartyId,
					InvolvedPartyAddressMap.claimId = SOURCE.claimId,
					InvolvedPartyAddressMap.nonLocationOfLossAddressId = SOURCE.nonLocationOfLossAddressId,
					InvolvedPartyAddressMap.claimRoleCode = SOURCE.claimRoleCode,
					/*InvolvedPartyAddressMap.isActive = SOURCE.isActive,*/
					InvolvedPartyAddressMap.dateInserted = @dateInserted
					/*isoClaimId*/
					/*involvedPartySequenceId*/
			FROM
				#IPAMData AS SOURCE	
				INNER JOIN dbo.InvolvedPartyAddressMap
					ON SOURCE.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND SOURCE.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId
			WHERE
				SOURCE.existingIPAMClaimId IS NOT NULL
				AND
				(
					InvolvedPartyAddressMap.involvedPartyId <> SOURCE.involvedPartyId
					OR InvolvedPartyAddressMap.claimId <> SOURCE.claimId
					OR InvolvedPartyAddressMap.nonLocationOfLossAddressId <> SOURCE.nonLocationOfLossAddressId
					OR InvolvedPartyAddressMap.claimRoleCode <> SOURCE.claimRoleCode
					/*OR InvolvedPartyAddressMap.isActive <> SOURCE.isActive
					OR InvolvedPartyAddressMap.dateInserted <> SOURCE.dateInserted
					OR InvolvedPartyAddressMap.isoClaimId <> SOURCE.isoClaimId
					OR InvolvedPartyAddressMap.involvedPartySequenceId <> SOURCE.involvedPartySequenceId*/
				);
				
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
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@stepId = 100,
				@stepDescription = 'InsertNewIPAddressMapping(s)',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedPartyAddressMap
			(
				involvedPartyId,
				claimId,
				nonLocationOfLossAddressId,
				claimRoleCode,
				isActive,
				dateInserted,
				isoClaimId,
				involvedPartySequenceId
			)
			SELECT
				InvolvedParty.involvedPartyId,
				V_ActiveClaim.claimId,
				V_ActiveNonLocationOfLoss.addressId,
				ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK'),
				1 AS isActive,
				@dateInserted,
				InvolvedParty.isoClaimId,
				InvolvedParty.involvedPartySequenceId
			FROM
				#ScrubbedNameData
				INNER JOIN dbo.V_ActiveClaim
					ON #ScrubbedNameData.isoClaimId = V_ActiveClaim.isoClaimId
				INNER JOIN dbo.InvolvedParty
					ON #ScrubbedNameData.isoClaimId = InvolvedParty.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedParty.involvedPartySequenceId
				INNER JOIN dbo.V_ActiveNonLocationOfLoss
					ON #ScrubbedNameData.isoClaimId = V_ActiveNonLocationOfLoss.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = V_ActiveNonLocationOfLoss.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedPartyAddressMap
					ON #ScrubbedNameData.isoClaimId = InvolvedPartyAddressMap.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedPartyAddressMap.involvedPartySequenceId
			WHERE
				InvolvedPartyAddressMap.isoCLaimId IS NULL;
				
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
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		PRINT'in catch';
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			PRINT'ROLLBACK in catch, int tran count: ' + CAST(@internalTransactionCount AS VARCHAR(100));
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();
		PRINT'pre log activity';

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
		PRINT'post log activity';
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
		PRINT'in raiseError Ifblock (int-tran-count=0)';
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertInvolvedParty; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

--PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*

*/