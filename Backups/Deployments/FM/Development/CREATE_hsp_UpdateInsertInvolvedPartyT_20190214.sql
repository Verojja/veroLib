SET NOEXEC OFF;

/*
TODO:
*/

/*
Tables referenced:
	[ClaimSearch].dbo.CS_Lookup_Melissa_InvolvedParty_Mapping_to_CLT00001
	[ClaimSearch].dbo.CS_Lookup_Unique_InvolvedPartyes_Melissa_Output
	[ClaimSearch_Prod].dbo.CLT00001
*/
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ?????
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for data-refresh of the InvolvedParty Table.
			 
			 Note: It may be possible, prior to proper ER, for a quazi-duplicate
			  row to be created for IP that updates to SP or A that updates to NON-A.
			  First solution I can think of is to abandon IXVs; need to do performance
			   check against non IXV DML.
			Performance: 
				Explore LOCK_ESCALATION at the partition level 
					set the LOCK_ESCALATION option of the ALTER TABLE statement to AUTO.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertInvolvedPartyT
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		PRINT'IN TRY';
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			PRINT'TranCOUNT 0 making tran.';
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.InvolvedParty_TActivityLog
				WHERE
					InvolvedParty_TActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND InvolvedParty_TActivityLog.isSuccessful = 1
					AND DATEDIFF(HOUR,GETDATE(),InvolvedParty_TActivityLog.executionDateTime) < 12
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
						MAX(InvolvedParty_TActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.InvolvedParty_TActivityLog WITH (NOLOCK)
			WHERE
				InvolvedParty_TActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND InvolvedParty_TActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CreateSupportDataTempTable',
				@stepStartDateTime = GETDATE();
				
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
						ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
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
								ClaimSearch_Prod.dbo.CLT00009 WITH (NOLOCK)
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
						LEFT OUTER JOIN dbo.InvolvedParty_T AS ExistingInvolvedParty WITH (NOLOCK)
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
				/*Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,*/
				/*ServicesProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,*/
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
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServicesProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServicesProviders.I_NM_ADR
			WHERE
				Aliases.I_NM_ADR_AKA IS NULL
				AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NULL;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			UPDATE dbo.InvolvedParty_T WITH (TABLOCKX)
				SET
					InvolvedParty_T.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty_T.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty_T.isBusiness = SOURCE.isBusiness,
					InvolvedParty_T.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,
					InvolvedParty_T.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty_T.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty_T.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty_T.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty_T.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty_T.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty_T.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty_T.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty_T.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty_T.genderCode = SOURCE.genderCode,
					InvolvedParty_T.passportID = SOURCE.passportID,
					InvolvedParty_T.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty_T.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty_T.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty_T.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty_T.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty_T.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty_T.fullName = SOURCE.fullName,
					InvolvedParty_T.firstName = SOURCE.firstName,
					InvolvedParty_T.middleName = SOURCE.middleName,
					InvolvedParty_T.lastName = SOURCE.lastName,
					InvolvedParty_T.suffix = SOURCE.suffix,
					InvolvedParty_T.businessArea = SOURCE.businessArea,
					InvolvedParty_T.businessTel = SOURCE.businessTel,
					InvolvedParty_T.cellArea = SOURCE.cellArea,
					InvolvedParty_T.cellTel = SOURCE.cellTel,
					InvolvedParty_T.faxArea = SOURCE.faxArea,
					InvolvedParty_T.faxTel = SOURCE.faxTel,
					InvolvedParty_T.homeArea = SOURCE.homeArea,
					InvolvedParty_T.homeTel = SOURCE.homeTel,
					InvolvedParty_T.pagerArea = SOURCE.pagerArea,
					InvolvedParty_T.pagerTel = SOURCE.pagerTel,
					InvolvedParty_T.otherArea = SOURCE.otherArea,
					InvolvedParty_T.otherTel = SOURCE.otherTel,
					/*InvolvedParty_T.isActive = SOURCE.isActive,*/
					InvolvedParty_T.dateInserted = @dateInserted
					/*InvolvedParty_T.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty_T.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty_T
					ON SOURCE.involvedPartyId = InvolvedParty_T.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty_T.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty_T.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty_T.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty_T.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty_T.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty_T.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty_T.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty_T.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty_T.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty_T.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty_T.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty_T.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty_T.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty_T.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty_T.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty_T.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty_T.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty_T.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty_T.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty_T.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty_T.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty_T.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty_T.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty_T.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty_T.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty_T.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty_T.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty_T.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty_T.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty_T.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty_T.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty_T.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty_T.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty_T.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty_T.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			INSERT INTO dbo.InvolvedParty_T WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				involvedPartyRoleCode,
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
				SOURCE.involvedPartyRoleCode,
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
				/*COALESCE for forced FKViolation, in the instance of a Primary record not existing*/
				COALESCE(AliasInvolvedParty.involvedPartyId, Aliases.I_NM_ADR_AKA) AS isAliasOfInvolvedPartyId,
				/*ServiceProviders.I_NM_ADR_SVC_PRVD AS isServiceProviderOfInvolvedPartyId,*/
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
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServiceProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServiceProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServiceProviders.I_NM_ADR
				LEFT OUTER JOIN dbo.InvolvedParty_T AS AliasInvolvedParty
					ON Aliases.I_ALLCLM = AliasInvolvedParty.isoClaimId
						AND Aliases.I_NM_ADR_AKA = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				Aliases.I_NM_ADR_AKA IS NOT NULL
				AND ServiceProviders.I_NM_ADR_SVC_PRVD IS NULL;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			UPDATE dbo.InvolvedParty_T WITH (TABLOCKX)
				SET
					InvolvedParty_T.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty_T.isServiceProviderOfInvolvedPartyId = NULL,
					InvolvedParty_T.isBusiness = SOURCE.isBusiness,
					InvolvedParty_T.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,
					InvolvedParty_T.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty_T.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty_T.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty_T.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty_T.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty_T.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty_T.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty_T.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty_T.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty_T.genderCode = SOURCE.genderCode,
					InvolvedParty_T.passportID = SOURCE.passportID,
					InvolvedParty_T.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty_T.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty_T.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty_T.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty_T.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty_T.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty_T.fullName = SOURCE.fullName,
					InvolvedParty_T.firstName = SOURCE.firstName,
					InvolvedParty_T.middleName = SOURCE.middleName,
					InvolvedParty_T.lastName = SOURCE.lastName,
					InvolvedParty_T.suffix = SOURCE.suffix,
					InvolvedParty_T.businessArea = SOURCE.businessArea,
					InvolvedParty_T.businessTel = SOURCE.businessTel,
					InvolvedParty_T.cellArea = SOURCE.cellArea,
					InvolvedParty_T.cellTel = SOURCE.cellTel,
					InvolvedParty_T.faxArea = SOURCE.faxArea,
					InvolvedParty_T.faxTel = SOURCE.faxTel,
					InvolvedParty_T.homeArea = SOURCE.homeArea,
					InvolvedParty_T.homeTel = SOURCE.homeTel,
					InvolvedParty_T.pagerArea = SOURCE.pagerArea,
					InvolvedParty_T.pagerTel = SOURCE.pagerTel,
					InvolvedParty_T.otherArea = SOURCE.otherArea,
					InvolvedParty_T.otherTel = SOURCE.otherTel,
					/*InvolvedParty_T.isActive = SOURCE.isActive,*/
					InvolvedParty_T.dateInserted = @dateInserted
					/*InvolvedParty_T.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty_T.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
				INNER JOIN dbo.InvolvedParty_T
					ON SOURCE.involvedPartyId = InvolvedParty_T.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty_T.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					ISNULL(InvolvedParty_T.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty_T.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty_T.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty_T.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty_T.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty_T.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty_T.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty_T.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty_T.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty_T.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty_T.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty_T.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty_T.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty_T.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty_T.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty_T.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty_T.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty_T.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty_T.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty_T.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty_T.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty_T.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty_T.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty_T.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty_T.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty_T.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty_T.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty_T.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty_T.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty_T.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty_T.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty_T.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty_T.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty_T.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			INSERT INTO dbo.InvolvedParty_T WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				involvedPartyRoleCode,
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
				SOURCE.involvedPartyRoleCode,
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
				/*Aliases.I_NM_ADR_AKA AS isAliasOfInvolvedPartyId,*/
				COALESCE(ServiceProviderInvolvedParty.involvedPartyId, ServiceProviders.I_NM_ADR_SVC_PRVD) AS isServiceProviderOfInvolvedPartyId,
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
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServiceProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServiceProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServiceProviders.I_NM_ADR
				LEFT OUTER JOIN dbo.InvolvedParty_T AS ServiceProviderInvolvedParty WITH (NOLOCK)
					ON ServiceProviders.I_ALLCLM = ServiceProviderInvolvedParty.isoClaimId
						AND ServiceProviders.I_NM_ADR_SVC_PRVD = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				Aliases.I_NM_ADR_AKA IS NULL
				AND ServiceProviders.I_NM_ADR_SVC_PRVD IS NOT NULL;
				
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			UPDATE dbo.InvolvedParty_T WITH (TABLOCKX)
				SET
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty_T.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty_T.isBusiness = SOURCE.isBusiness,
					InvolvedParty_T.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,
					InvolvedParty_T.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty_T.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty_T.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty_T.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty_T.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty_T.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty_T.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty_T.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty_T.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty_T.genderCode = SOURCE.genderCode,
					InvolvedParty_T.passportID = SOURCE.passportID,
					InvolvedParty_T.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty_T.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty_T.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty_T.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty_T.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty_T.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty_T.fullName = SOURCE.fullName,
					InvolvedParty_T.firstName = SOURCE.firstName,
					InvolvedParty_T.middleName = SOURCE.middleName,
					InvolvedParty_T.lastName = SOURCE.lastName,
					InvolvedParty_T.suffix = SOURCE.suffix,
					InvolvedParty_T.businessArea = SOURCE.businessArea,
					InvolvedParty_T.businessTel = SOURCE.businessTel,
					InvolvedParty_T.cellArea = SOURCE.cellArea,
					InvolvedParty_T.cellTel = SOURCE.cellTel,
					InvolvedParty_T.faxArea = SOURCE.faxArea,
					InvolvedParty_T.faxTel = SOURCE.faxTel,
					InvolvedParty_T.homeArea = SOURCE.homeArea,
					InvolvedParty_T.homeTel = SOURCE.homeTel,
					InvolvedParty_T.pagerArea = SOURCE.pagerArea,
					InvolvedParty_T.pagerTel = SOURCE.pagerTel,
					InvolvedParty_T.otherArea = SOURCE.otherArea,
					InvolvedParty_T.otherTel = SOURCE.otherTel,
					/*InvolvedParty_T.isActive = SOURCE.isActive,*/
					InvolvedParty_T.dateInserted = @dateInserted
					/*InvolvedParty_T.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty_T.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty_T
					ON SOURCE.involvedPartyId = InvolvedParty_T.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty_T.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty_T.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty_T.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty_T.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty_T.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty_T.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty_T.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty_T.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty_T.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty_T.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty_T.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty_T.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty_T.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty_T.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty_T.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty_T.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty_T.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty_T.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty_T.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty_T.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty_T.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty_T.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty_T.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty_T.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty_T.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty_T.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty_T.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty_T.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty_T.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty_T.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty_T.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty_T.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty_T.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty_T.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty_T.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty_T.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			INSERT INTO dbo.InvolvedParty_T WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				involvedPartyRoleCode,
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
				SOURCE.involvedPartyRoleCode,
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
				COALESCE(AliasInvolvedParty.involvedPartyId, Aliases.I_NM_ADR_AKA) AS isAliasOfInvolvedPartyId,
				COALESCE(ServiceProviderInvolvedParty.involvedPartyId, ServiceProviders.I_NM_ADR_SVC_PRVD) AS isServiceProviderOfInvolvedPartyId,
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
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServiceProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServiceProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServiceProviders.I_NM_ADR
				LEFT OUTER JOIN dbo.InvolvedParty_T AS ServiceProviderInvolvedParty
					ON ServiceProviders.I_ALLCLM = ServiceProviderInvolvedParty.isoClaimId
						AND ServiceProviders.I_NM_ADR_SVC_PRVD = ServiceProviderInvolvedParty.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON Aliases.I_ALLCLM = AliasInvolvedParty.isoClaimId
						AND Aliases.I_NM_ADR_AKA = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				Aliases.I_NM_ADR_AKA IS NOT NULL
				AND ServiceProviders.I_NM_ADR_SVC_PRVD IS NOT NULL;
				
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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

			UPDATE dbo.InvolvedParty_T WITH (TABLOCKX)
				SET
					InvolvedParty_T.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty_T.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					InvolvedParty_T.isBusiness = SOURCE.isBusiness,
					InvolvedParty_T.involvedPartyRoleCode = SOURCE.involvedPartyRoleCode,
					InvolvedParty_T.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty_T.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty_T.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty_T.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty_T.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty_T.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty_T.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty_T.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty_T.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty_T.genderCode = SOURCE.genderCode,
					InvolvedParty_T.passportID = SOURCE.passportID,
					InvolvedParty_T.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty_T.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty_T.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty_T.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty_T.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty_T.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty_T.fullName = SOURCE.fullName,
					InvolvedParty_T.firstName = SOURCE.firstName,
					InvolvedParty_T.middleName = SOURCE.middleName,
					InvolvedParty_T.lastName = SOURCE.lastName,
					InvolvedParty_T.suffix = SOURCE.suffix,
					InvolvedParty_T.businessArea = SOURCE.businessArea,
					InvolvedParty_T.businessTel = SOURCE.businessTel,
					InvolvedParty_T.cellArea = SOURCE.cellArea,
					InvolvedParty_T.cellTel = SOURCE.cellTel,
					InvolvedParty_T.faxArea = SOURCE.faxArea,
					InvolvedParty_T.faxTel = SOURCE.faxTel,
					InvolvedParty_T.homeArea = SOURCE.homeArea,
					InvolvedParty_T.homeTel = SOURCE.homeTel,
					InvolvedParty_T.pagerArea = SOURCE.pagerArea,
					InvolvedParty_T.pagerTel = SOURCE.pagerTel,
					InvolvedParty_T.otherArea = SOURCE.otherArea,
					InvolvedParty_T.otherTel = SOURCE.otherTel,
					/*InvolvedParty_T.isActive = SOURCE.isActive,*/
					InvolvedParty_T.dateInserted = @dateInserted
					/*InvolvedParty_T.isoClaimId = SOURCE.isoClaimId,
					InvolvedParty_T.involvedPartySequenceId = SOURCE.involvedPartySequenceId*/
			FROM
				#FMAliasedServiceProviderData AS SOURCE
				INNER JOIN dbo.InvolvedParty_T
					ON SOURCE.involvedPartyId = InvolvedParty_T.involvedPartyId
			WHERE
				SOURCE.involvedPartyId IS NOT NULL
				AND 
				(
					/*ISNULL(InvolvedParty_T.involvedPartyId,'') <> ISNULL(SOURCE.involvedPartyId,'')*/
					/*OR */ISNULL(InvolvedParty_T.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty_T.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty_T.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty_T.involvedPartyRoleCode,'') <> ISNULL(SOURCE.involvedPartyRoleCode,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty_T.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty_T.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty_T.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty_T.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty_T.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty_T.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty_T.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty_T.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty_T.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty_T.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty_T.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty_T.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty_T.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty_T.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty_T.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty_T.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty_T.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty_T.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty_T.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty_T.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty_T.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty_T.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty_T.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty_T.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty_T.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty_T.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty_T.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty_T.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty_T.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*OR ISNULL(InvolvedParty_T.isActive,'') <> ISNULL(SOURCE.isActive,'')
					OR ISNULL(InvolvedParty_T.dateInserted,'') <> ISNULL(SOURCE.dateInserted,'')
					OR ISNULL(InvolvedParty_T.isoClaimId,'') <> ISNULL(SOURCE.isoClaimId,'')
					OR ISNULL(InvolvedParty_T.involvedPartySequenceId,'') <> ISNULL(SOURCE.involvedPartySequenceId,'')*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
				@stepDescription = 'InsertNewFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty_T WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isBusiness,
				involvedPartyRoleCode,
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
				SOURCE.involvedPartyRoleCode,
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
			INSERT INTO dbo.InvolvedParty_TActivityLog
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
		INSERT INTO dbo.InvolvedParty_TActivityLog
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
					+ 'of hsp_UpdateInsertInvolvedPartyT; ErrorMsg: '
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
COMMIT TRANSACTION
20190114 : 5:27PM
20190122 : 9:52AM
20190122 : 4:50PM
20190130 : 9:54AM
*/