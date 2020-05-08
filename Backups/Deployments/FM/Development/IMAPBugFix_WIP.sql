BEGIN TRANSACTION

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
						'20080101'
						AS VARCHAR(10)
					),
				'-','')
				AS INT
			)
			--AND CLT00004.I_ALLCLM = '1S004855394'
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1

--SELECT * FROM #ScrubbedNameData

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
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR_AKA
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServicesProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServicesProviders.I_NM_ADR_SVC_PRVD
			WHERE
				Aliases.I_NM_ADR_AKA IS NULL
				AND ServicesProviders.I_NM_ADR_SVC_PRVD IS NULL;


--SELECT * FROM ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
--INNER JOIN #ScrubbedNameData
--					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
--						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR_AKA
	
--SELECT
--	*
--FROM
--	#FMNonAliasedInvolvedPartyData


		
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
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON Aliases.I_ALLCLM = AliasInvolvedParty.isoClaimId
						AND Aliases.I_NM_ADR_AKA = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				Aliases.I_NM_ADR_AKA IS NOT NULL
				AND ServiceProviders.I_NM_ADR_SVC_PRVD IS NULL;


--SELECT * FROM #FMAliasedInvolvedPartyData



		
			SELECT
				#ScrubbedNameData.involvedPartyId,
				/*COALESCE for forced FKViolation, in the instance of a Primary record not existing*/
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
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
				INTO #FMAliasedInvolvedPartyDataTwo
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = Aliases.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = Aliases.I_NM_ADR_AKA
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServiceProviders WITH (NOLOCK)
					ON #ScrubbedNameData.isoClaimId = ServiceProviders.I_ALLCLM
						AND #ScrubbedNameData.involvedPartySequenceId = ServiceProviders.I_NM_ADR
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON Aliases.I_ALLCLM = AliasInvolvedParty.isoClaimId
						AND Aliases.I_NM_ADR = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				Aliases.I_NM_ADR_AKA IS NOT NULL
				AND ServiceProviders.I_NM_ADR_SVC_PRVD IS NULL;
				
	

--SELECT * FROM #FMAliasedInvolvedPartyDataTwo



SELECT
				InvolvedParty.involvedPartyId,
				V_ActiveClaim.claimId,
				V_ActiveNonLocationOfLoss.addressId,
				ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK'),
				1 AS isActive,
				'20190426',
				InvolvedParty.isoClaimId,
				InvolvedParty.involvedPartySequenceId
			FROM
				dbo.FireMarshalDriver
				INNER JOIN dbo.V_ActiveClaim
					ON FireMarshalDriver.isoClaimId = V_ActiveClaim.isoClaimId
				INNER JOIN #ScrubbedNameData
					ON FireMarshalDriver.isoClaimId = #ScrubbedNameData.isoClaimId
				INNER JOIN dbo.InvolvedParty
					ON FireMarshalDriver.isoClaimId = InvolvedParty.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = InvolvedParty.involvedPartySequenceId
				INNER JOIN dbo.V_ActiveNonLocationOfLoss
					ON FireMarshalDriver.isoClaimId = V_ActiveNonLocationOfLoss.isoClaimId
						AND InvolvedParty.involvedPartySequenceId = V_ActiveNonLocationOfLoss.involvedPartySequenceId
			--	LEFT OUTER JOIN dbo.InvolvedPartyAddressMap
			--		ON V_ActiveClaim.isoClaimId = InvolvedPartyAddressMap.isoClaimId
			--			AND InvolvedParty.involvedPartyId = InvolvedPartyAddressMap.involvedPartyId
			--			AND V_ActiveNonLocationOfLoss.addressId = InvolvedPartyAddressMap.nonLocationOfLossAddressId
			--			AND ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK') = InvolvedPartyAddressMap.claimRoleCode
			--WHERE
			--	InvolvedPartyAddressMap.isoCLaimId IS NULL;

ROLLBACK TRANSACTION