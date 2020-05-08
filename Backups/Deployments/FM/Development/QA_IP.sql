BEGIN TRANSACTION
USE ClaimSearch_DEV
SELECT
*
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
			CLT00004.I_ALLCLM IN
			(
				'4L004368602',
				'7M004306859',
				'2X004340224',
				'7V004056264',
				'9C004380551'
			)
			--CLT00004.Date_Insert >= CAST(
			--	REPLACE(
			--		CAST(
			--			@dateFilterParam
			--			AS VARCHAR(10)
			--		),
			--	'-','')
			--	AS INT
			--)
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1
ORDER BY 
DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId;




SELECT
	DuplicateValueSetList.allClmId,
	DuplicateValueSetList.nmAdr
	INTO #tempSet
FROM
	(
		VALUES
		('2X004340224',1),
		('2X004340224',2),
		('2X004340224',3),
		('2X004340224',4),
		('2X004340224',5),
		('2X004340224',6),
		('2X004340224',7),
		('2X004340224',8)
		--,
		--('4L004368602',1),
		--('4L004368602',2),
		--('7M004306859',1),
		--('7M004306859',2),
		--('7M004306859',3),
		--('7M004306859',4),
		--('7M004306859',5),
		--('7M004306859',6),
		--('7V004056264',1),
		--('7V004056264',2),
		--('7V004056264',3),
		--('7V004056264',4),
		--('9C004380551',1),
		--('9C004380551',2),
		--('9C004380551',3),
		--('9C004380551',4),
		--('9C004380551',5)
	) AS DuplicateValueSetList (allClmId, nmAdr)
	
SELECT *
FROM dbo.InvolvedParty
	INNER JOIN #tempSet
		ON InvolvedParty.isoClaimId = #tempSet.allClmId
			AND InvolvedParty.involvedPartySequenceId = #tempSet.nmAdr

SELECT
	*
FROM
	ClaimSearch_Prod.dbo.CLT00004
	INNER JOIN #tempSet
		ON CLT00004.I_ALLCLM = #tempSet.allClmId
			AND CLT00004.I_NM_ADR = #tempSet.nmAdr

SELECT
	*
FROM
	ClaimSearch_Prod.dbo.CLT00006
	INNER JOIN #tempSet
		ON CLT00006.I_ALLCLM = #tempSet.allClmId
			AND CLT00006.I_NM_ADR = #tempSet.nmAdr

SELECT
	*
FROM
	ClaimSearch_Prod.dbo.CLT00006
	INNER JOIN #tempSet
		ON CLT00006.I_ALLCLM = #tempSet.allClmId
			AND CLT00006.I_NM_ADR_AKA = #tempSet.nmAdr


SELECT
	*
FROM
	ClaimSearch_Prod.dbo.CLT00021
	INNER JOIN #tempSet
		ON CLT00021.I_ALLCLM = #tempSet.allClmId
			AND CLT00021.I_NM_ADR = #tempSet.nmAdr

SELECT
	*
FROM
	ClaimSearch_Prod.dbo.CLT00021
	INNER JOIN #tempSet
		ON CLT00021.I_ALLCLM = #tempSet.allClmId
			AND CLT00021.I_NM_ADR_SVC_PRVD = #tempSet.nmAdr

ROLLBACK TRANSACTION