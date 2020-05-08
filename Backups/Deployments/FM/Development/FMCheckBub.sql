BEGIN TRANSACTION

DECLARE @IAllClm VARCHAR(11) = '8T004993371'

DECLARE
	@dateFilterParam DATETIME2(0) = '20140101',
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
			MAX(AddressActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
			CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.AddressActivityLog  WITH (NOLOCK)
WHERE
	AddressActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND AddressActivityLog.isSuccessful = 1;
SET @sourceDateTime = @dateFilterParam;

SELECT
	DuplicateDataSetPerformanceHackMelissaNameMap.addressId,
	CAST(1 AS BIT) AS isLocationOfLoss,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCodeExtended,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyFIPS,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountryCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.latitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.longitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.geoAccuracy,
	DuplicateDataSetPerformanceHackMelissaNameMap.melissaMappingKey,
	DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId
	--INTO #LocationOfLossData
FROM
	(/*Notes on DuplicateDataSetPerformanceHack: dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 contains duplicate records
		performance of rowNumber/partition is noticeably better than using DISTINCT*/
		SELECT
			ExistingAddress.addressId,
			CAST(LTRIM(RTRIM(CLT00001.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId, 
			ROW_NUMBER() OVER(
				PARTITION BY
					CLT00001.ALLCLMROWID
				ORDER BY
					CLT00001.Date_Insert DESC
					/*CS_Lookup_EntityIDs.[YEAR] DESC*/
			) AS uniqueInstanceValue,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR1)),'') AS VARCHAR(50))AS originalAddressLine1,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR2)),'') AS VARCHAR(50))AS originalAddressLine2,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.M_LOL_CITY)),'') AS VARCHAR(25))AS originalCityName,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ST_ALPH)),'') AS CHAR(2))AS originalStateCode,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ZIP)),'') AS VARCHAR(9))AS originalZipCode,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine1,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine2,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(25))AS scrubbedCityName,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(2))AS scrubbedStateCode,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(5))AS scrubbedZipCode,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(4))AS scrubbedZipCodeExtended,
			CAST(
				COALESCE
				(
					NULLIF(LTRIM(RTRIM(NULL)),''),
					NULLIF(LTRIM(RTRIM(NULL)),'')
				) AS VARCHAR(25)
			) AS scrubbedCountyName,
			CAST(
				COALESCE
				(
					NULLIF(LTRIM(RTRIM(NULL)),''),
					NULLIF(LTRIM(RTRIM(NULL)),'')
				) AS VARCHAR(25)
			) AS scrubbedCountyFIPS,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(3))AS scrubbedCountryCode,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS latitude,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS longitude,
			CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS geoAccuracy,
			CAST(NULL AS BIGINT) AS melissaMappingKey
		FROM
			dbo.FireMarshalDriver WITH (NOLOCK)
			INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
				ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
			LEFT OUTER JOIN dbo.Address AS ExistingAddress WITH (NOLOCK)
				ON ExistingAddress.isoClaimId = CLT00001.I_ALLCLM
					AND ExistingAddress.isLocationOfLoss = 1
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
			--	ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
			--	ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
		WHERE
			/*Deprecating due to performance costs, and current profile state. RDW 20190328:
				NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
			*/
			CLT00001.I_ALLCLM = @IAllClm
			--AND CLT00001.Date_Insert >= CAST(
			--	REPLACE(
			--		CAST(
			--			@dateFilterParam
			--			AS VARCHAR(10)
			--		),
			--	'-','')
			--	AS INT
			--)
			--OR CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert >= CAST(@dateFilterParam AS DATE)
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;

ROLLBACK TRANSACTION
	